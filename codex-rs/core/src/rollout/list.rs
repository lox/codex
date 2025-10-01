use std::cmp::Reverse;
use std::collections::VecDeque;
use std::io::{self};
use std::path::Path;
use std::path::PathBuf;

use codex_file_search as file_search;
use std::num::NonZero;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use time::OffsetDateTime;
use time::PrimitiveDateTime;
use time::format_description::FormatItem;
use time::macros::format_description;
use uuid::Uuid;

use super::SESSIONS_SUBDIR;
use crate::protocol::EventMsg;
use codex_protocol::mcp_protocol::ConversationId;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::RolloutLine;

/// Returned page of conversation summaries.
#[derive(Debug, Default, PartialEq)]
pub struct ConversationsPage {
    /// Conversation summaries ordered newest first.
    pub items: Vec<ConversationItem>,
    /// Opaque pagination token to resume after the last item, or `None` if end.
    pub next_cursor: Option<Cursor>,
    /// Total number of files touched while scanning this request.
    pub num_scanned_files: usize,
    /// True if a hard scan cap was hit; consider resuming with `next_cursor`.
    pub reached_scan_cap: bool,
}

/// Summary information for a conversation rollout file.
#[derive(Debug, PartialEq)]
pub struct ConversationItem {
    /// Absolute path to the rollout file.
    pub path: PathBuf,
    /// First up to `HEAD_RECORD_LIMIT` JSONL records parsed as JSON (includes meta line).
    pub head: Vec<serde_json::Value>,
    /// Last up to `TAIL_RECORD_LIMIT` JSONL response records parsed as JSON.
    pub tail: Vec<serde_json::Value>,
}

/// Hard cap to bound worst‑case work per request.
const MAX_SCAN_FILES: usize = 100;
const HEAD_RECORD_LIMIT: usize = 10;
const TAIL_RECORD_LIMIT: usize = 10;

/// Check if a conversation's cwd matches the filter path.
/// Looks for SessionMeta in the head records and compares the cwd field.
fn matches_cwd_filter(head: &[serde_json::Value], filter_cwd: &Path) -> bool {
    for record in head {
        // SessionMeta records have a "cwd" field at the top level
        if let Some(cwd_value) = record.get("cwd") {
            if let Some(cwd_str) = cwd_value.as_str() {
                let conversation_cwd = PathBuf::from(cwd_str);
                return conversation_cwd == filter_cwd;
            }
        }
    }
    false
}

/// Pagination cursor identifying a file by its modification time and UUID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cursor {
    /// File modification time (mtime) as seconds since Unix epoch.
    mtime_secs: i64,
    /// Nanoseconds component of mtime for sub-second precision.
    mtime_nanos: u32,
    /// Conversation UUID from the filename (for stable ordering).
    id: Uuid,
}

impl Cursor {
    fn new(mtime_secs: i64, mtime_nanos: u32, id: Uuid) -> Self {
        Self {
            mtime_secs,
            mtime_nanos,
            id,
        }
    }
}

impl serde::Serialize for Cursor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Format: "{mtime_secs}.{mtime_nanos}|{uuid}"
        serializer.serialize_str(&format!(
            "{}.{:09}|{}",
            self.mtime_secs, self.mtime_nanos, self.id
        ))
    }
}

impl<'de> serde::Deserialize<'de> for Cursor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        parse_cursor(&s).ok_or_else(|| serde::de::Error::custom("invalid cursor"))
    }
}

/// Retrieve recorded conversation file paths with token pagination. The returned `next_cursor`
/// can be supplied on the next call to resume after the last returned item, resilient to
/// concurrent new sessions being appended. Ordering is by mtime desc (most recently modified first),
/// then UUID desc for stable ordering. If `cwd_filter` is provided, only conversations from that
/// working directory are included.
pub(crate) async fn get_conversations(
    codex_home: &Path,
    page_size: usize,
    cursor: Option<&Cursor>,
    cwd_filter: Option<&Path>,
) -> io::Result<ConversationsPage> {
    let mut root = codex_home.to_path_buf();
    root.push(SESSIONS_SUBDIR);

    if !root.exists() {
        return Ok(ConversationsPage {
            items: Vec::new(),
            next_cursor: None,
            num_scanned_files: 0,
            reached_scan_cap: false,
        });
    }

    let anchor = cursor.cloned();

    let result =
        traverse_directories_for_paths(root.clone(), page_size, anchor, cwd_filter).await?;
    Ok(result)
}

/// Load the full contents of a single conversation session file at `path`.
/// Returns the entire file contents as a String.
#[allow(dead_code)]
pub(crate) async fn get_conversation(path: &Path) -> io::Result<String> {
    tokio::fs::read_to_string(path).await
}

/// Load conversation file paths from disk using directory traversal.
///
/// Directory layout: `~/.codex/sessions/YYYY/MM/DD/rollout-YYYY-MM-DDThh-mm-ss-<uuid>.jsonl`
/// Sorted by mtime desc (most recently modified first).
async fn traverse_directories_for_paths(
    root: PathBuf,
    page_size: usize,
    anchor: Option<Cursor>,
    cwd_filter: Option<&Path>,
) -> io::Result<ConversationsPage> {
    // Collect all rollout files with their mtime and UUID
    let mut all_files: Vec<(i64, u32, Uuid, PathBuf)> = Vec::new();
    let mut scanned_files = 0usize;

    // Traverse all directories to collect files
    let year_dirs = collect_dirs_desc(&root, |s| s.parse::<u16>().ok()).await?;
    'outer: for (_year, year_path) in year_dirs.iter() {
        if scanned_files >= MAX_SCAN_FILES {
            break;
        }
        let month_dirs = collect_dirs_desc(year_path, |s| s.parse::<u8>().ok()).await?;
        for (_month, month_path) in month_dirs.iter() {
            if scanned_files >= MAX_SCAN_FILES {
                break 'outer;
            }
            let day_dirs = collect_dirs_desc(month_path, |s| s.parse::<u8>().ok()).await?;
            for (_day, day_path) in day_dirs.iter() {
                if scanned_files >= MAX_SCAN_FILES {
                    break 'outer;
                }
                let day_files = collect_files_with_mtime(day_path).await?;
                scanned_files += day_files.len();
                all_files.extend(day_files);
                if scanned_files >= MAX_SCAN_FILES {
                    break 'outer;
                }
            }
        }
    }

    // Sort by mtime desc, then uuid desc for stability
    all_files.sort_by_key(|(mtime_secs, mtime_nanos, id, _path)| {
        (Reverse(*mtime_secs), Reverse(*mtime_nanos), Reverse(*id))
    });

    // Apply pagination using anchor
    let mut items: Vec<ConversationItem> = Vec::with_capacity(page_size);
    let mut anchor_passed = anchor.is_none();
    let (anchor_mtime_secs, anchor_mtime_nanos, anchor_id) = match &anchor {
        Some(c) => (c.mtime_secs, c.mtime_nanos, c.id),
        None => (i64::MAX, u32::MAX, Uuid::max()),
    };

    for (mtime_secs, mtime_nanos, id, path) in all_files.into_iter() {
        if !anchor_passed {
            // Check if we've passed the anchor point
            if mtime_secs < anchor_mtime_secs
                || (mtime_secs == anchor_mtime_secs && mtime_nanos < anchor_mtime_nanos)
                || (mtime_secs == anchor_mtime_secs
                    && mtime_nanos == anchor_mtime_nanos
                    && id < anchor_id)
            {
                anchor_passed = true;
            } else {
                continue;
            }
        }

        if items.len() >= page_size {
            break;
        }

        // Read head and tail to validate and filter
        let (head, tail, saw_session_meta, saw_user_event) =
            read_head_and_tail(&path, HEAD_RECORD_LIMIT, TAIL_RECORD_LIMIT)
                .await
                .unwrap_or((Vec::new(), Vec::new(), false, false));

        // Apply filters: must have session meta and at least one user message event
        if saw_session_meta && saw_user_event {
            // If cwd_filter is set, check if conversation matches
            if let Some(filter_cwd) = cwd_filter {
                if !matches_cwd_filter(&head, filter_cwd) {
                    continue;
                }
            }
            items.push(ConversationItem { path, head, tail });
        }
    }

    let next = build_next_cursor(&items);

    Ok(ConversationsPage {
        items,
        next_cursor: next,
        num_scanned_files: scanned_files,
        reached_scan_cap: scanned_files >= MAX_SCAN_FILES,
    })
}

pub async fn find_rollout_by_conversation_id(
    codex_home: &Path,
    conversation_id: &ConversationId,
) -> io::Result<Option<PathBuf>> {
    let root = codex_home.join(SESSIONS_SUBDIR);
    if !root.exists() {
        return Ok(None);
    }

    let mut to_visit = VecDeque::new();
    to_visit.push_back(root);
    let mut matches = Vec::new();
    let suffix = format!("-{conversation_id}.jsonl");

    while let Some(dir) = to_visit.pop_front() {
        let mut entries = tokio::fs::read_dir(&dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let file_type = entry.file_type().await?;
            let path = entry.path();
            if file_type.is_dir() {
                to_visit.push_back(path);
            } else if file_type.is_file()
                && entry
                    .file_name()
                    .to_str()
                    .map(|name| name.starts_with("rollout-") && name.ends_with(&suffix))
                    .unwrap_or(false)
            {
                matches.push(path);
            }
        }
    }

    match matches.len() {
        0 => Ok(None),
        1 => Ok(matches.pop()),
        _ => {
            let preview: Vec<String> = matches
                .iter()
                .take(3)
                .map(|p| p.display().to_string())
                .collect();
            let preview_joined = preview.join(", ");
            let suggestion = if matches.len() > preview.len() {
                format!(
                    "{} (showing {} of {} matches)",
                    preview_joined,
                    preview.len(),
                    matches.len()
                )
            } else {
                preview_joined
            };
            Err(io::Error::other(format!(
                "found {} rollout files for conversation {conversation_id}; re-run with --resume-rollout <path> to choose one (examples: {})",
                matches.len(),
                suggestion
            )))
        }
    }
}

/// Pagination cursor token format: "{mtime_secs}.{mtime_nanos}|{uuid}"
/// The cursor orders files by mtime desc, then UUID desc.
fn parse_cursor(token: &str) -> Option<Cursor> {
    let (mtime_str, uuid_str) = token.split_once('|')?;
    let (secs_str, nanos_str) = mtime_str.split_once('.')?;

    let mtime_secs: i64 = secs_str.parse().ok()?;
    let mtime_nanos: u32 = nanos_str.parse().ok()?;
    let uuid = Uuid::parse_str(uuid_str).ok()?;

    Some(Cursor::new(mtime_secs, mtime_nanos, uuid))
}

fn build_next_cursor(items: &[ConversationItem]) -> Option<Cursor> {
    let last = items.last()?;
    let file_name = last.path.file_name()?.to_string_lossy();
    let (_ts, id) = parse_timestamp_uuid_from_filename(&file_name)?;

    // Get the mtime of the last item
    let metadata = std::fs::metadata(&last.path).ok()?;
    let modified = metadata.modified().ok()?;
    let duration = modified
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let mtime_secs = duration.as_secs() as i64;
    let mtime_nanos = duration.subsec_nanos();

    Some(Cursor::new(mtime_secs, mtime_nanos, id))
}

/// Collects immediate subdirectories of `parent`, parses their (string) names with `parse`,
/// and returns them sorted descending by the parsed key.
async fn collect_dirs_desc<T, F>(parent: &Path, parse: F) -> io::Result<Vec<(T, PathBuf)>>
where
    T: Ord + Copy,
    F: Fn(&str) -> Option<T>,
{
    let mut dir = tokio::fs::read_dir(parent).await?;
    let mut vec: Vec<(T, PathBuf)> = Vec::new();
    while let Some(entry) = dir.next_entry().await? {
        if entry
            .file_type()
            .await
            .map(|ft| ft.is_dir())
            .unwrap_or(false)
            && let Some(s) = entry.file_name().to_str()
            && let Some(v) = parse(s)
        {
            vec.push((v, entry.path()));
        }
    }
    vec.sort_by_key(|(v, _)| Reverse(*v));
    Ok(vec)
}

/// Collects rollout files in a directory with their mtime and UUID.
/// Returns Vec<(mtime_secs, mtime_nanos, uuid, path)>
async fn collect_files_with_mtime(parent: &Path) -> io::Result<Vec<(i64, u32, Uuid, PathBuf)>> {
    let mut dir = tokio::fs::read_dir(parent).await?;
    let mut collected: Vec<(i64, u32, Uuid, PathBuf)> = Vec::new();

    while let Some(entry) = dir.next_entry().await? {
        let file_type = entry.file_type().await?;
        if !file_type.is_file() {
            continue;
        }

        let file_name = entry.file_name();
        let Some(name_str) = file_name.to_str() else {
            continue;
        };

        if !name_str.starts_with("rollout-") || !name_str.ends_with(".jsonl") {
            continue;
        }

        // Extract UUID from filename
        let Some((_ts, uuid)) = parse_timestamp_uuid_from_filename(name_str) else {
            continue;
        };

        // Get file metadata for mtime
        let path = entry.path();
        let metadata = match tokio::fs::metadata(&path).await {
            Ok(m) => m,
            Err(_) => continue,
        };

        let modified = match metadata.modified() {
            Ok(t) => t,
            Err(_) => continue,
        };

        // Convert SystemTime to Unix timestamp
        let duration = modified
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let mtime_secs = duration.as_secs() as i64;
        let mtime_nanos = duration.subsec_nanos();

        collected.push((mtime_secs, mtime_nanos, uuid, path));
    }

    Ok(collected)
}

fn parse_timestamp_uuid_from_filename(name: &str) -> Option<(OffsetDateTime, Uuid)> {
    // Expected: rollout-YYYY-MM-DDThh-mm-ss-<uuid>.jsonl
    let core = name.strip_prefix("rollout-")?.strip_suffix(".jsonl")?;

    // Scan from the right for a '-' such that the suffix parses as a UUID.
    let (sep_idx, uuid) = core
        .match_indices('-')
        .rev()
        .find_map(|(i, _)| Uuid::parse_str(&core[i + 1..]).ok().map(|u| (i, u)))?;

    let ts_str = &core[..sep_idx];
    let format: &[FormatItem] =
        format_description!("[year]-[month]-[day]T[hour]-[minute]-[second]");
    let ts = PrimitiveDateTime::parse(ts_str, format).ok()?.assume_utc();
    Some((ts, uuid))
}

async fn read_head_and_tail(
    path: &Path,
    head_limit: usize,
    tail_limit: usize,
) -> io::Result<(Vec<serde_json::Value>, Vec<serde_json::Value>, bool, bool)> {
    use tokio::io::AsyncBufReadExt;

    let file = tokio::fs::File::open(path).await?;
    let reader = tokio::io::BufReader::new(file);
    let mut lines = reader.lines();
    let mut head: Vec<serde_json::Value> = Vec::new();
    let mut saw_session_meta = false;
    let mut saw_user_event = false;

    while head.len() < head_limit {
        let line_opt = lines.next_line().await?;
        let Some(line) = line_opt else { break };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let parsed: Result<RolloutLine, _> = serde_json::from_str(trimmed);
        let Ok(rollout_line) = parsed else { continue };

        match rollout_line.item {
            RolloutItem::SessionMeta(session_meta_line) => {
                if let Ok(val) = serde_json::to_value(session_meta_line) {
                    head.push(val);
                    saw_session_meta = true;
                }
            }
            RolloutItem::ResponseItem(item) => {
                if let Ok(val) = serde_json::to_value(item) {
                    head.push(val);
                }
            }
            RolloutItem::TurnContext(_) => {
                // Not included in `head`; skip.
            }
            RolloutItem::Compacted(_) => {
                // Not included in `head`; skip.
            }
            RolloutItem::EventMsg(ev) => {
                if matches!(ev, EventMsg::UserMessage(_)) {
                    saw_user_event = true;
                }
            }
        }
    }

    let tail = if tail_limit == 0 {
        Vec::new()
    } else {
        read_tail_records(path, tail_limit).await?
    };

    Ok((head, tail, saw_session_meta, saw_user_event))
}

async fn read_tail_records(path: &Path, max_records: usize) -> io::Result<Vec<serde_json::Value>> {
    use std::io::SeekFrom;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncSeekExt;

    if max_records == 0 {
        return Ok(Vec::new());
    }

    const CHUNK_SIZE: usize = 8192;

    let mut file = tokio::fs::File::open(path).await?;
    let mut pos = file.seek(SeekFrom::End(0)).await?;
    if pos == 0 {
        return Ok(Vec::new());
    }

    let mut buffer: Vec<u8> = Vec::new();

    loop {
        let slice_start = match (pos > 0, buffer.iter().position(|&b| b == b'\n')) {
            (true, Some(idx)) => idx + 1,
            _ => 0,
        };
        let tail = collect_last_response_values(&buffer[slice_start..], max_records);
        if tail.len() >= max_records || pos == 0 {
            return Ok(tail);
        }

        let read_size = CHUNK_SIZE.min(pos as usize);
        if read_size == 0 {
            return Ok(tail);
        }
        pos -= read_size as u64;
        file.seek(SeekFrom::Start(pos)).await?;
        let mut chunk = vec![0; read_size];
        file.read_exact(&mut chunk).await?;
        chunk.extend_from_slice(&buffer);
        buffer = chunk;
    }
}

fn collect_last_response_values(buffer: &[u8], max_records: usize) -> Vec<serde_json::Value> {
    use std::borrow::Cow;

    if buffer.is_empty() || max_records == 0 {
        return Vec::new();
    }

    let text: Cow<'_, str> = String::from_utf8_lossy(buffer);
    let mut collected_rev: Vec<serde_json::Value> = Vec::new();
    for line in text.lines().rev() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let parsed: serde_json::Result<RolloutLine> = serde_json::from_str(trimmed);
        let Ok(rollout_line) = parsed else { continue };
        if let RolloutItem::ResponseItem(item) = rollout_line.item
            && let Ok(val) = serde_json::to_value(item)
        {
            collected_rev.push(val);
            if collected_rev.len() == max_records {
                break;
            }
        }
    }
    collected_rev.reverse();
    collected_rev
}

/// Locate a recorded conversation rollout file by its UUID string using the existing
/// paginated listing implementation. Returns `Ok(Some(path))` if found, `Ok(None)` if not present
/// or the id is invalid.
pub async fn find_conversation_path_by_id_str(
    codex_home: &Path,
    id_str: &str,
) -> io::Result<Option<PathBuf>> {
    // Validate UUID format early.
    if Uuid::parse_str(id_str).is_err() {
        return Ok(None);
    }

    let mut root = codex_home.to_path_buf();
    root.push(SESSIONS_SUBDIR);
    if !root.exists() {
        return Ok(None);
    }
    // This is safe because we know the values are valid.
    #[allow(clippy::unwrap_used)]
    let limit = NonZero::new(1).unwrap();
    // This is safe because we know the values are valid.
    #[allow(clippy::unwrap_used)]
    let threads = NonZero::new(2).unwrap();
    let cancel = Arc::new(AtomicBool::new(false));
    let exclude: Vec<String> = Vec::new();
    let compute_indices = false;

    let results = file_search::run(
        id_str,
        limit,
        &root,
        exclude,
        threads,
        cancel,
        compute_indices,
    )
    .map_err(|e| io::Error::other(format!("file search failed: {e}")))?;

    Ok(results
        .matches
        .into_iter()
        .next()
        .map(|m| root.join(m.path)))
}
