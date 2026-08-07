//! WebSocket protocol message types and encoding/decoding.
//!
//! Message format:
//! - Text: `ACTION|JSON`
//! - Binary: `[0x00, 0x00][36 bytes sessionId][4 bytes chunkIndex BE][payload]`

#![allow(dead_code)]

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::fmt;

pub const SEPARATOR: &str = "|";

// ── Binary frame constants ─────────────────────────────────────────────

/// Binary frame prefix (0x00 0x00)
pub const PREFIX_BC: [u8; 2] = [b'0', b'0'];

/// Session ID length in binary frames
pub const SESSION_ID_LEN: usize = 36;

/// Chunk index length in binary frames (4 bytes, big-endian)
pub const CHUNK_INDEX_LEN: usize = 4;

// ── Client → Server actions ─────────────────────────────────────────────

/// Client-to-server action types
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum ClientAction {
    Authorization,
    ClientInfo,
    NoteSync,
    NoteModify,
    NoteDelete,
    NoteRename,
    NoteCheck,
    NoteRePush,
    FileSync,
    FileUploadCheck,
    FileDelete,
    FileChunkDownload,
    FolderSync,
    FolderModify,
    FolderDelete,
    FolderRename,
    SettingSync,
    SettingModify,
    SettingDelete,
}

// ── Server → Client actions ─────────────────────────────────────────────

/// Server-to-client action types
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum ServerAction {
    NoteSyncEnd,
    NoteSyncModify,
    NoteSyncDelete,
    NoteSyncRename,
    NoteSyncMtime,
    NoteSyncNeedPush,
    NoteModifyAck,
    NoteDeleteAck,
    FileSyncUpdate,
    FileSyncDelete,
    FileSyncRename,
    FileSyncMtime,
    FileSyncChunkDownload,
    FileUpload,
    FileUploadAck,
    FileDeleteAck,
    FileSyncEnd,
    SettingSyncModify,
    SettingSyncDelete,
    SettingSyncRename,
    SettingSyncMtime,
    SettingSyncNeedUpload,
    SettingSyncEnd,
    SettingModifyAck,
    FolderSyncModify,
    FolderSyncDelete,
    FolderSyncRename,
    FolderSyncEnd,
}

// ── Combined action enum ────────────────────────────────────────────────

/// All possible action types (bidirectional)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Client(ClientAction),
    Server(ServerAction),
}

impl fmt::Display for ClientAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientAction::Authorization => write!(f, "Authorization"),
            ClientAction::ClientInfo => write!(f, "ClientInfo"),
            ClientAction::NoteSync => write!(f, "NoteSync"),
            ClientAction::NoteModify => write!(f, "NoteModify"),
            ClientAction::NoteDelete => write!(f, "NoteDelete"),
            ClientAction::NoteRename => write!(f, "NoteRename"),
            ClientAction::NoteCheck => write!(f, "NoteCheck"),
            ClientAction::NoteRePush => write!(f, "NoteRePush"),
            ClientAction::FileSync => write!(f, "FileSync"),
            ClientAction::FileUploadCheck => write!(f, "FileUploadCheck"),
            ClientAction::FileDelete => write!(f, "FileDelete"),
            ClientAction::FileChunkDownload => write!(f, "FileChunkDownload"),
            ClientAction::FolderSync => write!(f, "FolderSync"),
            ClientAction::FolderModify => write!(f, "FolderModify"),
            ClientAction::FolderDelete => write!(f, "FolderDelete"),
            ClientAction::FolderRename => write!(f, "FolderRename"),
            ClientAction::SettingSync => write!(f, "SettingSync"),
            ClientAction::SettingModify => write!(f, "SettingModify"),
            ClientAction::SettingDelete => write!(f, "SettingDelete"),
        }
    }
}

impl fmt::Display for ServerAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServerAction::NoteSyncEnd => write!(f, "NoteSyncEnd"),
            ServerAction::NoteSyncModify => write!(f, "NoteSyncModify"),
            ServerAction::NoteSyncDelete => write!(f, "NoteSyncDelete"),
            ServerAction::NoteSyncRename => write!(f, "NoteSyncRename"),
            ServerAction::NoteSyncMtime => write!(f, "NoteSyncMtime"),
            ServerAction::NoteSyncNeedPush => write!(f, "NoteSyncNeedPush"),
            ServerAction::NoteModifyAck => write!(f, "NoteModifyAck"),
            ServerAction::NoteDeleteAck => write!(f, "NoteDeleteAck"),
            ServerAction::FileSyncUpdate => write!(f, "FileSyncUpdate"),
            ServerAction::FileSyncDelete => write!(f, "FileSyncDelete"),
            ServerAction::FileSyncRename => write!(f, "FileSyncRename"),
            ServerAction::FileSyncMtime => write!(f, "FileSyncMtime"),
            ServerAction::FileSyncChunkDownload => write!(f, "FileSyncChunkDownload"),
            ServerAction::FileUpload => write!(f, "FileUpload"),
            ServerAction::FileUploadAck => write!(f, "FileUploadAck"),
            ServerAction::FileDeleteAck => write!(f, "FileDeleteAck"),
            ServerAction::FileSyncEnd => write!(f, "FileSyncEnd"),
            ServerAction::SettingSyncModify => write!(f, "SettingSyncModify"),
            ServerAction::SettingSyncDelete => write!(f, "SettingSyncDelete"),
            ServerAction::SettingSyncRename => write!(f, "SettingSyncRename"),
            ServerAction::SettingSyncMtime => write!(f, "SettingSyncMtime"),
            ServerAction::SettingSyncNeedUpload => write!(f, "SettingSyncNeedUpload"),
            ServerAction::SettingSyncEnd => write!(f, "SettingSyncEnd"),
            ServerAction::SettingModifyAck => write!(f, "SettingModifyAck"),
            ServerAction::FolderSyncModify => write!(f, "FolderSyncModify"),
            ServerAction::FolderSyncDelete => write!(f, "FolderSyncDelete"),
            ServerAction::FolderSyncRename => write!(f, "FolderSyncRename"),
            ServerAction::FolderSyncEnd => write!(f, "FolderSyncEnd"),
        }
    }
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Action::Client(a) => a.fmt(f),
            Action::Server(a) => a.fmt(f),
        }
    }
}

impl std::str::FromStr for Action {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Authorization" => Ok(Action::Client(ClientAction::Authorization)),
            "ClientInfo" => Ok(Action::Client(ClientAction::ClientInfo)),
            "NoteSync" => Ok(Action::Client(ClientAction::NoteSync)),
            "NoteModify" => Ok(Action::Client(ClientAction::NoteModify)),
            "NoteDelete" => Ok(Action::Client(ClientAction::NoteDelete)),
            "NoteRename" => Ok(Action::Client(ClientAction::NoteRename)),
            "NoteCheck" => Ok(Action::Client(ClientAction::NoteCheck)),
            "NoteRePush" => Ok(Action::Client(ClientAction::NoteRePush)),
            "FileSync" => Ok(Action::Client(ClientAction::FileSync)),
            "FileUploadCheck" => Ok(Action::Client(ClientAction::FileUploadCheck)),
            "FileDelete" => Ok(Action::Client(ClientAction::FileDelete)),
            "FileChunkDownload" => Ok(Action::Client(ClientAction::FileChunkDownload)),
            "FolderSync" => Ok(Action::Client(ClientAction::FolderSync)),
            "FolderModify" => Ok(Action::Client(ClientAction::FolderModify)),
            "FolderDelete" => Ok(Action::Client(ClientAction::FolderDelete)),
            "FolderRename" => Ok(Action::Client(ClientAction::FolderRename)),
            "SettingSync" => Ok(Action::Client(ClientAction::SettingSync)),
            "SettingModify" => Ok(Action::Client(ClientAction::SettingModify)),
            "SettingDelete" => Ok(Action::Client(ClientAction::SettingDelete)),
            "NoteSyncEnd" => Ok(Action::Server(ServerAction::NoteSyncEnd)),
            "NoteSyncModify" => Ok(Action::Server(ServerAction::NoteSyncModify)),
            "NoteSyncDelete" => Ok(Action::Server(ServerAction::NoteSyncDelete)),
            "NoteSyncRename" => Ok(Action::Server(ServerAction::NoteSyncRename)),
            "NoteSyncMtime" => Ok(Action::Server(ServerAction::NoteSyncMtime)),
            "NoteSyncNeedPush" => Ok(Action::Server(ServerAction::NoteSyncNeedPush)),
            "NoteModifyAck" => Ok(Action::Server(ServerAction::NoteModifyAck)),
            "NoteDeleteAck" => Ok(Action::Server(ServerAction::NoteDeleteAck)),
            "FileSyncUpdate" => Ok(Action::Server(ServerAction::FileSyncUpdate)),
            "FileSyncDelete" => Ok(Action::Server(ServerAction::FileSyncDelete)),
            "FileSyncRename" => Ok(Action::Server(ServerAction::FileSyncRename)),
            "FileSyncMtime" => Ok(Action::Server(ServerAction::FileSyncMtime)),
            "FileSyncChunkDownload" => Ok(Action::Server(ServerAction::FileSyncChunkDownload)),
            "FileUpload" => Ok(Action::Server(ServerAction::FileUpload)),
            "FileUploadAck" => Ok(Action::Server(ServerAction::FileUploadAck)),
            "FileDeleteAck" => Ok(Action::Server(ServerAction::FileDeleteAck)),
            "FileSyncEnd" => Ok(Action::Server(ServerAction::FileSyncEnd)),
            "SettingSyncModify" => Ok(Action::Server(ServerAction::SettingSyncModify)),
            "SettingSyncDelete" => Ok(Action::Server(ServerAction::SettingSyncDelete)),
            "SettingSyncRename" => Ok(Action::Server(ServerAction::SettingSyncRename)),
            "SettingSyncMtime" => Ok(Action::Server(ServerAction::SettingSyncMtime)),
            "SettingSyncNeedUpload" => Ok(Action::Server(ServerAction::SettingSyncNeedUpload)),
            "SettingSyncEnd" => Ok(Action::Server(ServerAction::SettingSyncEnd)),
            "SettingModifyAck" => Ok(Action::Server(ServerAction::SettingModifyAck)),
            "FolderSyncModify" => Ok(Action::Server(ServerAction::FolderSyncModify)),
            "FolderSyncDelete" => Ok(Action::Server(ServerAction::FolderSyncDelete)),
            "FolderSyncRename" => Ok(Action::Server(ServerAction::FolderSyncRename)),
            "FolderSyncEnd" => Ok(Action::Server(ServerAction::FolderSyncEnd)),
            _ => Err(format!("Unknown action: {}", s)),
        }
    }
}

// ── Status codes ────────────────────────────────────────────────────────

pub mod status_code {
    pub const SUCCESS: i32 = 1;
    pub const NO_UPDATE: i32 = 6;
    pub const SUCCESS_ALT: i32 = 200;
    pub const PARAM_ERROR: i32 = 305;
    pub const NOTE_SAVE_FAIL: i32 = 433;
    pub const CONTENT_CONFLICT: i32 = 441;
    pub const UPLOAD_SESSION_INVALID: i32 = 463;
    pub const SYNC_CONFLICT: i32 = 490;
}

// ── Message DTOs ────────────────────────────────────────────────────────

/// Generic server response wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response<T = Value> {
    pub code: i32,
    pub status: bool,
    #[serde(default)]
    pub message: String,
    pub data: Option<T>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vault: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
}

/// Authorization request (just a token string)
pub type AuthorizationRequest = String;

/// Authorization response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorizationResponse {
    pub version: String,
    #[serde(rename = "gitTag")]
    pub git_tag: String,
    #[serde(rename = "buildTime")]
    pub build_time: String,
}

/// Client info request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientInfoRequest {
    pub name: String,
    pub version: String,
    #[serde(rename = "type")]
    pub client_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offline_sync_strategy: Option<String>,
}

/// Client info response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientInfoResponse {
    #[serde(rename = "versionIsNew")]
    pub version_is_new: bool,
    #[serde(rename = "versionNewName")]
    pub version_new_name: Option<String>,
    #[serde(rename = "versionNewLink")]
    pub version_new_link: Option<String>,
    #[serde(rename = "pluginVersionIsNew")]
    pub plugin_version_is_new: Option<bool>,
    #[serde(rename = "pluginVersionNewName")]
    pub plugin_version_new_name: Option<String>,
    #[serde(rename = "pluginVersionNewLink")]
    pub plugin_version_new_link: Option<String>,
}

// ── Note DTOs ────────────────────────────────────────────────────────────

/// Note sync request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoteSyncRequest {
    pub vault: String,
    #[serde(rename = "lastTime")]
    pub last_time: i64,
    pub notes: Vec<NoteSyncCheck>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoteSyncCheck {
    pub path: String,
    #[serde(rename = "pathHash")]
    pub path_hash: String,
    #[serde(rename = "contentHash")]
    pub content_hash: String,
    pub ctime: i64,
    pub mtime: i64,
}

/// Note modify/create request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoteModifyRequest {
    pub vault: String,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "pathHash")]
    pub path_hash: Option<String>,
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "contentHash")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "baseHash")]
    pub base_hash: Option<String>,
    #[serde(default)]
    pub ctime: i64,
    #[serde(default)]
    pub mtime: i64,
    #[serde(default)]
    #[serde(rename = "createOnly")]
    pub create_only: bool,
}

/// Note delete request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoteDeleteRequest {
    pub vault: String,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "pathHash")]
    pub path_hash: Option<String>,
}

/// Note rename request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoteRenameRequest {
    pub vault: String,
    #[serde(rename = "oldPath")]
    pub old_path: String,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "oldPathHash")]
    pub old_path_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "pathHash")]
    pub path_hash: Option<String>,
}

/// Note sync end message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoteSyncEndMessage {
    #[serde(rename = "lastTime")]
    pub last_time: i64,
    #[serde(rename = "needUploadCount")]
    pub need_upload_count: i64,
    #[serde(rename = "needModifyCount")]
    pub need_modify_count: i64,
    #[serde(rename = "needSyncMtimeCount")]
    pub need_sync_mtime_count: i64,
    #[serde(rename = "needDeleteCount")]
    pub need_delete_count: i64,
    #[serde(default)]
    pub messages: Vec<QueuedMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
}

/// Queued message in sync end
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedMessage {
    pub action: String,
    pub data: Value,
}

/// Note sync modify message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoteSyncModifyMessage {
    pub path: String,
    #[serde(rename = "pathHash")]
    pub path_hash: String,
    pub content: String,
    #[serde(rename = "contentHash")]
    pub content_hash: String,
    pub ctime: i64,
    pub mtime: i64,
    #[serde(rename = "lastTime")]
    pub last_time: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vault: Option<String>,
}

/// Note sync delete message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoteSyncDeleteMessage {
    pub path: String,
    #[serde(rename = "pathHash")]
    pub path_hash: String,
    #[serde(rename = "lastTime")]
    pub last_time: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vault: Option<String>,
}

/// Note sync rename message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoteSyncRenameMessage {
    #[serde(rename = "oldPath")]
    pub old_path: String,
    #[serde(rename = "oldPathHash")]
    pub old_path_hash: String,
    pub path: String,
    #[serde(rename = "pathHash")]
    pub path_hash: String,
    #[serde(rename = "lastTime")]
    pub last_time: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vault: Option<String>,
}

/// Note sync mtime message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoteSyncMtimeMessage {
    pub path: String,
    #[serde(rename = "pathHash")]
    pub path_hash: String,
    pub mtime: i64,
    #[serde(rename = "lastTime")]
    pub last_time: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vault: Option<String>,
}

/// Note sync need push message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoteSyncNeedPushMessage {
    pub path: String,
    #[serde(rename = "pathHash")]
    pub path_hash: String,
    #[serde(rename = "lastTime")]
    pub last_time: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vault: Option<String>,
}

// ── File DTOs ────────────────────────────────────────────────────────────

/// File sync request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSyncRequest {
    pub vault: String,
    #[serde(rename = "lastTime")]
    pub last_time: i64,
    pub files: Vec<FileSyncCheck>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSyncCheck {
    pub path: String,
    #[serde(rename = "pathHash")]
    pub path_hash: String,
    #[serde(rename = "contentHash")]
    pub content_hash: String,
    pub size: i64,
    pub ctime: i64,
    pub mtime: i64,
}

/// File upload check request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileUploadCheckRequest {
    pub vault: String,
    pub path: String,
    #[serde(rename = "pathHash")]
    pub path_hash: String,
    #[serde(rename = "contentHash")]
    pub content_hash: String,
    pub size: i64,
    pub ctime: i64,
    pub mtime: i64,
}

/// File upload response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileUploadResponse {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    #[serde(rename = "chunkSize")]
    pub chunk_size: i64,
}

/// File sync update message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSyncUpdateMessage {
    pub path: String,
    #[serde(rename = "pathHash")]
    pub path_hash: String,
    #[serde(rename = "contentHash")]
    pub content_hash: String,
    pub size: i64,
    pub ctime: i64,
    pub mtime: i64,
    #[serde(rename = "lastTime")]
    pub last_time: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vault: Option<String>,
}

/// File sync end message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSyncEndMessage {
    #[serde(rename = "lastTime")]
    pub last_time: i64,
    #[serde(rename = "needUploadCount")]
    pub need_upload_count: i64,
    #[serde(rename = "needModifyCount")]
    pub need_modify_count: i64,
    #[serde(rename = "needSyncMtimeCount")]
    pub need_sync_mtime_count: i64,
    #[serde(rename = "needDeleteCount")]
    pub need_delete_count: i64,
    #[serde(default)]
    pub messages: Vec<QueuedMessage>,
}

/// File delete request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileDeleteRequest {
    pub vault: String,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "pathHash")]
    pub path_hash: Option<String>,
}

// ── Folder DTOs ──────────────────────────────────────────────────────────

/// Folder sync request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FolderSyncRequest {
    pub vault: String,
    #[serde(rename = "lastTime")]
    pub last_time: i64,
    pub folders: Vec<FolderSyncCheck>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FolderSyncCheck {
    pub path: String,
    #[serde(rename = "pathHash")]
    pub path_hash: String,
    pub mtime: i64,
}

/// Folder modify request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FolderModifyRequest {
    pub vault: String,
    pub path: String,
}

/// Folder delete request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FolderDeleteRequest {
    pub vault: String,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "pathHash")]
    pub path_hash: Option<String>,
}

/// Folder rename request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FolderRenameRequest {
    pub vault: String,
    #[serde(rename = "oldPath")]
    pub old_path: String,
    pub path: String,
}

/// Folder sync modify message (server → client)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FolderSyncModifyMessage {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vault: Option<String>,
}

/// Folder sync delete message (server → client)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FolderSyncDeleteMessage {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vault: Option<String>,
}

/// Folder sync rename message (server → client)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FolderSyncRenameMessage {
    #[serde(rename = "oldPath")]
    pub old_path: String,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vault: Option<String>,
}

/// Folder sync end message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FolderSyncEndMessage {
    #[serde(rename = "lastTime")]
    pub last_time: i64,
    #[serde(rename = "needModifyCount")]
    pub need_modify_count: i64,
    #[serde(rename = "needDeleteCount")]
    pub need_delete_count: i64,
    #[serde(default)]
    pub messages: Vec<QueuedMessage>,
}

// ── Setting DTOs ─────────────────────────────────────────────────────────

/// Setting sync request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettingSyncRequest {
    pub vault: String,
    #[serde(rename = "lastTime")]
    pub last_time: i64,
    pub settings: Vec<SettingSyncCheck>,
    #[serde(default)]
    pub cover: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettingSyncCheck {
    pub path: String,
    #[serde(rename = "pathHash")]
    pub path_hash: String,
    #[serde(rename = "contentHash")]
    pub content_hash: String,
    pub mtime: i64,
}

/// Setting sync end message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettingSyncEndMessage {
    #[serde(rename = "lastTime")]
    pub last_time: i64,
    #[serde(rename = "needUploadCount")]
    pub need_upload_count: i64,
    #[serde(rename = "needModifyCount")]
    pub need_modify_count: i64,
    #[serde(rename = "needSyncMtimeCount")]
    pub need_sync_mtime_count: i64,
    #[serde(rename = "needDeleteCount")]
    pub need_delete_count: i64,
    #[serde(default)]
    pub messages: Vec<QueuedMessage>,
}

// ── Binary frame helpers ────────────────────────────────────────────────

/// Build a binary chunk frame
pub fn build_binary_chunk(session_id: &str, chunk_index: u32, data: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(2 + SESSION_ID_LEN + CHUNK_INDEX_LEN + data.len());
    frame.extend_from_slice(&PREFIX_BC);

    // Session ID: exactly 36 bytes, padded with null if shorter
    let sid_bytes = session_id.as_bytes();
    frame.extend_from_slice(&sid_bytes[..sid_bytes.len().min(SESSION_ID_LEN)]);
    if sid_bytes.len() < SESSION_ID_LEN {
        frame.extend(std::iter::repeat(0u8).take(SESSION_ID_LEN - sid_bytes.len()));
    }

    // Chunk index: 4 bytes big-endian
    frame.extend_from_slice(&chunk_index.to_be_bytes());

    // Payload
    frame.extend_from_slice(data);

    frame
}

/// Parse a binary chunk frame (without prefix)
pub fn parse_binary_chunk(raw: &[u8]) -> Result<(String, u32, &[u8]), String> {
    if raw.len() < SESSION_ID_LEN + CHUNK_INDEX_LEN {
        return Err(format!(
            "Binary frame too short: {} bytes, need at least {}",
            raw.len(),
            SESSION_ID_LEN + CHUNK_INDEX_LEN
        ));
    }

    let sid = std::str::from_utf8(&raw[..SESSION_ID_LEN])
        .map_err(|e| format!("Invalid session ID: {}", e))?
        .trim_end_matches('\0')
        .to_string();

    let chunk_index = u32::from_be_bytes([
        raw[SESSION_ID_LEN],
        raw[SESSION_ID_LEN + 1],
        raw[SESSION_ID_LEN + 2],
        raw[SESSION_ID_LEN + 3],
    ]);

    let data = &raw[SESSION_ID_LEN + CHUNK_INDEX_LEN..];

    Ok((sid, chunk_index, data))
}

// ── Binary Chunk Builder ────────────────────────────────────────────────

/// Builder for constructing binary chunk frames
#[derive(Debug, Clone)]
pub struct BinaryChunkBuilder {
    session_id: String,
    chunk_index: u32,
    data: Vec<u8>,
}

impl BinaryChunkBuilder {
    /// Create a new builder with session ID
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            chunk_index: 0,
            data: Vec::new(),
        }
    }

    /// Set the chunk index
    pub fn chunk_index(mut self, index: u32) -> Self {
        self.chunk_index = index;
        self
    }

    /// Set the payload data
    pub fn data(mut self, data: impl Into<Vec<u8>>) -> Self {
        self.data = data.into();
        self
    }

    /// Build the binary frame
    pub fn build(&self) -> Vec<u8> {
        build_binary_chunk(&self.session_id, self.chunk_index, &self.data)
    }
}

// ── Binary Chunk Parser ───────────────────────────────────────────────────

/// Parsed binary chunk result
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedChunk {
    pub session_id: String,
    pub chunk_index: u32,
    pub data: Vec<u8>,
}

/// Parser for binary chunk frames
#[derive(Debug, Clone)]
pub struct BinaryChunkParser;

impl BinaryChunkParser {
    /// Parse a complete binary frame (including 2-byte prefix)
    pub fn parse(frame: &[u8]) -> Result<ParsedChunk, String> {
        // Check prefix
        if frame.len() < PREFIX_BC.len() {
            return Err("Frame too short: missing prefix".to_string());
        }
        if frame[..PREFIX_BC.len()] != PREFIX_BC {
            return Err(format!(
                "Invalid prefix: expected {:02x?}, got {:02x?}",
                PREFIX_BC,
                &frame[..PREFIX_BC.len()]
            ));
        }

        // Parse the rest (after prefix)
        let raw = &frame[PREFIX_BC.len()..];
        let (session_id, chunk_index, data) = parse_binary_chunk(raw)?;

        Ok(ParsedChunk {
            session_id,
            chunk_index,
            data: data.to_vec(),
        })
    }

    /// Check if a frame is a binary chunk (starts with prefix)
    pub fn is_binary_frame(data: &[u8]) -> bool {
        data.len() >= PREFIX_BC.len() && data[..PREFIX_BC.len()] == PREFIX_BC
    }
}

// ── Chunk Reassembler ─────────────────────────────────────────────────────

/// Reassembles binary chunks into complete data
#[derive(Debug)]
pub struct ChunkReassembler {
    /// Session ID being reassembled
    session_id: Option<String>,
    /// Ordered chunks by index
    chunks: std::collections::BTreeMap<u32, Vec<u8>>,
    /// Total expected chunks (if known)
    expected_count: Option<u32>,
    /// Highest chunk index seen
    highest_index: u32,
}

impl ChunkReassembler {
    /// Create a new reassembler
    pub fn new() -> Self {
        Self {
            session_id: None,
            chunks: std::collections::BTreeMap::new(),
            expected_count: None,
            highest_index: 0,
        }
    }

    /// Add a chunk to the reassembler
    /// Returns Ok(true) if this chunk completes the data
    pub fn add_chunk(&mut self, chunk: ParsedChunk) -> Result<bool, String> {
        // Validate session ID consistency
        match &self.session_id {
            Some(sid) if sid != &chunk.session_id => {
                return Err(format!(
                    "Session ID mismatch: expected {}, got {}",
                    sid, chunk.session_id
                ));
            }
            _ => {}
        }

        // Set session ID if first chunk
        if self.session_id.is_none() {
            self.session_id = Some(chunk.session_id);
        }

        // Track highest index
        self.highest_index = self.highest_index.max(chunk.chunk_index);

        // Insert chunk (will overwrite duplicate indices)
        self.chunks.insert(chunk.chunk_index, chunk.data);

        // Check if complete (all indices 0..=highest_index present)
        Ok(self.is_complete())
    }

    /// Set expected chunk count (allows early completion detection)
    pub fn set_expected_count(&mut self, count: u32) {
        self.expected_count = Some(count);
    }

    /// Check if all chunks have been received
    /// Requires `expected_count` to be set; without it, completeness cannot be determined
    pub fn is_complete(&self) -> bool {
        match self.expected_count {
            Some(count) => self.chunks.len() >= count as usize,
            None => false,
        }
    }

    /// Get the reassembled data
    /// Returns None if not all chunks received
    pub fn get_data(&self) -> Option<Vec<u8>> {
        if !self.is_complete() {
            return None;
        }

        let count = self.expected_count?;
        let mut result = Vec::new();

        for i in 0..count {
            if let Some(data) = self.chunks.get(&i) {
                result.extend_from_slice(data);
            } else {
                return None;
            }
        }

        Some(result)
    }

    /// Get missing chunk indices
    pub fn missing_indices(&self) -> Vec<u32> {
        let max_index = self
            .expected_count
            .map(|c| c - 1)
            .unwrap_or(self.highest_index);

        (0..=max_index)
            .filter(|i| !self.chunks.contains_key(i))
            .collect()
    }

    /// Get current chunk count
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// Get session ID
    pub fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }

    /// Reset the reassembler
    pub fn reset(&mut self) {
        self.session_id = None;
        self.chunks.clear();
        self.expected_count = None;
        self.highest_index = 0;
    }
}

impl Default for ChunkReassembler {
    fn default() -> Self {
        Self::new()
    }
}

// ── Message encoding/decoding ───────────────────────────────────────────

/// Encode a message to `ACTION|JSON` format
pub fn encode_message<T: Serialize>(
    action: &Action,
    payload: &T,
) -> Result<String, serde_json::Error> {
    let json = serde_json::to_string(payload)?;
    Ok(format!("{}{}{}", action, SEPARATOR, json))
}

/// Encode a simple string message (like Authorization token)
pub fn encode_simple_message(action: &Action, payload: &str) -> String {
    format!("{}{}{}", action, SEPARATOR, payload)
}

/// Decode a message from `ACTION|JSON` format
pub fn decode_message(text: &str) -> Result<(Action, Value), String> {
    let idx = text.find(SEPARATOR);

    let (action_str, json_str) = match idx {
        Some(i) => (&text[..i], &text[i + SEPARATOR.len()..]),
        None => (text, "{}"),
    };

    let action: Action = action_str.parse()?;

    let data =
        serde_json::from_str(json_str).unwrap_or_else(|_| Value::String(json_str.to_string()));

    Ok((action, data))
}

/// Decode a message with typed payload
pub fn decode_message_as<T: DeserializeOwned>(text: &str) -> Result<(Action, T), String> {
    let (action, value) = decode_message(text)?;
    let payload = serde_json::from_value(value)
        .map_err(|e| format!("Failed to deserialize payload: {}", e))?;
    Ok((action, payload))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_message() {
        let action = Action::Client(ClientAction::Authorization);
        let msg = encode_simple_message(&action, "test-token");
        assert_eq!(msg, "Authorization|test-token");
    }

    #[test]
    fn test_decode_message() {
        let (action, data) = decode_message("Authorization|\"test-token\"").unwrap();
        assert_eq!(action, Action::Client(ClientAction::Authorization));
        assert_eq!(data, Value::String("test-token".to_string()));
    }

    #[test]
    fn test_binary_chunk_roundtrip() {
        let session_id = "abc123-def456-ghi789";
        let chunk_index = 42u32;
        let data = b"hello world";

        let frame = build_binary_chunk(session_id, chunk_index, data);

        // Skip the prefix for parsing
        let raw = &frame[2..];
        let (sid, idx, payload) = parse_binary_chunk(raw).unwrap();

        assert_eq!(sid, session_id);
        assert_eq!(idx, chunk_index);
        assert_eq!(payload, data);
    }

    // ── BinaryChunkBuilder Tests ────────────────────────────────────────

    #[test]
    fn test_builder_basic() {
        let frame = BinaryChunkBuilder::new("test-session-id")
            .chunk_index(5)
            .data(b"test payload")
            .build();

        assert!(BinaryChunkParser::is_binary_frame(&frame));

        let parsed = BinaryChunkParser::parse(&frame).unwrap();
        assert_eq!(parsed.session_id, "test-session-id");
        assert_eq!(parsed.chunk_index, 5);
        assert_eq!(parsed.data, b"test payload");
    }

    #[test]
    fn test_builder_empty_data() {
        let frame = BinaryChunkBuilder::new("sid")
            .chunk_index(0)
            .data(&[][..])
            .build();

        let parsed = BinaryChunkParser::parse(&frame).unwrap();
        assert_eq!(parsed.data, Vec::<u8>::new());
    }

    #[test]
    fn test_builder_session_id_padding() {
        // Session ID shorter than 36 bytes should be padded
        let short_id = "short";
        let frame = BinaryChunkBuilder::new(short_id)
            .chunk_index(0)
            .data(b"x")
            .build();

        let parsed = BinaryChunkParser::parse(&frame).unwrap();
        assert_eq!(parsed.session_id, short_id);
    }

    #[test]
    fn test_builder_session_id_truncation() {
        // Session ID longer than 36 bytes should be truncated
        let long_id = "this-is-a-very-long-session-id-that-exceeds-36-bytes";
        let frame = BinaryChunkBuilder::new(long_id)
            .chunk_index(0)
            .data(b"x")
            .build();

        let parsed = BinaryChunkParser::parse(&frame).unwrap();
        assert_eq!(parsed.session_id.len(), 36);
        assert!(long_id.starts_with(&parsed.session_id));
    }

    // ── BinaryChunkParser Tests ────────────────────────────────────────

    #[test]
    fn test_parser_invalid_prefix() {
        let bad_frame = vec![0x01, 0x02, 0x03, 0x04];
        let result = BinaryChunkParser::parse(&bad_frame);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid prefix"));
    }

    #[test]
    fn test_parser_too_short() {
        let short_frame = vec![b'0', b'0'];
        let result = BinaryChunkParser::parse(&short_frame);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("too short"));
    }

    #[test]
    fn test_parser_is_binary_frame() {
        let binary = vec![b'0', b'0', 0x01, 0x02];
        let text = b"Hello|{}";

        assert!(BinaryChunkParser::is_binary_frame(&binary));
        assert!(!BinaryChunkParser::is_binary_frame(text));
    }

    #[test]
    fn test_parser_roundtrip_large_data() {
        let data: Vec<u8> = (0..=255).cycle().take(10000).collect();
        let frame = BinaryChunkBuilder::new("large-data-test")
            .chunk_index(123)
            .data(data.clone())
            .build();

        let parsed = BinaryChunkParser::parse(&frame).unwrap();
        assert_eq!(parsed.data, data);
    }

    // ── ChunkReassembler Tests ────────────────────────────────────────

    #[test]
    fn test_reassembler_single_chunk() {
        let mut reassembler = ChunkReassembler::new();
        reassembler.set_expected_count(1);

        let chunk = ParsedChunk {
            session_id: "test-session".to_string(),
            chunk_index: 0,
            data: vec![1, 2, 3, 4],
        };

        let complete = reassembler.add_chunk(chunk).unwrap();
        assert!(complete);

        let data = reassembler.get_data().unwrap();
        assert_eq!(data, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_reassembler_multiple_chunks() {
        let mut reassembler = ChunkReassembler::new();
        reassembler.set_expected_count(3);

        let chunk0 = ParsedChunk {
            session_id: "session".to_string(),
            chunk_index: 0,
            data: vec![1, 2],
        };
        let chunk1 = ParsedChunk {
            session_id: "session".to_string(),
            chunk_index: 1,
            data: vec![3, 4],
        };
        let chunk2 = ParsedChunk {
            session_id: "session".to_string(),
            chunk_index: 2,
            data: vec![5, 6],
        };

        assert!(!reassembler.add_chunk(chunk0).unwrap());
        assert!(!reassembler.add_chunk(chunk1).unwrap());
        assert!(reassembler.add_chunk(chunk2).unwrap());

        let data = reassembler.get_data().unwrap();
        assert_eq!(data, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_reassembler_out_of_order() {
        let mut reassembler = ChunkReassembler::new();
        reassembler.set_expected_count(3);

        let chunk2 = ParsedChunk {
            session_id: "session".to_string(),
            chunk_index: 2,
            data: vec![5, 6],
        };
        let chunk0 = ParsedChunk {
            session_id: "session".to_string(),
            chunk_index: 0,
            data: vec![1, 2],
        };
        let chunk1 = ParsedChunk {
            session_id: "session".to_string(),
            chunk_index: 1,
            data: vec![3, 4],
        };

        reassembler.add_chunk(chunk2).unwrap();
        reassembler.add_chunk(chunk0).unwrap();
        let complete = reassembler.add_chunk(chunk1).unwrap();

        assert!(complete);
        let data = reassembler.get_data().unwrap();
        assert_eq!(data, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_reassembler_session_mismatch() {
        let mut reassembler = ChunkReassembler::new();

        let chunk1 = ParsedChunk {
            session_id: "session-a".to_string(),
            chunk_index: 0,
            data: vec![1],
        };
        let chunk2 = ParsedChunk {
            session_id: "session-b".to_string(),
            chunk_index: 1,
            data: vec![2],
        };

        reassembler.add_chunk(chunk1).unwrap();
        let result = reassembler.add_chunk(chunk2);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Session ID mismatch"));
    }

    #[test]
    fn test_reassembler_expected_count() {
        let mut reassembler = ChunkReassembler::new();
        reassembler.set_expected_count(3);

        let chunk0 = ParsedChunk {
            session_id: "session".to_string(),
            chunk_index: 0,
            data: vec![1],
        };
        let chunk1 = ParsedChunk {
            session_id: "session".to_string(),
            chunk_index: 1,
            data: vec![2],
        };
        let chunk2 = ParsedChunk {
            session_id: "session".to_string(),
            chunk_index: 2,
            data: vec![3],
        };

        reassembler.add_chunk(chunk0).unwrap();
        reassembler.add_chunk(chunk1).unwrap();
        assert!(reassembler.add_chunk(chunk2).unwrap());
    }

    #[test]
    fn test_reassembler_missing_indices() {
        let mut reassembler = ChunkReassembler::new();

        let chunk0 = ParsedChunk {
            session_id: "session".to_string(),
            chunk_index: 0,
            data: vec![1],
        };
        let chunk2 = ParsedChunk {
            session_id: "session".to_string(),
            chunk_index: 2,
            data: vec![3],
        };

        reassembler.add_chunk(chunk0).unwrap();
        reassembler.add_chunk(chunk2).unwrap();

        let missing = reassembler.missing_indices();
        assert_eq!(missing, vec![1]);
    }

    #[test]
    fn test_reassembler_reset() {
        let mut reassembler = ChunkReassembler::new();
        reassembler.set_expected_count(1);

        let chunk = ParsedChunk {
            session_id: "session".to_string(),
            chunk_index: 0,
            data: vec![1, 2, 3],
        };

        reassembler.add_chunk(chunk).unwrap();
        assert!(reassembler.is_complete());

        reassembler.reset();
        assert!(!reassembler.is_complete());
        assert_eq!(reassembler.chunk_count(), 0);
        assert!(reassembler.session_id().is_none());
    }

    #[test]
    fn test_reassembler_duplicate_chunk() {
        let mut reassembler = ChunkReassembler::new();
        reassembler.set_expected_count(1);

        let chunk1 = ParsedChunk {
            session_id: "session".to_string(),
            chunk_index: 0,
            data: vec![1, 2],
        };
        let chunk2 = ParsedChunk {
            session_id: "session".to_string(),
            chunk_index: 0,
            data: vec![3, 4],
        };

        reassembler.add_chunk(chunk1).unwrap();
        reassembler.add_chunk(chunk2).unwrap();

        // Second chunk should overwrite first
        let data = reassembler.get_data().unwrap();
        assert_eq!(data, vec![3, 4]);
    }

    // ── Integration Tests ──────────────────────────────────────────────

    #[test]
    fn test_full_roundtrip_with_reassembler() {
        let session_id = "integration-test-session-id";
        let original_data: Vec<u8> = (0..=255).cycle().take(5000).collect();

        // Split into chunks
        let chunk_size = 1000;
        let chunks: Vec<_> = original_data
            .chunks(chunk_size)
            .enumerate()
            .map(|(i, data)| {
                BinaryChunkBuilder::new(session_id)
                    .chunk_index(i as u32)
                    .data(data.to_vec())
                    .build()
            })
            .collect();

        // Reassemble
        let mut reassembler = ChunkReassembler::new();
        reassembler.set_expected_count(chunks.len() as u32);

        for frame in &chunks {
            let parsed = BinaryChunkParser::parse(frame).unwrap();
            reassembler.add_chunk(parsed).unwrap();
        }

        let reassembled = reassembler.get_data().unwrap();
        assert_eq!(reassembled, original_data);
    }
}
