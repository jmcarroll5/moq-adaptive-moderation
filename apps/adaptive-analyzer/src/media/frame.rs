use bytes::Bytes;

/// An object from MoQ track
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VideoObjectId {
    pub group_id: u64,
    pub object_id: u64,
}

/// CMAF fragment
#[derive(Debug, Clone)]
pub struct EncodedFragment {
    pub source: VideoObjectId,
    pub payload: Bytes,
}

pub struct DecodedFrame {
    pub source: Option<VideoObjectId>,
    pub width: u32,
    pub height: u32,
    pub pts_us: Option<i64>,
    pub rgb: Vec<u8>,
}