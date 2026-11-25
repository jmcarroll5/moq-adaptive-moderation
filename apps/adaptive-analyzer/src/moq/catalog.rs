use crate::moq::connection::MoqConnection;use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use moqtail::model::common::tuple::{Tuple, TupleField};
use moqtail::model::control::constant::GroupOrder;
use moqtail::model::control::control_message::ControlMessage;
use moqtail::model::control::subscribe::Subscribe;
use moqtail::model::error::StreamResetCode;
use moqtail::model::parameter::message_parameter::MessageParameter;
use moqtail::transport::control_stream_handler::ControlStreamHandler;
use moqtail::transport::data_stream_handler::RecvDataStream;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::timeout;
use tracing::{debug, info};

const CATALOG_TRACK_NAME: &str = "catalog";
const CATALOG_REQUEST_ID: u64 = 0;
const CATALOG_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug, Clone, Deserialize)]
pub struct Catalog {
    pub version: String,
    pub tracks: Vec<MediaTrack>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MediaTrack {
    pub name: String,
    pub role: String,
    pub packaging: String,
    pub codec: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub init_data: Option<String>,
}

impl Catalog {
    /// Parse JSON payload from the catalog track
    pub fn from_payload(payload: &[u8]) -> Result<Self> {
        serde_json::from_slice(payload).context("Failed to parse CMSF catalog")
    }

    /// Return all video tracks advertised
    pub fn video_tracks(&self) -> impl Iterator<Item = &MediaTrack> {
        self.tracks.iter().filter(|track| track.role == "video")
    }

    /// Find video track by name
    pub fn video_track(&self, name: &str) -> Option<&MediaTrack> {
        self.video_tracks().find(|track| track.name == name)
    }

    /// Check if track has initialization data
    pub fn is_ready(&self, track_name: Option<&str>) -> bool {
        match track_name {
            Some(name) => self.video_track(name).is_some_and(MediaTrack::has_init_data),
            None => self.video_tracks().any(MediaTrack::has_init_data),
        }
    }
}

impl MediaTrack {
    pub fn has_init_data(&self) -> bool {
        self.init_data.as_ref().is_some_and(|data| !data.is_empty())
    }

    /// Decode base64 MP4 initialization segment.
    pub fn init_segment(&self) -> Result<Option<Vec<u8>>> {
        self.init_data.as_ref().map(|data| {
            STANDARD.decode(data).with_context(|| format!("Invalid initialization data for track '{}'", self.name))
        }).transpose()
    }

    pub fn is_h264_cmaf(&self) -> bool {
        self.packaging == "cmaf" && self.codec.as_deref().is_some_and(|codec| codec.starts_with("avc1."))
    }
}

/// Subscribe to the publisher's catalog and wait until initialization data
/// is available for the requested video track.
pub async fn retrieve_catalog(moq: &MoqConnection, namespace: &str, track_name: Option<&str>) -> Result<Catalog> {
    let connection = &moq.connection;

    info!("Subscribing to catalog: {namespace}/{CATALOG_TRACK_NAME}");

    let subscribe = Subscribe::new_latest_object(
        CATALOG_REQUEST_ID,
        Tuple::from_utf8_path(namespace),
        TupleField::from_utf8(CATALOG_TRACK_NAME),
        vec![
            MessageParameter::new_subscriber_priority(0),
            MessageParameter::new_group_order(GroupOrder::Original),
            MessageParameter::new_forward(true),
        ],
    );

    // Create the subscription request.
    let (send, recv) = connection.open_bi().await?;
    let mut request_stream = ControlStreamHandler::new(send, recv);

    request_stream.send(&ControlMessage::Subscribe(Box::new(subscribe))).await
        .map_err(|error| anyhow::anyhow!("Failed to subscribe to catalog: {error:?}"))?;

    let catalog_alias = match request_stream.next_message().await {
        Ok(ControlMessage::SubscribeOk(response)) => response.track_alias,
        Ok(response) => bail!("Unexpected catalog subscription response: {response:?}"),
        Err(error) => bail!("Catalog subscription failed: {error:?}"),
    };

    info!("Catalog subscription successful, alias={catalog_alias}");

    let pending_fetches = Arc::new(RwLock::new(BTreeMap::new()));

    // Keep the request stream alive while waiting for catalog objects.
    let read_catalog = async {
        loop {
            let stream = connection.accept_uni().await.context("Failed to accept catalog data stream")?;
            let stream_handler = RecvDataStream::new(stream, pending_fetches.clone());
            let mut handler = &stream_handler;

            loop {
                let (next_handler, object) = handler.next_object().await;

                let Some(object) = object else {
                    break;
                };

                handler = next_handler;

                if object.track_alias != catalog_alias {
                    continue;
                }

                let Some(payload) = object.payload else {
                    continue;
                };

                let catalog = Catalog::from_payload(&payload)?;

                debug!("Catalog received: version={}, tracks={}", catalog.version, catalog.tracks.len());

                if catalog.is_ready(track_name) {
                    return Ok::<Catalog, anyhow::Error>(catalog);
                }

                debug!("Waiting for video initialization data...");
            }
        }
    };

    let catalog = timeout(CATALOG_TIMEOUT, read_catalog).await
        .context("Timed out waiting for catalog initialization data")??;

    info!("Catalog ready: {} video track(s)", catalog.video_tracks().count());

    for track in catalog.video_tracks() {
        info!(
            "Video track: name={}, codec={}, packaging={}, resolution={}x{}",
            track.name,
            track.codec.as_deref().unwrap_or("unknown"),
            track.packaging,
            track.width.unwrap_or(0),
            track.height.unwrap_or(0),
        );
    }

    // The catalog subscription is no longer needed
    request_stream.reset_and_stop(StreamResetCode::Cancelled.to_u64());
    
    Ok(catalog)
}

pub async fn retrieve_video_init_segment(moq: &MoqConnection, namespace: &str, track_name: &str) -> Result<Vec<u8>> {
    let catalog = retrieve_catalog(moq, namespace, Some(track_name)).await?;

    let track = catalog.video_track(track_name).context("Video track not found in catalog")?;

    if !track.is_h264_cmaf() {
        anyhow::bail!("Unsupported video format: {}", track.codec.as_deref().unwrap_or("unknown"));
    }

    track.init_segment()?.context("Missing video initialization segment")
}