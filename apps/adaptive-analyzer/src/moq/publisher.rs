use std::sync::Arc;
use std::time::Duration;
use anyhow::Result;
use bytes::Bytes;
use moqtail::model::common::tuple::{Tuple, TupleField};
use moqtail::model::control::control_message::ControlMessage;
use moqtail::model::control::publish::Publish;
use moqtail::model::data::constant::DEFAULT_PUBLISHER_PRIORITY;
use moqtail::model::data::object::Object;
use moqtail::model::data::subgroup_header::SubgroupHeader;
use moqtail::model::data::subgroup_object::SubgroupObject;
use moqtail::model::parameter::message_parameter::MessageParameter;
use moqtail::transport::connection::TransportConnection;
use moqtail::transport::control_stream_handler::ControlStreamHandler;
use moqtail::transport::data_stream_handler::{HeaderInfo, SendDataStream};
use tokio::sync::Mutex;
use tracing::info;

const TRACK_NAME: &str = "filter";
const TRACK_ALIAS: u64 = 1;
const REQUEST_ID: u64 = 2;
const TEST_INTERVAL: Duration = Duration::from_secs(1);

/// Registers the filter track and publishes test JSON messages.
pub async fn run(connection: Arc<TransportConnection>, namespace: String) -> Result<()> {
    let namespace_tuple = Tuple::from_utf8_path(&namespace);

    let publish = Publish::new(
        REQUEST_ID,
        namespace_tuple,
        TupleField::from_utf8(TRACK_NAME),
        TRACK_ALIAS,
        vec![MessageParameter::Forward { forward: true }],
        vec![],
    );

    // Open a bidirectional stream for the PUBLISH request.
    let (send, recv) = connection.open_bi().await?;
    let mut request_stream = ControlStreamHandler::new(send, recv);

    // Send PUBLISH to the relay.
    request_stream.send(&ControlMessage::Publish(Box::new(publish))).await
        .map_err(|error| anyhow::anyhow!("Failed to publish filter track: {error:?}"))?;

    // Wait for acknowledgement.
    match request_stream.next_message().await {
        Ok(ControlMessage::RequestOk(response)) => {
            response.validate_track_properties(false)
                .map_err(|error| anyhow::anyhow!("Invalid PUBLISH response: {error:?}"))?;
            info!("Filter track published: {namespace}/{TRACK_NAME}");
        }
        Ok(other) => anyhow::bail!("Expected REQUEST_OK for filter track, received {other:?}"),
        Err(error) => anyhow::bail!("Failed waiting for filter PUBLISH response: {error:?}"),
    }

    // Testing by sending objects
    let mut group_id = 0u64;
    loop {
        let json = format!(r#"{{"type":"test","sequence":{group_id},"message":"Hello from Rust analyzer"}}"#);

        send_test_object(&connection, group_id, json.clone()).await?;
        info!("Published filter test object: group={group_id}, object=0, payload={json}");

        group_id = group_id.checked_add(1).ok_or_else(|| anyhow::anyhow!("Filter group ID overflow"))?;
        tokio::time::sleep(TEST_INTERVAL).await;
    }
}

async fn send_test_object(connection: &TransportConnection, group_id: u64, json: String) -> Result<()> {
    let stream = Arc::new(Mutex::new(connection.open_uni().await?));

    let header = SubgroupHeader::new_with_explicit_id(
        TRACK_ALIAS,
        group_id,
        1,
        Some(DEFAULT_PUBLISHER_PRIORITY),
        true,
        true,
        true,
    );

    let mut handler = SendDataStream::new(stream, HeaderInfo::Subgroup { header }).await?;

    let subgroup_object = SubgroupObject {
        object_id: 0,
        properties: Some(vec![]),
        object_status: None,
        payload: Some(Bytes::from(json)),
    };

    let object = Object::try_from_subgroup(
        subgroup_object,
        TRACK_ALIAS,
        group_id,
        Some(1),
        Some(DEFAULT_PUBLISHER_PRIORITY),
        DEFAULT_PUBLISHER_PRIORITY,
    )?;

    handler.send_object(&object, None).await?;
    handler.flush().await?;
    handler.finish().await?;
    Ok(())
}
