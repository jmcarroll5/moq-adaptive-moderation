use tokio::sync::mpsc;
use tracing::info;
use crate::media::frame::DecodedFrame;

pub async fn run(mut frame_rx: mpsc::Receiver<DecodedFrame>) {
    let mut count = 0u64;

    while let Some(frame) = frame_rx.recv().await {
        count += 1;

        if count <= 5 || count.is_multiple_of(10) {
            info!("Frame {}: {}x{}, pts_us={:?}", count, frame.width, frame.height, frame.pts_us);
        }
    }
}