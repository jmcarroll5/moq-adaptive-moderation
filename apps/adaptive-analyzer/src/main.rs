mod cli;
mod stats;
mod moq;
mod media;
mod analysis;

use anyhow::Result;
use clap::Parser;
use cli::Cli;
use moq::connection::MoqConnection;
use moq::subscriber::SubscribeConfig;
use tracing::info;
use tracing_subscriber::{EnvFilter, filter::LevelFilter};
use moq::catalog::retrieve_video_init_segment;

use moq::publisher;

use crate::media::decoder;

#[tokio::main]
async fn main() -> Result<()> {
  init_logging();

  // start connection
  let cli = Cli::parse();
  info!("Starting adaptive analyzer: server={}, namespace={}, track={}", cli.server, cli.namespace, cli.track_name);
  let connection = MoqConnection::establish(&cli.server, cli.no_cert_validation, cli.transport).await?;

  // read catalog
  let init_segment = retrieve_video_init_segment(&connection, &cli.namespace, &cli.track_name).await?;
  info!("Retrieved initialization segment: {} bytes", init_segment.len());

  // filter track
  let publishing_connection = connection.connection.clone();
  let publishing_namespace = cli.namespace.clone();

  // run the publisher concurrently
  let publisher_task = tokio::spawn(async move {
    if let Err(error) = publisher::run(publishing_connection, publishing_namespace).await {
      tracing::error!("Filter publisher failed: {error:#}");
    }
  });

  // initialize and run the decoder
  let (fragment_tx, frame_rx, decoder_task) = decoder::spawn_decoder(init_segment);

  // start analyzer
  let analysis_task = tokio::spawn(analysis::run(frame_rx));

  //subscribe to the video track
  let config = SubscribeConfig::for_video(cli.namespace, cli.track_name, cli.delivery_mode, cli.duration);
  let subscription_result = moq::subscriber::run(connection, config, fragment_tx).await;

  // Stop filtered publisher when the video subscription ends
  publisher_task.abort();
  let _ = publisher_task.await;

  // wait for decoder and analyzer to finish
  let decoder_result = decoder_task.await?;
  analysis_task.await?;

  // check for errors
  subscription_result?;
  decoder_result?;

  Ok(())
}

fn init_logging() {
  let env_filter = EnvFilter::builder()
      .with_default_directive(LevelFilter::INFO.into())
      .from_env_lossy();

  tracing_subscriber::fmt()
      .with_target(true)
      .with_level(true)
      .with_env_filter(env_filter)
      .init();
}