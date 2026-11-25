use clap::{Parser, ValueEnum};

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Transport {
  WebTransport,
  Quic,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum DeliveryMode {
  Subgroup,
  Datagram,
}

#[derive(Debug, Parser)]
#[command(name = "adaptive-analyzer", about = "Adaptive content analyzer for MoQ live video streams")]
pub struct Cli {
  /// MoQ relay URL
  #[arg(long, default_value = "moqt://127.0.0.1:4433")]
  pub server: String,

  /// Video track namespace
  #[arg(long, default_value = "moqtail/test")]
  pub namespace: String,

  /// Video track name
  #[arg(long, default_value = "test")]
  pub track_name: String,

  /// Connection transport
  #[arg(long, value_enum, default_value = "quic")]
  pub transport: Transport,

  /// Object delivery mode
  #[arg(long, value_enum, default_value = "subgroup")]
  pub delivery_mode: DeliveryMode,

  /// Subscription duration in seconds (0 = unlimited)
  #[arg(long, default_value_t = 0)]
  pub duration: u64,

  /// Disable TLS certificate verification (development only)
  #[arg(long, default_value_t = false)]
  pub no_cert_validation: bool,
}
