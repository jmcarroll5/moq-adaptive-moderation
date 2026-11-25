use std::collections::{HashMap, VecDeque};
use std::io::{self, Read};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use ffmpeg_next as ffmpeg;
use ffmpeg::format::context::StreamIo;
use ffmpeg::media::Type;
use ffmpeg::software::scaling::{context::Context as ScalingContext, flag::Flags};
use ffmpeg::util::frame::video::Video;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::frame::{DecodedFrame, EncodedFragment, VideoObjectId};

type PendingObjects = Arc<Mutex<VecDeque<VideoObjectId>>>;

struct FragmentReader {
    fragment_rx: mpsc::Receiver<EncodedFragment>,
    current_payload: Bytes,
    offset: usize,
    started: bool,
    pending_objects: PendingObjects,
}
impl FragmentReader {
    fn new(init_segment: Vec<u8>, fragments: mpsc::Receiver<EncodedFragment>, pending_objects: PendingObjects) -> Self {
        Self {
            fragment_rx: fragments,
            current_payload: Bytes::from(init_segment),
            offset: 0,
            started: false,
            pending_objects,
        }
    }
}

impl Read for FragmentReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }

        loop {
            // Return remaining bytes in the current segment
            if self.offset < self.current_payload.len() {
                let count = buffer.len().min(self.current_payload.len() - self.offset);
                buffer[..count].copy_from_slice(&self.current_payload[self.offset..self.offset + count]);
                self.offset += count;
                return Ok(count);
            }

            // wait for more encoded fragments
            let Some(fragment) = self.fragment_rx.blocking_recv() else {
                return Ok(0);
            };
            // try again if payload is empty
            if fragment.payload.is_empty() {
                continue;
            }

            // start at keyframes, if not 0 continue until a keyframe
            if !self.started {
                if fragment.source.object_id != 0 {
                    continue;
                }
                // mark started for next non key frames (not 0 id)
                self.started = true;
                info!("Starting video decoding at group={}", fragment.source.group_id);
            }

            // saves the objectID
            let source = fragment.source;
            self.pending_objects.lock()
                .map_err(|_| io::Error::other("Pending object queue poisoned"))?
                .push_back(source);

            self.current_payload = fragment.payload;
            self.offset = 0;
        }
    }
}

/// Creates channels for encoded fragments and decoded frames, and starts ffmpeg decoder
pub fn spawn_decoder(
    init_segment: Vec<u8>,
) -> (
    mpsc::Sender<EncodedFragment>,
    mpsc::Receiver<DecodedFrame>,
    tokio::task::JoinHandle<Result<()>>,
) {
    let (fragment_tx, fragment_rx) = mpsc::channel::<EncodedFragment>(64);
    let (frame_tx, frame_rx) = mpsc::channel::<DecodedFrame>(4);

    let decoder_task = tokio::task::spawn_blocking(move || {
        run_decoder(init_segment, fragment_rx, frame_tx)
    });

    (fragment_tx, frame_rx, decoder_task)
}

/// Runs the FFmpeg decoding
///
/// This function is blocking and owns a FFmpeg decoder for the entire video stream.
pub fn run_decoder(
    init_segment: Vec<u8>,
    fragment_rx: mpsc::Receiver<EncodedFragment>, // receives cmaf fragments
    frame_tx: mpsc::Sender<DecodedFrame>, // sends rgb frames
) -> Result<()> {
    if init_segment.is_empty() {
        bail!("Cannot initialize FFmpeg without an MP4 initialization segment");
    }
    ffmpeg::init().context("Failed to initialize FFmpeg")?;

    let pending_objects = Arc::new(Mutex::new(VecDeque::<VideoObjectId>::new()));
    let reader = FragmentReader::new(init_segment, fragment_rx, pending_objects.clone());
    let io = StreamIo::from_read(reader).context("Failed to create FFmpeg input stream")?;

    // FFmpeg reads initialization segment then CMAF fragments
    let mut input = ffmpeg::format::input_from_stream(io, Some("live.mp4"), None)
        .context("Failed to open fragmented MP4 stream")?;

    let video_stream = input.streams().best(Type::Video).context("No video stream found")?;
    let stream_index = video_stream.index();
    let time_base = video_stream.time_base();

    let decoder_context = ffmpeg::codec::context::Context::from_parameters(video_stream.parameters())
        .context("Failed to create video decoder context")?;

    let mut decoder = decoder_context.decoder().video().context("Failed to open video decoder")?;

    info!("FFmpeg video decoder initialized");
    info!("Video stream index={}, time base={}/{}", stream_index, time_base.numerator(), time_base.denominator());

    let mut scaler: Option<(ffmpeg::format::Pixel, u32, u32, ScalingContext)> = None;
    let mut frame_count = 0u64;

    // Maps ffmpeg timestamps to MoQ objectIDs
    let mut frame_sources = HashMap::<i64, VideoObjectId>::new();

    loop {
        let mut packet = ffmpeg::Packet::empty();

        match packet.read(&mut input) {
            Ok(()) => {}
            Err(ffmpeg::Error::Eof) => break,
            Err(error) => return Err(error).context("Failed to read an MP4 packet"),
        }

        if packet.stream() != stream_index {
            continue;
        }

        // Match the next video packet with the next MoQ object.
        // currently assumes one video sample per cmaf object
        let source = pending_objects.lock()
            .map_err(|_| anyhow::anyhow!("Pending object queue poisoned"))?
            .pop_front();

        if let Some(source) = source {
            if let Some(pts) = packet.pts() {
                if frame_sources.insert(pts, source).is_some() {
                    warn!("Duplicate packet PTS={pts}; replacing previous source identity");
                }
            } else {
                warn!("Video packet has no PTS: source={source:?}");
            }
        } else {
            warn!("No MoQ object identity available for video packet: pts={:?}", packet.pts());
        }

        // send h264 to ffmpeg
        match decoder.send_packet(&packet) {
            Ok(()) => {}
            Err(ffmpeg::Error::Other { errno }) if errno == ffmpeg::error::EAGAIN => {
                drain_frames(&mut decoder, &mut scaler, time_base, &frame_tx, &mut frame_count, &mut frame_sources)?;
                decoder.send_packet(&packet).context("Failed to submit video packet")?;
            }
            Err(error) => return Err(error).context("Failed to submit video packet"),
        }

        drain_frames(&mut decoder, &mut scaler, time_base, &frame_tx, &mut frame_count, &mut frame_sources)?;
    }

    // flush decoder when input ends
    decoder.send_eof().context("Failed to flush video decoder")?;
    drain_frames(&mut decoder, &mut scaler, time_base, &frame_tx, &mut frame_count, &mut frame_sources)?;

    info!("Video decoder finished: {} frames decoded", frame_count);
    Ok(())
}

/// Retrieves decoded frames from ffmpeg and converts to rgb frames using convert_frame()
fn drain_frames(
    decoder: &mut ffmpeg::decoder::Video,
    scaler: &mut Option<(ffmpeg::format::Pixel, u32, u32, ScalingContext)>,
    time_base: ffmpeg::Rational,
    sender: &mpsc::Sender<DecodedFrame>,    //where to send decoded frame
    frame_count: &mut u64,  // number of decoded frames
    frame_sources: &mut HashMap<i64, VideoObjectId>, //maps ffmpeg timestamp with MoQ objectID
) -> Result<()> {
    loop {
        let mut decoded = Video::empty();

        match decoder.receive_frame(&mut decoded) {
            Ok(()) => {}
            Err(ffmpeg::Error::Other { errno }) if errno == ffmpeg::error::EAGAIN => break,
            Err(ffmpeg::Error::Eof) => break,
            Err(error) => return Err(error).context("Failed to receive decoded video frame"),
        }

        let mut frame = convert_frame(&decoded, scaler, time_base)?;

        // map pts with original MoQ objectID
        frame.source = decoded.pts().and_then(|pts| frame_sources.remove(&pts));

        *frame_count += 1;

        if *frame_count <= 5 || (*frame_count).is_multiple_of(10) {
            info!(
                "Decoded frame {}: {}x{}, pts_us={:?}, source={:?}",
                *frame_count, frame.width, frame.height, frame.pts_us, frame.source
            );
        } else {
            debug!("Decoded frame {}: pts_us={:?}, source={:?}", *frame_count, frame.pts_us, frame.source);
        }

        if frame.source.is_none() {
            warn!("Could not match decoded frame {} to a MoQ object: pts_us={:?}", *frame_count, frame.pts_us);
        }

        sender.blocking_send(frame).context("Decoded frame receiver closed")?;
    }

    Ok(())
}

/// convert ffmpeg frame to rgb frame
fn convert_frame(
    decoded: &Video,    //ffmpeg decoded video
    scaler: &mut Option<(ffmpeg::format::Pixel, u32, u32, ScalingContext)>, //ffmpeg uses to convert frame
    time_base: ffmpeg::Rational,    //ffmpeg converts time
) -> Result<DecodedFrame> {
    let width = decoded.width();
    let height = decoded.height();
    let format = decoded.format();

    let needs_scaler = match scaler {
        Some((current_format, current_width, current_height, _)) => {
            *current_format != format || *current_width != width || *current_height != height
        }
        None => true,
    };

    if needs_scaler {
        let context = ScalingContext::get(
            format,
            width,
            height,
            ffmpeg::format::Pixel::RGB24,
            width,
            height,
            Flags::BILINEAR,
        )
            .context("Failed to initialize RGB conversion")?;

        *scaler = Some((format, width, height, context));
    }

    let mut rgb_frame = Video::empty();

    scaler.as_mut().context("RGB scaler not initialized")?.3
        .run(decoded, &mut rgb_frame)
        .context("Failed to convert frame to RGB24")?;

    // do not copy padding
    let row_bytes = (width as usize).checked_mul(3).context("Invalid RGB row size")?;
    let total_bytes = row_bytes.checked_mul(height as usize).context("Invalid RGB frame size")?;
    let stride = rgb_frame.stride(0);
    let plane = rgb_frame.data(0);

    let mut rgb = Vec::with_capacity(total_bytes);

    for row in 0..height as usize {
        let start = row * stride;
        let end = start + row_bytes;
        let pixels = plane.get(start..end).context("Invalid RGB plane layout")?;
        rgb.extend_from_slice(pixels);
    }

    // convert pts to us
    let pts_us = decoded.pts().and_then(|pts| {
        let numerator = i128::from(time_base.numerator());
        let denominator = i128::from(time_base.denominator());

        if denominator == 0 {
            return None;
        }

        let microseconds = i128::from(pts) * numerator * 1_000_000 / denominator;
        i64::try_from(microseconds).ok()
    });

    Ok(DecodedFrame {
        source: None,
        width,
        height,
        pts_us,
        rgb,
    })
}
