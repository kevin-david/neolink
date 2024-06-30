use anyhow::{anyhow, Result};
use gstreamer::{prelude::*, ClockTime, FlowError};
use gstreamer_app::AppSrc;
use gstreamer_rtsp_server::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::{
    sync::{broadcast::channel as broadcast, watch::channel as watch},
    task::JoinSet,
    time::{sleep, Duration},
};
use tokio_stream::{wrappers::BroadcastStream, Stream, StreamExt};
use tokio_util::sync::CancellationToken;

use crate::common::{Permit, StampedData, UseCounter, VidFormat};
use crate::{
    common::{NeoInstance, StreamConfig, StreamInstance},
    AnyResult,
};

use super::{factory::*, gst::NeoRtspServer};

#[derive(Clone)]
struct PauseAffectors {
    motion: bool,
    push: bool,
    client: bool,
}

/// This handles the stream by activating and deacivating it as required
pub(super) async fn stream_main(
    mut stream_instance: StreamInstance,
    camera: NeoInstance,
    rtsp: &NeoRtspServer,
    users: &HashSet<String>,
    paths: &[String],
) -> Result<()> {
    let mut camera_config = camera.config().await?.clone();
    let name = camera_config.borrow().name.clone();

    let mut curr_pause;
    loop {
        let this_loop_cancel = CancellationToken::new();
        let _drop_guard = this_loop_cancel.clone().drop_guard();

        stream_instance.activate().await?;

        // Wait for a valid stream format to be detected
        stream_instance
            .config
            .wait_for(|config| config.vid_ready())
            .await?;
        // After vid give it 1s to look for audio
        // Ignore timeout but check err
        if let Ok(v) = tokio::time::timeout(
            Duration::from_secs(1),
            stream_instance.config.wait_for(|config| config.aud_ready()),
        )
        .await
        {
            v?;
        }

        curr_pause = camera_config.borrow().pause.clone();

        let last_stream_config = stream_instance.config.borrow().clone();
        let mut thread_stream_config = stream_instance.config.clone();

        let (pause_affector_tx, pause_affector) = watch(PauseAffectors {
            motion: false,
            push: false,
            client: false,
        });
        let pause_affector_tx = Arc::new(pause_affector_tx);

        let mut set = JoinSet::<AnyResult<()>>::new();
        // Handles the on off of the stream with the client pause
        let client_counter = UseCounter::new().await;
        let client_count = client_counter.create_deactivated().await?;

        // Client count affector
        if curr_pause.on_disconnect {
            let thread_name = name.clone();
            let client_count = client_counter.create_deactivated().await?;
            let thread_pause_affector_tx = pause_affector_tx.clone();
            let cancel = this_loop_cancel.clone();
            set.spawn(async move {
                tokio::select! {
                    _ = cancel.cancelled() => AnyResult::Ok(()),
                    v = async {
                        loop {
                            client_count.aquired_users().await?;
                            log::info!("{}: Enabling Client", thread_name);
                            thread_pause_affector_tx.send_modify(|current| {
                                current.client = true;
                            });

                            client_count.dropped_users().await?;
                            log::info!("{}: Pausing Client", thread_name);
                            thread_pause_affector_tx.send_modify(|current| {
                                current.client = false;
                            });
                        }
                    } => v,
                }
            });
        }

        // Motion affector
        if curr_pause.on_motion {
            let thread_name = name.clone();
            let thread_pause_affector_tx = pause_affector_tx.clone();
            let cancel = this_loop_cancel.clone();

            let mut motion = camera.motion().await?;
            let delta = Duration::from_secs_f64(curr_pause.motion_timeout);

            set.spawn(async move {
                tokio::select! {
                    _ = cancel.cancelled() => AnyResult::Ok(()),
                    v = async {
                        loop {
                            motion
                                .wait_for(|md| matches!(md, crate::common::MdState::Start(_)))
                                .await?;
                            log::info!("{}: Enabling Motion", thread_name);
                            thread_pause_affector_tx.send_modify(|current| {
                                current.motion = true;
                            });

                            motion
                                .wait_for(
                                    |md| matches!(md, crate::common::MdState::Stop(n) if n.elapsed()>delta),
                                )
                                .await?;
                            log::info!("{}: Pausing Motion", thread_name);
                            thread_pause_affector_tx.send_modify(|current| {
                                current.motion = false;
                            });
                        }
                    } => v,
                }
            });

            // Push notfications
            let mut pn = camera.push_notifications().await?;
            let mut curr_pn = None;
            let thread_name = name.clone();
            let thread_pause_affector_tx = pause_affector_tx.clone();
            let cancel = this_loop_cancel.clone();
            set.spawn(async move {
                tokio::select! {
                    _ = cancel.cancelled() => AnyResult::Ok(()),
                    v = async {
                        loop {
                            curr_pn = pn
                                .wait_for(|pn| pn != &curr_pn && pn.is_some())
                                .await?
                                .clone();
                            log::info!("{}: Enabling Push Notification", thread_name);
                            thread_pause_affector_tx.send_modify(|current| {
                                current.push = true;
                            });
                            tokio::select! {
                                v = pn.wait_for(|pn| pn != &curr_pn && pn.is_some()) => {
                                    v?;
                                    // If another PN during wait then go back to wait more
                                    continue;
                                }
                                _ = sleep(Duration::from_secs(30)) => {}
                            }
                            log::info!("{}: Pausing Push Notification", thread_name);
                            thread_pause_affector_tx.send_modify(|current| {
                                current.push = false;
                            });
                        }
                    } => v,
                }
            });
        }

        if curr_pause.on_motion || curr_pause.on_disconnect {
            // Take over activation
            let cancel = this_loop_cancel.clone();
            let mut client_activator = stream_instance.activator_handle().await;
            client_activator.deactivate().await?;
            stream_instance.deactivate().await?;
            let mut pause_affector = tokio_stream::wrappers::WatchStream::new(pause_affector);
            let thread_curr_pause = curr_pause.clone();
            set.spawn(async move {
                tokio::select! {
                    _ = cancel.cancelled() => AnyResult::Ok(()),
                    v = async {
                        while let Some(state) = pause_affector.next().await {
                            if thread_curr_pause.on_motion && thread_curr_pause.on_disconnect {
                                if state.client && (state.motion || state.push) {
                                    client_activator.activate().await?;
                                } else {
                                    client_activator.deactivate().await?;
                                }
                            } else if thread_curr_pause.on_motion {
                                if state.motion || state.push {
                                    client_activator.activate().await?;
                                } else {
                                    client_activator.deactivate().await?;
                                }
                            } else if thread_curr_pause.on_disconnect {
                                if state.client {
                                    client_activator.activate().await?;
                                } else {
                                    client_activator.deactivate().await?;
                                }
                            } else {
                                unreachable!()
                            }
                        }
                        AnyResult::Ok(())
                    } => v,
                }
            });
        }

        // This thread jsut keeps it active for 30s after an initial start to build the buffer
        let cancel = this_loop_cancel.clone();
        let mut init_activator = stream_instance.activator_handle().await;
        let init_camera = camera.clone();
        set.spawn(async move {
            tokio::select! {
                _ = cancel.cancelled() => AnyResult::Ok(()),
                v = async {
                    init_activator.activate().await?;
                    let _ = init_camera
                        .run_task(|_| {
                            Box::pin(async move {
                                sleep(Duration::from_secs(30)).await;
                                AnyResult::Ok(())
                            })
                        })
                        .await;
                    init_activator.deactivate().await?;
                    AnyResult::Ok(())
                } => v,
            }
        });

        // Task to just report the number of clients for debug purposes
        let cancel = this_loop_cancel.clone();
        let counter = client_counter.create_deactivated().await?;
        let mut cur_count = 0;
        set.spawn(async move {
            tokio::select! {
                _ = cancel.cancelled() => AnyResult::Ok(()),
                v = async {
                    loop {
                        cur_count = *counter.get_counter().wait_for(|v| v != &cur_count).await?;
                        log::trace!("cur_count: {cur_count:?}");
                    }
                } => v,
            }
        });

        // This runs the actual stream.
        // The select will restart if the stream's config updates
        break tokio::select! {
            v = thread_stream_config.wait_for(|new_conf| new_conf != &last_stream_config) => {
                let v = v?;
                // If stream config changes we reload the stream
                log::info!("{}: Stream Configuration Changed. Reloading Streams", &name);
                log::trace!("    From {:?} to {:?}", last_stream_config, v.clone());
                continue;
            },
            v = camera_config.wait_for(|new_conf| new_conf.pause != curr_pause ) => {
                v?;
                // If pause config changes restart
                log::info!("{}: Pause Configuration Changed. Reloading Streams", &name);
                continue;
            },
            v = stream_run(&name, &stream_instance, rtsp, &last_stream_config, users, paths, client_count) => v,
        };
    }
}

/// This handles the stream itself by creating the factory and pushing messages into it
async fn stream_run(
    name: &str,
    stream_instance: &StreamInstance,
    rtsp: &NeoRtspServer,
    stream_config: &StreamConfig,
    users: &HashSet<String>,
    paths: &[String],
    client_count: Permit,
) -> AnyResult<()> {
    let vidstream = stream_instance.vid.resubscribe();
    let audstream = stream_instance.aud.resubscribe();
    let vid_history = stream_instance.vid_history.clone();
    let aud_history = stream_instance.aud_history.clone();

    // Finally ready to create the factory and connect the stream
    let mounts = rtsp
        .mount_points()
        .ok_or(anyhow!("RTSP server lacks mount point"))?;
    // Create the factory
    let (factory, mut client_rx) = make_factory(stream_config).await?;

    factory.add_permitted_roles(users);

    for path in paths.iter() {
        log::debug!("Path: {}", path);
        mounts.add_factory(path, factory.clone());
    }
    log::info!("{}: Available at {}", name, paths.join(", "));

    let stream_cancel = CancellationToken::new();
    let drop_guard = stream_cancel.clone().drop_guard();
    let mut set = JoinSet::new();
    // Wait for new media client data to come in from the factory
    while let Some(mut client_data) = client_rx.recv().await {
        // New media created
        let vid = client_data.vid.take().map(|data| data.app);
        let aud = client_data.aud.take().map(|data| data.app);

        // This is the data that gets sent to gstreamer thread
        // It represents the combination of the camera stream and the appsrc seek messages
        // At 30fps for 15s with audio you need about 900 frames
        // Therefore the buffer is rather large at 2000
        let (aud_data_tx, aud_data_rx) = broadcast(2000);
        let (vid_data_tx, vid_data_rx) = broadcast(2000);

        // This thread takes the video data from the cam and passed it into the stream
        let mut vidstream = BroadcastStream::new(vidstream.resubscribe());
        let thread_vid_data_tx = vid_data_tx.clone();
        let thread_stream_cancel = stream_cancel.clone();
        let thread_vid_history = vid_history.clone();
        set.spawn(async move {
            let r = tokio::select! {
                _ = thread_stream_cancel.cancelled() => AnyResult::Ok(()),
                v = async {
                    // Send Initial
                    {
                        let history = thread_vid_history.borrow();
                        // let last_ts = history.back().map(|s| s.ts);
                        for data in history.iter() {
                            thread_vid_data_tx.send(
                                // StampedData {
                                //     keyframe: data.keyframe,
                                //     data: data.data.clone(),
                                //     ts: last_ts.unwrap()
                                // }
                                data.clone()
                            )?;
                        }
                    }

                    // Send new
                    while let Some(frame) = vidstream.next().await {
                        if let Ok(data) = frame {
                            thread_vid_data_tx.send(
                                data
                            )?;
                        }
                    };
                    AnyResult::Ok(())
                } => v,
            };
            log::trace!("Stream Vid Media End {r:?}");
            AnyResult::Ok(())
        });

        // This thread takes the audio data from the cam and passed it into the stream
        let mut audstream = BroadcastStream::new(audstream.resubscribe());
        let thread_stream_cancel = stream_cancel.clone();
        let thread_aud_data_tx = aud_data_tx.clone();
        let thread_aud_history = aud_history.clone();
        set.spawn(async move {
            let r = tokio::select! {
                _ = thread_stream_cancel.cancelled() => AnyResult::Ok(()),
                v = async {
                    // Send Initial
                    {
                        let history = thread_aud_history.borrow();
                        // let last_ts = history.back().map(|s| s.ts);
                        for data in history.iter() {
                            thread_aud_data_tx.send(
                                // StampedData {
                                //     keyframe: data.keyframe,
                                //     data: data.data.clone(),
                                //     ts: last_ts.unwrap()
                                // }
                                data.clone()

                            )?;
                        }
                    }

                    // Send new
                    while let Some(frame) = audstream.next().await {
                        if let Ok(data) = frame {
                            thread_aud_data_tx.send(
                                data
                            )?;
                        }
                    };
                    AnyResult::Ok(())
                } => v,
            };
            log::trace!("Stream Aud Media End: {r:?}");
            AnyResult::Ok(())
        });

        // Handles sending the video data into gstreamer
        let thread_stream_cancel = stream_cancel.clone();
        let vid_data_rx = BroadcastStream::new(vid_data_rx).filter(|f| f.is_ok()); // Filter to ignore lagged
        let thread_vid = vid.clone();
        let mut thread_client_count = client_count.subscribe();
        let thread_format = stream_config.vid_format;
        let (ts_tx, ts_rx) = tokio::sync::watch::channel(Duration::ZERO);
        // let fallback_time = Duration::from_secs(3);
        let framerate =
            Duration::from_millis(1000u64 / std::cmp::max(stream_config.fps as u64, 5u64));
        if let Some(thread_vid) = thread_vid {
            set.spawn(async move {
                thread_client_count.activate().await?;
                let r = tokio::select! {
                    _ = thread_stream_cancel.cancelled() => {
                        AnyResult::Ok(())
                    },
                    v = send_to_appsrc(
                        pad_vid(
                            // insert_filler(
                                frametime_stream(
                                    sync_stream(
                                        wait_for_keyframe(
                                            vid_data_rx,
                                        ),
                                        ts_tx,
                                    ),
                                    framerate
                                ),
                            //     thread_format,
                            //     framerate,
                            // ),
                            thread_format,
                        ),
                        &thread_vid
                    ) => {
                        v
                    },
                };
                drop(thread_client_count);
                let _ = thread_vid.end_of_stream();
                r
            });
        }

        // Handles the audio data into gstreamer
        let thread_stream_cancel = stream_cancel.clone();
        let aud_data_rx = BroadcastStream::new(aud_data_rx).filter(|f| f.is_ok()); // Filter to ignore lagged
        let thread_aud = aud.clone();
        let aud_framerate =
            Duration::from_millis(1000u64 / std::cmp::max(stream_config.fps as u64, 5u64));
        if let Some(thread_aud) = thread_aud {
            set.spawn(async move {
                let r = tokio::select! {
                    _ = thread_stream_cancel.cancelled() => {
                        AnyResult::Ok(())
                    },
                    v = send_to_appsrc(
                        frametime_stream(
                            hold_stream(
                                wait_for_keyframe(
                                    aud_data_rx
                                ),
                                ts_rx,
                            ),
                            aud_framerate),
                        &thread_aud) => {
                        v
                    },
                };
                let _ = thread_aud.end_of_stream();
                r
            });
        }
    }
    // At this point the factory has been destroyed
    // Cancel any remaining threads that are trying to send data
    // Although it should be finished already when the appsrcs are dropped
    stream_cancel.cancel();
    drop(drop_guard);
    while set.join_next().await.is_some() {}
    log::trace!("Stream done");
    AnyResult::Ok(())
}

fn check_live(app: &AppSrc) -> Result<()> {
    app.bus().ok_or(anyhow!("App source is closed"))?;
    app.pads()
        .iter()
        .all(|pad| pad.is_linked())
        .then_some(())
        .ok_or(anyhow!("App source is not linked"))
}

#[allow(dead_code)]
fn get_runtime(app: &AppSrc) -> Option<Duration> {
    if let Some(clock) = app.clock() {
        if let Some(time) = clock.time() {
            if let Some(base_time) = app.base_time() {
                let runtime = time.saturating_sub(base_time);
                return Some(Duration::from_micros(runtime.useconds()));
            }
        }
    }
    None
}

// This ensures we start at a keyframe
fn wait_for_keyframe<E, T: Stream<Item = Result<StampedData, E>> + Unpin>(
    mut stream: T,
) -> impl Stream<Item = AnyResult<StampedData>> + Unpin {
    Box::pin(async_stream::stream! {
        let mut found_key = false;
        while let Some(frame) = stream.next().await {
            if let Ok(frame) = frame {
                if frame.keyframe || found_key {
                    found_key = true;
                    yield Ok(frame);
                }
            }
        }
    })
}

// Take a stream of stamped data and release them
// only when they are after a certain time stamp
// This time stamp is sent via a shared watcher
//
// This is used to ensure that the audio does not run
// ahead of the video too much
fn hold_stream<E, T: Stream<Item = Result<StampedData, E>> + Unpin>(
    mut stream: T,
    mut vid_ts: tokio::sync::watch::Receiver<Duration>,
) -> impl Stream<Item = AnyResult<StampedData>> + Unpin {
    Box::pin(async_stream::stream! {
        while let Some(frame) = stream.next().await {
            if let Ok(frame) = frame {
                if frame.ts >= *vid_ts.borrow() {
                    yield Ok(frame);
                } else {
                    // Can't seem to throw an error here as it will cause
                    // a borrow checker to hold over the await
                    let _ = vid_ts.wait_for(|o| *o >= frame.ts).await;
                    yield Ok(frame);
                }
            }
        }
    })
}

/// This is the counter part to [`hold_stream`]
///
/// It just updates the ts in the watcher
fn sync_stream<E, T: Stream<Item = Result<StampedData, E>> + Unpin>(
    mut stream: T,
    vid_ts: tokio::sync::watch::Sender<Duration>,
) -> impl Stream<Item = AnyResult<StampedData>> + Unpin {
    Box::pin(async_stream::stream! {
        while let Some(frame) = stream.next().await {
            if let Ok(frame) = frame {
                vid_ts.send_replace(frame.ts);
                yield Ok(frame);
            }
        }
    })
}

// Take a stream of stamped data and reorder it
// in case they are out of order
// this also releases frames in waves of keyframe so it should replace `hold_stream`
#[allow(dead_code)]
fn ensure_order<E, T: Stream<Item = Result<StampedData, E>> + Unpin>(
    mut stream: T,
) -> impl Stream<Item = AnyResult<StampedData>> + Unpin {
    Box::pin(async_stream::stream! {
        let mut frame_buffer: Vec<StampedData> = vec![];
        while let Some(frame) = stream.next().await {
            if let Ok(frame) = frame {
                if ! frame.keyframe {
                    // Buffer until keyframe and reorder
                    let mut reorder_buffer = vec![];
                    while frame_buffer.last().is_some_and(|v| v.ts > frame.ts) {
                        reorder_buffer.push(frame_buffer.pop().unwrap());
                    }
                    frame_buffer.push(frame);

                    while let Some(frame) = reorder_buffer.pop() {
                        frame_buffer.push(frame);
                    }
                } else {
                    // On key frame flush out
                    for frame in frame_buffer.drain(..) {
                        yield Ok(frame);
                    }
                    yield(Ok(frame));
                }
            }
        }
    })
}

// Take a stream of stamped data pause until
// it is time to display it
fn frametime_stream<E, T: Stream<Item = Result<StampedData, E>> + Unpin>(
    mut stream: T,
    expected_frame_rate: Duration,
) -> impl Stream<Item = AnyResult<StampedData>> + Unpin {
    Box::pin(async_stream::stream! {
        let mut ts_before = Duration::MAX;
        while let Some(frame) = stream.next().await {
            if let Ok(frame) = frame {
                if frame.ts < ts_before {
                    ts_before = frame.ts;
                }
                let wait = std::cmp::min(frame.ts.saturating_sub(ts_before), expected_frame_rate);
                tokio::time::sleep(wait).await;
                ts_before = frame.ts;
                yield Ok(frame);
            }
        }
    })
}

#[allow(dead_code)]
/// Insert filler into the video stream when no frames are comming
/// this should help the stream not be considered dead while
/// we wait for the reconnect
fn insert_filler<E, T: Stream<Item = Result<StampedData, E>> + Unpin>(
    mut stream: T,
    format: VidFormat,
    delay: Duration,
) -> impl Stream<Item = AnyResult<StampedData>> + Unpin {
    let mut last_ts = Duration::ZERO;
    Box::pin(async_stream::stream! {
        while let Some(frame) = tokio::select!{
            v = stream.next() => {
                v
            },
            _ = tokio::time::sleep(delay) => {
                log::trace!("Filling");
                match format {
                    VidFormat::H264 => {
                        Some(Ok(StampedData {
                            data: Arc::new(h264_filler(4096)),
                            keyframe: false,
                            ts: last_ts,
                        }))
                    }
                    VidFormat::H265 => {
                        Some(Ok(StampedData {
                            data: Arc::new(h265_filler(4096)),
                            keyframe: false,
                            ts: last_ts,
                        }))
                    }
                    VidFormat::None => unreachable!(),
                }
            }
        } {
            if let Ok(frame) = frame {
                last_ts = frame.ts;
                yield Ok(frame);
            }
        }
    })
}

fn h265_filler(size: usize) -> Vec<u8> {
    assert!(size >= 6);
    let mut buf = vec![0x0, 0x0, 0x1, 0b01001100, 0x0];
    buf.resize(size, 0xFF);
    buf[size - 1] = 0x80;
    buf
}

fn h264_filler(size: usize) -> Vec<u8> {
    assert!(size >= 5);
    let mut buf = vec![0x0, 0x0, 0x1, 0xC];
    buf.resize(size, 0xFF);
    buf[size - 1] = 0x80;
    buf
}

/// Takes a stream and pads it with filler blocks up to 4kb in h265 or h264 format
fn pad_vid<E, T: Stream<Item = Result<StampedData, E>> + Unpin>(
    mut stream: T,
    format: VidFormat,
) -> impl Stream<Item = AnyResult<StampedData>> + Unpin {
    let min_pad: usize = match format {
        VidFormat::H264 => 5,
        VidFormat::H265 => 6,
        VidFormat::None => unreachable!(),
    };
    Box::pin(async_stream::stream! {
        while let Some(frame) = stream.next().await {
            if let Ok(frame) = frame {
                if (frame.data.len() % 4096) != 0 {
                    let pad_size: usize = (frame.data.len() + min_pad).div_ceil(4096) * 4096 - frame.data.len();
                    let frame = StampedData {
                            keyframe: frame.keyframe,
                            data: Arc::new(
                                match format {
                                    VidFormat::H264 => {
                                        frame.data.iter().chain(
                                            h264_filler(pad_size).iter()
                                        ).copied().collect()
                                    }
                                    VidFormat::H265 => {
                                        frame.data.iter().chain(
                                            h265_filler(pad_size).iter()
                                        ).copied().collect()
                                    }
                                    VidFormat::None => unreachable!(),
                                }
                            ),
                            ts: frame.ts,
                        };
                    yield Ok(frame);
                } else {
                    yield Ok(frame);
                }
            }
        }
    })
}

#[allow(dead_code)]
// This will take a stream and if there is a notibable lack of data
// then it will repeat the last keyframe (if there have been no
// pframes in between)
fn repeat_keyframe<E, T: Stream<Item = Result<StampedData, E>> + Unpin>(
    mut stream: T,
    fallback_time: Duration,
    frame_rate: Duration,
) -> impl Stream<Item = Result<StampedData, E>> + Unpin {
    Box::pin(async_stream::stream! {
        let mut was_repeating = false;
        while let Some(frame) = stream.next().await {
            if let Ok(frame) = frame {
                if frame.keyframe {
                    let repeater = frame.clone();
                    yield Ok(frame);

                    // Wait for either timeout or a new frame
                    let mut fallback_time = fallback_time;
                    loop {
                        tokio::select!{
                            v = stream.next() => {
                                if let Some(frame) = v {
                                    if let Ok(frame) = frame {
                                        if was_repeating {
                                            was_repeating = false;
                                        }
                                        yield Ok(frame);
                                        break;
                                    }
                                } else {
                                    break;
                                }
                            },
                            _ = sleep(fallback_time) => {
                                if !was_repeating {
                                    // This way we only print once
                                    was_repeating = true;
                                }
                                fallback_time = frame_rate;

                                yield Ok(repeater.clone());
                            }
                        }
                    }
                } else {
                    // P frames go through as-is
                    yield Ok(frame);
                }
            }
        }
    })
}

/// Takes a stream and sends it to an appsrc
async fn send_to_appsrc<E, T: Stream<Item = Result<StampedData, E>> + Unpin>(
    mut stream: T,
    appsrc: &AppSrc,
) -> AnyResult<()> {
    let mut ts_0 = Duration::MAX;
    let mut wait_for_iframe = true;
    let mut pools: HashMap<usize, gstreamer::BufferPool> = Default::default();
    let mut paused = true;
    appsrc.set_state(gstreamer::State::Paused).unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::channel::<StampedData>(2000);

    // Run blocking code on a seperate thread
    let appsrc = appsrc.clone();
    std::thread::spawn(move || {
        let r = (move || {
            while let Some(data) = rx.blocking_recv() {
                check_live(&appsrc)?; // Stop if appsrc is dropped
                if wait_for_iframe && !data.keyframe {
                    continue;
                } else if wait_for_iframe {
                    wait_for_iframe = false;
                }
                if ts_0 > data.ts {
                    ts_0 = data.ts;
                }
                let rt = data.ts - ts_0;
                log::trace!(
                    "Sending frame with TimeStamp: {:?} on {}",
                    rt,
                    appsrc.name()
                );
                let buf = {
                    // let mut gst_buf = pool.acquire_buffer(None).unwrap();
                    let msg_size = data.data.len();
                    let pool = pools.entry(msg_size).or_insert_with_key(|size| {
                        let pool = gstreamer::BufferPool::new();
                        let mut pool_config = pool.config();
                        pool_config.set_params(None, (*size) as u32, 8, 0);
                        pool.set_config(pool_config).unwrap();
                        // let (allocator, alloc_parms) = pool.allocator().unwrap();
                        pool.set_active(true).unwrap();
                        pool
                    });
                    let mut gst_buf = pool.acquire_buffer(None).unwrap();
                    // let mut gst_buf = gstreamer::Buffer::with_size(data.data.len()).unwrap();
                    {
                        let gst_buf_mut = gst_buf.get_mut().unwrap();
                        let time = ClockTime::from_useconds(rt.as_micros() as u64);
                        // gst_buf_mut.set_dts(ClockTime::from_useconds(dts));
                        gst_buf_mut.set_dts(time);
                        gst_buf_mut.set_pts(time);
                        let mut gst_buf_data = gst_buf_mut.map_writable().unwrap();
                        gst_buf_data.copy_from_slice(data.data.as_slice());
                    }
                    gst_buf
                };

                match appsrc.push_buffer(buf) {
                    Ok(_) => {
                        // log::info!(
                        //     "Send {}{} on {}",
                        //     data.data.len(),
                        //     if data.keyframe { " (keyframe)" } else { "" },
                        //     appsrc.name()
                        // );
                        Ok(())
                    }
                    Err(FlowError::Flushing) => {
                        // Buffer is full just skip
                        //
                        // But ensure we start with an iframe to reduce gray screens
                        wait_for_iframe = true;
                        log::info!("Buffer full on {}", appsrc.name());
                        Ok(())
                    }
                    Err(e) => Err(anyhow!("Error in streaming: {e:?}")),
                }?;
                if appsrc.current_level_bytes() >= appsrc.max_bytes() * 2 / 3 && paused {
                    appsrc.set_state(gstreamer::State::Playing).unwrap();
                    paused = false;
                } else if appsrc.current_level_bytes() <= appsrc.max_bytes() / 3 && !paused {
                    appsrc.set_state(gstreamer::State::Paused).unwrap();
                    paused = true;
                }
            }
            AnyResult::Ok(())
        })();
        log::trace!("r: {:?}", r);
        r
    });

    // Send to the blocking thread
    while let Some(Ok(data)) = stream.next().await {
        // Start on iframes
        if tx.send(data).await.is_err() {
            break;
        }
    }
    Ok(())
}
