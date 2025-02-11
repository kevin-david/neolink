use super::*;

use crate::common::PushNoti;
use tokio::sync::watch::channel as watch;

impl NeoInstance {
    pub(crate) async fn uid(&self) -> Result<String> {
        let (reply_tx, reply_rx) = oneshot();
        self.camera_control
            .send(NeoCamCommand::GetUid(reply_tx))
            .await?;
        Ok(reply_rx.await?)
    }

    pub(crate) async fn push_notifications(&self) -> Result<WatchReceiver<Option<PushNoti>>> {
        let uid = self.uid().await?;
        let (instance_tx, instance_rx) = oneshot();
        self.camera_control
            .send(NeoCamCommand::PushNoti(instance_tx))
            .await?;
        let mut source_watch = instance_rx.await?;

        let (fwatch_tx, fwatch_rx) = watch(None);
        tokio::task::spawn(async move {
            loop {
                match source_watch
                    .wait_for(|i| {
                        fwatch_tx.borrow().as_ref() != i.as_ref()
                            && i.as_ref()
                                .is_some_and(|i| i.message.contains(&format!("\"{uid}\"")))
                    })
                    .await
                {
                    Ok(pn) => {
                        log::trace!("Forwarding push notification about {}", uid);
                        let _ = fwatch_tx.send_replace(pn.clone());
                    }
                    Err(e) => {
                        break Err(e);
                    }
                }
            }?;
            AnyResult::Ok(())
        });

        Ok(fwatch_rx)
    }
}
