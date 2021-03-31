// Copyright Rivtower Technologies LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crossbeam::queue::SegQueue;
use inotify::{Inotify, WatchMask};
use std::fs;
use std::path::Path;
use tokio_stream::StreamExt;

pub static SYNC_FOLDERS: [&str; 3] = ["txs", "proposals", "blocks"];

#[derive(Debug, Clone, PartialEq)]
pub struct NotifyMessage {
    pub folder: String,
    pub filename: String,
}

pub struct Notifier {
    pub root: String,
    pub queue: SegQueue<NotifyMessage>,
}

impl Notifier {
    pub fn new(root: String) -> Self {
        Self {
            root,
            queue: SegQueue::new(),
        }
    }

    pub fn fetch_events(&self) -> Vec<NotifyMessage> {
        let mut events = Vec::new();
        while let Some(msg) = self.queue.pop() {
            events.push(msg);
        }
        events
    }

    fn walk_dir(&self, dir: &str) -> Vec<NotifyMessage> {
        let root_path = Path::new(&self.root);
        let path = root_path.join(dir);

        // walk dir to list files
        let ret = fs::read_dir(path);
        if ret.is_err() {
            return vec![];
        }
        let read_dir = ret.unwrap();

        read_dir
            .map(|e| {
                let e = e.unwrap();
                NotifyMessage {
                    folder: dir.to_string(),
                    filename: e.file_name().into_string().unwrap(),
                }
            })
            .collect()
    }

    pub fn list(&self) {
        for dir in SYNC_FOLDERS.iter() {
            let msg = self.walk_dir(dir);
            for m in msg {
                self.queue.push(m)
            }
        }
    }

    pub async fn watch(&self) {
        let mut inotify = Inotify::init().expect("Failed to initialize inotify");

        let root_path = Path::new(&self.root);
        let mut wds = Vec::new();
        for dir in SYNC_FOLDERS.iter() {
            let path = root_path.join(dir);
            let wd = inotify.add_watch(path, WatchMask::MOVED_TO).unwrap();
            wds.push(wd);
        }

        let mut buffer = vec![0u8; 4096];
        let mut stream = inotify.event_stream(&mut buffer).unwrap();

        while let Some(event_or_error) = stream.next().await {
            if let Ok(event) = event_or_error {
                let c = NotifyMessage {
                    folder: {
                        match wds.iter().position(|w| w == &event.wd) {
                            Some(position) => SYNC_FOLDERS[position].to_string(),
                            None => panic!("unexpected wd: {:?}", event.wd),
                        }
                    },
                    filename: event.name.unwrap().into_string().unwrap(),
                };
                self.queue.push(c);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;
    use tempdir::TempDir;
    use tokio::time;

    #[test]
    fn walk_dir_test() {
        let tempdir = TempDir::new("test").unwrap().into_path();
        let n = Notifier::new(tempdir.to_str().unwrap().to_string());

        let path = tempdir.join("txs");
        fs::create_dir(path.clone()).unwrap();
        let f1 = path.join("tx1");
        fs::write(f1, b"tx1").unwrap();

        sleep(Duration::new(5, 0));

        let f2 = path.join("tx2");
        fs::write(f2, b"tx2").unwrap();

        let f3 = path.join("tx3");
        fs::write(f3, b"tx3").unwrap();

        let d4 = path.join("d4");
        fs::create_dir(d4).unwrap();

        let msgs = n.walk_dir("txs");
        let expect_msg0 = NotifyMessage {
            folder: "txs".to_string(),
            filename: "tx3".to_string(),
        };
        let expect_msg1 = NotifyMessage {
            folder: "txs".to_string(),
            filename: "tx2".to_string(),
        };
        let expect_msg2 = NotifyMessage {
            folder: "txs".to_string(),
            filename: "tx1".to_string(),
        };
        assert_eq!(msgs.contains(&expect_msg0), true);
        assert_eq!(msgs.contains(&expect_msg1), true);
        assert_eq!(msgs.contains(&expect_msg2), true);
    }

    #[tokio::test]
    async fn watch_test() {
        let tempdir = TempDir::new("test").unwrap().into_path();
        let n = Arc::new(Notifier::new(tempdir.to_str().unwrap().to_string()));

        let txs_path = tempdir.join("txs");
        fs::create_dir(txs_path.clone()).unwrap();

        let blocks_path = tempdir.join("blocks");
        fs::create_dir(blocks_path.clone()).unwrap();

        let n_clone = n.clone();
        tokio::spawn(async move {
            n_clone.watch().await;
        });

        time::sleep(time::Duration::from_secs(1)).await;

        let f1 = txs_path.join("tx1");
        fs::write(f1, b"tx1").unwrap();

        sleep(Duration::new(1, 0));

        let f2 = blocks_path.join("block1");
        fs::write(f2, b"block1").unwrap();

        time::sleep(time::Duration::from_secs(1)).await;

        assert_eq!(
            n.queue.pop(),
            Some(NotifyMessage {
                folder: "txs".to_string(),
                filename: "tx1".to_string(),
            })
        );
        assert_eq!(
            n.queue.pop(),
            Some(NotifyMessage {
                folder: "blocks".to_string(),
                filename: "block1".to_string(),
            })
        );
        assert_eq!(n.queue.pop().is_none(), true);
    }
}
