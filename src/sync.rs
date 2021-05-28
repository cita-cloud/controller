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

#[derive(Debug, Clone, PartialEq)]
pub struct NotifyMessage {
    pub filename: String,
}

pub struct Notifier {
    pub root: String,
    pub sub_path: String,
    pub queue: SegQueue<NotifyMessage>,
}

impl Notifier {
    pub fn new(root: String, sub_path: String) -> Self {
        Self {
            root,
            sub_path,
            queue: SegQueue::new(),
        }
    }

    /*
    pub fn fetch_events(&self) -> Vec<NotifyMessage> {
        let mut events = Vec::new();
        while let Some(msg) = self.queue.pop() {
            events.push(msg);
        }
        events
    }*/

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
            .filter(|e| {
                if e.is_err() {
                    return false;
                }
                let e = e.as_ref().unwrap();
                if e.file_name().into_string().unwrap().starts_with('.') {
                    return false;
                }
                true
            })
            .map(|e| {
                let e = e.unwrap();
                NotifyMessage {
                    filename: e.file_name().into_string().unwrap(),
                }
            })
            .collect()
    }

    pub fn list_blocks(&self, current_block_number: u64) -> Vec<NotifyMessage> {
        let root_path = Path::new(&self.root);
        let path = root_path.join("blocks");

        // walk dir to list files
        let ret = fs::read_dir(path);
        if ret.is_err() {
            return vec![];
        }
        let read_dir = ret.unwrap();

        read_dir
            .filter(|e| {
                if e.is_err() {
                    return false;
                }
                let e = e.as_ref().unwrap();
                let ret = e.file_name().into_string().unwrap().parse::<u64>();
                if ret.is_err() {
                    return false;
                }
                if ret.unwrap() <= current_block_number {
                    return false;
                }
                true
            })
            .map(|e| {
                let e = e.unwrap();
                NotifyMessage {
                    filename: e.file_name().into_string().unwrap(),
                }
            })
            .collect()
    }

    pub fn list_txs(&self) -> Vec<NotifyMessage> {
        let root_path = Path::new(&self.root);
        let path = root_path.join("txs");

        // walk dir to list files
        let ret = fs::read_dir(path);
        if ret.is_err() {
            return vec![];
        }
        let read_dir = ret.unwrap();

        read_dir
            .filter(|e| {
                if e.is_err() {
                    return false;
                }
                let e = e.as_ref().unwrap();
                if !e.file_name().into_string().unwrap().starts_with("new_") {
                    return false;
                }
                true
            })
            .map(|e| {
                let e = e.unwrap();
                NotifyMessage {
                    filename: e.file_name().into_string().unwrap(),
                }
            })
            .collect()
    }

    pub fn list(&self, current_block_number: u64) {
        match self.sub_path.as_str() {
            "txs" => {
                let msg = self.list_txs();
                for m in msg {
                    self.queue.push(m)
                }
            }
            "proposals" => {
                let msg = self.walk_dir("proposals");
                for m in msg {
                    self.queue.push(m)
                }
            }
            "blocks" => {
                let msg = self.list_blocks(current_block_number);
                for m in msg {
                    self.queue.push(m)
                }
            }
            _ => {
                panic!("unexpected subpath")
            }
        }
    }

    pub async fn watch(&self) {
        let mut inotify = Inotify::init().expect("Failed to initialize inotify");

        let root_path = Path::new(&self.root);

        let path = root_path.join(&self.sub_path);
        inotify.add_watch(path, WatchMask::MOVED_TO).unwrap();

        let mut buffer = vec![0u8; 16384];
        let mut stream = inotify.event_stream(&mut buffer).unwrap();

        while let Some(event_or_error) = stream.next().await {
            if let Ok(event) = event_or_error {
                let c = NotifyMessage {
                    filename: event.name.unwrap().into_string().unwrap(),
                };
                self.queue.push(c);
            }
        }
    }
}
