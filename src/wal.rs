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

use crc32fast::Hasher as CrcHasher;
use log::warn;
use std::fs::{read_dir, DirBuilder, File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::str;
use std::{
    collections::{btree_map::Entry, BTreeMap},
    convert::TryInto,
};

const DELETE_FILE_INTERVAL: u64 = 8;
const INDEX_NAME: &str = "index";

#[derive(Debug, Clone, Copy)]
pub enum LogType {
    Skip = 0,
    Propose = 1,
    QuorumVotes = 2,
    FinalizeBlock = 3,
}

impl From<u8> for LogType {
    fn from(s: u8) -> LogType {
        match s {
            1 => LogType::Propose,
            2 => LogType::QuorumVotes,
            3 => LogType::FinalizeBlock,
            _ => LogType::Skip,
        }
    }
}

pub struct Wal {
    height_fs: BTreeMap<u64, File>,
    dir: String,
    current_height: u64,
    ifile: File,
}

impl Wal {
    fn delete_old_file(dir: &str, current_height: u64) -> io::Result<()> {
        for entry in read_dir(dir)? {
            let entry = entry?;
            let fpath = entry.path();
            if let Some(fname) = fpath.file_name() {
                let strs: Vec<&str> = fname.to_str().unwrap().split('.').collect();
                if !strs.is_empty() {
                    let num = strs[0].parse::<u64>().unwrap_or(current_height);
                    if num + DELETE_FILE_INTERVAL < current_height {
                        ::std::fs::remove_file(fpath)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn create(dir: &str) -> io::Result<Wal> {
        let fss = read_dir(&dir);
        if fss.is_err() {
            DirBuilder::new().recursive(true).create(dir)?;
        }

        let file_path = dir.to_string() + "/" + INDEX_NAME;
        let mut ifs = OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .open(file_path)?;
        ifs.seek(io::SeekFrom::Start(0)).unwrap();

        let mut string_buf: String = String::new();
        let res_fsize = ifs.read_to_string(&mut string_buf)?;
        let num_str = string_buf.trim();
        let cur_height: u64;
        let last_file_path: String;
        if res_fsize == 0 {
            last_file_path = dir.to_string() + "/1.log";
            cur_height = 1;
        } else {
            let hi_res = num_str.parse::<u64>();
            if let Ok(hi) = hi_res {
                cur_height = hi;
                last_file_path = dir.to_string() + "/" + cur_height.to_string().as_str() + ".log"
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "index file data wrong",
                ));
            }
        }

        Self::delete_old_file(dir, cur_height)?;

        let fs = OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .open(last_file_path)?;

        let mut tmp = BTreeMap::new();
        tmp.insert(cur_height, fs);

        Ok(Wal {
            height_fs: tmp,
            dir: dir.to_string(),
            current_height: cur_height,
            ifile: ifs,
        })
    }

    fn get_file_path(dir: &str, height: u64) -> String {
        let mut name = height.to_string();
        name += ".log";
        let pathname = dir.to_string() + "/";
        pathname + &*name
    }

    fn set_index_file(&mut self, height: u64) -> io::Result<u64> {
        self.current_height = height;
        self.ifile.seek(io::SeekFrom::Start(0))?;
        let hstr = height.to_string();
        let content = hstr.as_bytes();
        let len = content.len() as u64;
        self.ifile.set_len(len)?;
        self.ifile.write_all(content)?;
        self.ifile.sync_data()?;
        Ok(len)
    }

    pub fn set_height(&mut self, height: u64) -> io::Result<u64> {
        let len = self.set_index_file(height)?;
        if let Entry::Vacant(entry) = self.height_fs.entry(height) {
            let filename = Wal::get_file_path(&self.dir, height);
            let fs = OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(filename)?;
            entry.insert(fs);
        }

        if height > DELETE_FILE_INTERVAL {
            let newer_file = self.height_fs.split_off(&(height - DELETE_FILE_INTERVAL));
            for &i in self.height_fs.keys() {
                let delfilename = Wal::get_file_path(&self.dir, i);
                let _ = ::std::fs::remove_file(delfilename);
            }
            self.height_fs = newer_file;
        }
        Ok(len)
    }

    pub fn save(&mut self, height: u64, log_type: LogType, msg: &[u8]) -> io::Result<u64> {
        let mtype = log_type as u8;
        let mlen = msg.len() as u32;
        if mlen == 0 {
            return Ok(0);
        }

        if height > self.current_height && height < self.current_height + DELETE_FILE_INTERVAL {
            if let Entry::Vacant(entry) = self.height_fs.entry(height) {
                let filename = Wal::get_file_path(&self.dir, height);
                let fs = OpenOptions::new()
                    .read(true)
                    .create(true)
                    .write(true)
                    .open(filename)?;
                entry.insert(fs);
            }
        }

        let mut hlen = 0;
        if let Some(fs) = self.height_fs.get_mut(&height) {
            let len_bytes: [u8; 4] = mlen.to_le_bytes();
            let type_bytes: [u8; 1] = [mtype];
            let mut crc = CrcHasher::new();
            crc.update(msg);
            let check_sum = crc.finalize();
            fs.seek(io::SeekFrom::End(0))?;
            fs.write_all(&len_bytes[..])?;
            fs.write_all(&type_bytes[..])?;
            fs.write_all(&check_sum.to_le_bytes())?;
            hlen = fs.write(msg)?;
            fs.flush()?;
        } else {
            warn!(
                "wal not save height {} current height {} ",
                height, self.current_height
            );
        }
        let _ = self.set_height(height);
        Ok(hlen as u64)
    }

    #[allow(dead_code)]
    pub fn get_cur_height(&self) -> u64 {
        self.current_height
    }

    pub fn load(&self) -> Vec<(u8, Vec<u8>)> {
        let mut vec_buf: Vec<u8> = Vec::new();
        let mut vec_out: Vec<(u8, Vec<u8>)> = Vec::new();
        let cur_height = self.current_height;
        if self.height_fs.is_empty() || cur_height == 0 {
            return vec_out;
        }

        for (height, mut fs) in &self.height_fs {
            if *height < self.current_height {
                continue;
            }
            fs.seek(io::SeekFrom::Start(0)).unwrap();
            let res_fsize = fs.read_to_end(&mut vec_buf);
            if res_fsize.is_err() {
                return vec_out;
            }
            let fsize = res_fsize.unwrap();
            let mut index = 0;
            let crc = CrcHasher::new();
            loop {
                if index + 9 > fsize {
                    break;
                }
                let tmp = u32::from_le_bytes(vec_buf[index..index + 4].try_into().unwrap());
                let bodylen = tmp as usize;
                let mtype = vec_buf[index + 4];
                let saved_crc =
                    u32::from_le_bytes(vec_buf[index + 5..index + 9].try_into().unwrap());
                index += 9;
                if index + bodylen > fsize {
                    break;
                }
                let mut crc = crc.clone();

                crc.update(&vec_buf[index..index + bodylen]);

                let check_sum = crc.finalize();
                if check_sum != saved_crc {
                    warn!(
                        "wal crc checked error saved {} check {}",
                        saved_crc, check_sum
                    );
                    break;
                }
                vec_out.push((mtype, vec_buf[index..index + bodylen].to_vec()));
                index += bodylen;
            }
        }
        vec_out
    }

    pub fn clear_file(&mut self) -> io::Result<()> {
        self.height_fs.clear();
        for entry in read_dir(self.dir.clone())? {
            let fpath = entry?.path();
            let fname = fpath.file_name().map(|f| f.to_str()).flatten();
            if let Some(fname) = fname {
                if !fname.contains(INDEX_NAME) {
                    let _ = ::std::fs::remove_file(fpath);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crc32fast::Hasher as CrcHasher;
    #[test]
    fn test_crc() {
        let msg1: Vec<u8> = vec![1, 2, 3, 4, 5];
        let msg2: Vec<u8> = vec![5, 4, 3, 2, 1];

        let mut crc = CrcHasher::new();
        crc.update(&msg1);
        let check_sum1 = crc.finalize();

        let mut crc = CrcHasher::new();
        crc.update(&msg2);
        let check_sum2 = crc.finalize();

        let mut crc = CrcHasher::new();
        crc.update(&msg2);
        let sencond_check2 = crc.finalize();

        let mut crc = CrcHasher::new();
        crc.update(&msg1);
        let sencond_check1 = crc.finalize();

        assert_eq!(check_sum1, sencond_check1);
        assert_eq!(check_sum2, sencond_check2);
    }
}
