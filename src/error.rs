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

use cita_cloud_proto::common::Hash;

/// The error types
#[derive(Debug)]
pub enum Error {
    /// node in misbehave list
    MisbehaveNode,

    /// node in ban list
    BannedNode,

    /// message not provide address
    NoProvideAddress,

    /// not get the block
    NoBlock(u64),

    /// proposal not found
    NoProposal(Vec<u8>),

    /// block header is none
    NoneBlockHeader,

    /// early status received
    EarlyStatus,

    /// proto struct encode error
    EncodeError(String),

    /// proto struct encode error
    DecodeError(String),

    /// no candidate block
    NoCandidate,

    /// fork tree no block
    NoForkTree,

    /// find dup transaction
    DupTransaction(Hash),

    /// internal error, todo
    InternalError(Box<dyn std::error::Error + Send + Sync>),

    /// other errors, todo
    ExpectError(String),
}

impl ::std::error::Error for Error {}

impl ::std::fmt::Display for Error {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        match self {
            Error::MisbehaveNode => write!(f, "Node already in misbehave list"),
            Error::BannedNode => write!(f, "Node already in ban list"),
            Error::NoProvideAddress => write!(f, "No correct address provide"),
            Error::NoBlock(h) => write!(f, "Not get the {}th block", h),
            Error::NoProposal(hash) => write!(f, "proposal 0x{} not found", hex::encode(hash)),
            Error::NoneBlockHeader => write!(f, "BlockHeader should not be None"),
            Error::EarlyStatus => write!(f, "receive early status from same node"),
            Error::EncodeError(s) => write!(f, "Proto struct encode error: {}", s),
            Error::DecodeError(s) => write!(f, "Proto struct decode error: {}", s),
            Error::NoCandidate => write!(f, "No candidate block"),
            Error::NoForkTree => write!(f, "Fork tree no block"),
            Error::DupTransaction(h) => {
                write!(f, "Found dup transaction 0x{}", hex::encode(h.hash.clone()))
            }
            Error::InternalError(e) => write!(f, "Internal Error: {}", e),
            Error::ExpectError(s) => write!(f, "Expect error: {}", s),
        }
    }
}
