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

/// The error types todo reorganize to different module
#[derive(Debug)]
#[allow(dead_code)]
pub enum Error {
    /// node in misbehave list
    MisbehaveNode,

    /// node in ban list
    BannedNode,

    /// address not consistent with record origin
    AddressOriginCheckError,

    /// provide address len is not 20
    ProvideAddressError,

    /// message not provide address
    NoProvideAddress,

    /// not get the block
    NoBlock(u64),

    /// proposal is none
    NoneProposal,

    /// block body is none
    NoneBlockBody,

    /// block header is none
    NoneBlockHeader,

    /// chain status is none
    NoneChainStatus,

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
    DupTransaction(Vec<u8>),

    /// proposal too high
    ProposalTooHigh(u64, u64),

    /// proposal too low
    ProposalTooLow(u64, u64),

    /// proposal check error
    ProposalCheckError,

    /// block hash check error
    BlockCheckError,

    /// the sig of chain status init check error
    CSISigCheckError,

    /// chain version or chain id check error
    VersionOrIdCheckError,

    /// hash check error
    HashCheckError,

    /// hash len is not correct
    HashLenError,

    /// signature len is not correct
    SigLenError,

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
            Error::AddressOriginCheckError => {
                write!(f, "Address not consistent with record origin")
            }
            Error::ProvideAddressError => write!(f, "Provide address len is not 20"),
            Error::NoProvideAddress => write!(f, "No correct address provide"),
            Error::NoBlock(h) => write!(f, "Not get the {}th block", h),
            Error::NoneProposal => write!(f, "Proposal should not be none"),
            Error::NoneBlockBody => write!(f, "BlockBody should not be none"),
            Error::NoneBlockHeader => write!(f, "BlockHeader should not be none"),
            Error::NoneChainStatus => write!(f, "Chain status should not be none"),
            Error::EarlyStatus => write!(f, "receive early status from same node"),
            Error::EncodeError(s) => write!(f, "Proto struct encode error: {}", s),
            Error::DecodeError(s) => write!(f, "Proto struct decode error: {}", s),
            Error::NoCandidate => write!(f, "No candidate block"),
            Error::ProposalTooHigh(proposal, current) => write!(
                f,
                "Proposal(h: {}) is higher than current(h: {})",
                proposal, current
            ),
            Error::ProposalTooLow(proposal, current) => write!(
                f,
                "Proposal(h: {}) is lower than current(h: {})",
                proposal, current
            ),
            Error::ProposalCheckError => write!(f, "Proposal check error"),
            Error::NoForkTree => write!(f, "Fork tree no block"),
            Error::DupTransaction(h) => {
                write!(f, "Found dup transaction 0x{}", hex::encode(h))
            }
            Error::BlockCheckError => write!(f, "block hash check error"),
            Error::CSISigCheckError => write!(f, "The sig of chain status init check error"),
            Error::VersionOrIdCheckError => write!(f, "Chain version or chain id check error"),
            Error::HashCheckError => write!(f, "Hash check error"),
            Error::HashLenError => write!(f, "Hash len is not correct"),
            Error::SigLenError => write!(f, "Signature is not correct"),
            Error::InternalError(e) => write!(f, "Internal Error: {}", e),
            Error::ExpectError(s) => write!(f, "Expect error: {}", s),
        }
    }
}
