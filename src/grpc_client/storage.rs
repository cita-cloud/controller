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

use prost::Message;

use cita_cloud_proto::{
    blockchain::{raw_transaction::Tx::UtxoTx, Block, CompactBlock, RawTransaction},
    client::StorageClientTrait,
    common::{proposal_enum, BftProposal, Proof, ProposalEnum, StateRoot},
    controller::BlockNumber,
    status_code::StatusCodeEnum,
    storage::{ExtKey, Regions},
};
use cloud_util::storage::load_data;

use crate::util::u64_decode;

use super::storage_client;

pub async fn store_data(region: u32, key: Vec<u8>, value: Vec<u8>) -> StatusCodeEnum {
    cloud_util::storage::store_data(storage_client(), region, key, value).await
}

pub async fn assemble_proposal(mut block: Block, height: u64) -> Result<Vec<u8>, StatusCodeEnum> {
    block.proof.clear();
    block.state_root.clear();
    let pre_state_root = get_last_stateroot(height).await?;

    let proposal = ProposalEnum {
        proposal: Some(proposal_enum::Proposal::BftProposal(BftProposal {
            proposal: Some(block),
            pre_state_root,
            pre_proof: vec![],
        })),
    };

    let mut proposal_bytes = Vec::with_capacity(proposal.encoded_len());
    proposal.encode(&mut proposal_bytes).map_err(|_| {
        warn!("encode proposal({}) failed: buffer not sufficient", height);
        StatusCodeEnum::EncodeError
    })?;

    Ok(proposal_bytes)
}

pub async fn get_last_stateroot(h: u64) -> Result<Vec<u8>, StatusCodeEnum> {
    let pre_h = h - 1;
    let pre_height_bytes = pre_h.to_be_bytes().to_vec();

    let state_root = load_data(
        storage_client(),
        i32::from(Regions::Result) as u32,
        pre_height_bytes.clone(),
    )
    .await?;

    Ok(state_root)
}

pub async fn load_data_maybe_empty(region: u32, key: Vec<u8>) -> Result<Vec<u8>, StatusCodeEnum> {
    storage_client()
        .load(ExtKey { region, key })
        .await
        .map_or_else(
            |e| {
                warn!(
                    "load data maybe empty failed: {:?}. region: {}",
                    e.to_string(),
                    region
                );
                Err(StatusCodeEnum::StorageServerNotReady)
            },
            |value| match StatusCodeEnum::from(value.status.ok_or(StatusCodeEnum::NoneStatusCode)?)
            {
                StatusCodeEnum::Success => Ok(value.value),
                StatusCodeEnum::NotFound => Ok(vec![]),
                statue => Err(statue),
            },
        )
}

pub async fn get_full_block(height: u64) -> Result<Block, StatusCodeEnum> {
    let height_bytes = height.to_be_bytes().to_vec();

    let block_bytes = load_data(
        storage_client(),
        i32::from(Regions::FullBlock) as u32,
        height_bytes,
    )
    .await?;

    Block::decode(block_bytes.as_slice()).map_err(|_| {
        warn!("get full block failed: decode Block failed");
        StatusCodeEnum::DecodeError
    })
}

pub async fn db_get_tx(tx_hash: &[u8]) -> Result<RawTransaction, StatusCodeEnum> {
    let tx_hash_bytes = tx_hash.to_vec();

    let tx_bytes = load_data(
        storage_client(),
        i32::from(Regions::Transactions) as u32,
        tx_hash_bytes,
    )
    .await
    .map_err(|e| {
        warn!(
            "db get tx failed: {}. hash: 0x{}",
            e.to_string(),
            hex::encode(tx_hash),
        );
        StatusCodeEnum::NoTransaction
    })?;

    let raw_tx = RawTransaction::decode(tx_bytes.as_slice()).map_err(|_| {
        warn!(
            "db get tx failed: decode RawTransaction failed. hash: 0x{}",
            hex::encode(tx_hash)
        );
        StatusCodeEnum::DecodeError
    })?;

    Ok(raw_tx)
}
pub async fn load_tx_info(tx_hash: &[u8]) -> Result<(u64, u64), StatusCodeEnum> {
    let tx_hash_bytes = tx_hash.to_vec();

    let height_bytes = load_data(
        storage_client(),
        i32::from(Regions::TransactionHash2blockHeight) as u32,
        tx_hash_bytes.clone(),
    )
    .await
    .map_err(|e| {
        warn!(
            "load tx height failed: {}. hash: 0x{}",
            e.to_string(),
            hex::encode(tx_hash),
        );
        StatusCodeEnum::NoTxHeight
    })?;

    let tx_index_bytes = load_data(
        storage_client(),
        i32::from(Regions::TransactionIndex) as u32,
        tx_hash_bytes,
    )
    .await
    .map_err(|e| {
        warn!(
            "load tx index failed: {}. hash: 0x{}",
            e.to_string(),
            hex::encode(tx_hash),
        );
        StatusCodeEnum::NoTxIndex
    })?;

    let block_height = u64_decode(height_bytes);
    let tx_index = u64_decode(tx_index_bytes);

    Ok((block_height, tx_index))
}

pub async fn get_height_by_block_hash(hash: Vec<u8>) -> Result<BlockNumber, StatusCodeEnum> {
    let block_number = load_data(
        storage_client(),
        i32::from(Regions::BlockHash2blockHeight) as u32,
        hash.clone(),
    )
    .await
    .map_err(|e| {
        warn!(
            "get height by block hash failed: {}. hash: 0x{}",
            e.to_string(),
            hex::encode(hash),
        );
        StatusCodeEnum::NoBlockHeight
    })
    .map(u64_decode)?;
    Ok(BlockNumber { block_number })
}

pub async fn get_compact_block(height: u64) -> Result<CompactBlock, StatusCodeEnum> {
    let height_bytes = height.to_be_bytes().to_vec();

    let compact_block_bytes = load_data(
        storage_client(),
        i32::from(Regions::CompactBlock) as u32,
        height_bytes.clone(),
    )
    .await
    .map_err(|e| {
        warn!("get compact block({}) failed: {}", height, e.to_string());
        StatusCodeEnum::NoBlock
    })?;

    let compact_block = CompactBlock::decode(compact_block_bytes.as_slice()).map_err(|_| {
        warn!(
            "get compact block({}) failed: decode CompactBlock failed",
            height
        );
        StatusCodeEnum::DecodeError
    })?;

    Ok(compact_block)
}

pub async fn get_proof(height: u64) -> Result<Proof, StatusCodeEnum> {
    let height_bytes = height.to_be_bytes().to_vec();

    let proof = load_data(
        storage_client(),
        i32::from(Regions::Proof) as u32,
        height_bytes,
    )
    .await
    .map_err(|e| {
        warn!("get proof({}) failed: {}", height, e.to_string());
        StatusCodeEnum::NoProof
    })?;

    Ok(Proof { proof })
}

pub async fn get_state_root(height: u64) -> Result<StateRoot, StatusCodeEnum> {
    let height_bytes = height.to_be_bytes().to_vec();

    let state_root = load_data(
        storage_client(),
        i32::from(Regions::Result) as u32,
        height_bytes,
    )
    .await
    .map_err(|e| {
        warn!("get state_root({}) failed: {}", height, e.to_string());
        StatusCodeEnum::NoStateRoot
    })?;

    Ok(StateRoot { state_root })
}

pub async fn get_hash_in_range(mut hash: Vec<u8>, height: u64) -> Result<Vec<u8>, StatusCodeEnum> {
    let height_bytes = load_data(
        storage_client(),
        i32::from(Regions::TransactionHash2blockHeight) as u32,
        hash.clone(),
    )
    .await
    .unwrap();
    let mut tx_height = u64_decode(height_bytes);
    while tx_height >= height {
        hash = match load_data(
            storage_client(),
            i32::from(Regions::Transactions) as u32,
            hash.clone(),
        )
        .await
        {
            Ok(raw_tx_bytes) => {
                if let UtxoTx(tx) = RawTransaction::decode(raw_tx_bytes.as_slice())
                    .unwrap()
                    .tx
                    .unwrap()
                {
                    tx.transaction.unwrap().pre_tx_hash
                } else {
                    warn!(
                        "load utxo_tx failed: not utxo_tx. hash: 0x{}",
                        hex::encode(&hash)
                    );
                    return Err(StatusCodeEnum::NoneUtxo);
                }
            }
            Err(status) => {
                warn!(
                    "load utxo_tx failed: {}. hash: 0x{}",
                    status,
                    hex::encode(&hash)
                );
                return Err(StatusCodeEnum::NoTransaction);
            }
        };
        if hash == vec![0u8; 33] {
            tx_height = 0;
        } else {
            let height_bytes = load_data(
                storage_client(),
                i32::from(Regions::TransactionHash2blockHeight) as u32,
                hash.clone(),
            )
            .await?;
            tx_height = u64_decode(height_bytes);
        };
    }
    Ok(hash)
}
