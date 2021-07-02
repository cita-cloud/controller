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

use cita_cloud_proto::blockchain::{
    Block, BlockHeader, CompactBlock, CompactBlockBody, RawTransactions,
};
use cita_cloud_proto::consensus::consensus_service_client::ConsensusServiceClient;
use cita_cloud_proto::executor::executor_service_client::ExecutorServiceClient;
// use cita_cloud_proto::kms::{
//     kms_service_client::KmsServiceClient, HashDataRequest, RecoverSignatureRequest,
//     VerifyDataHashRequest,
// };
use cita_cloud_proto::network::{
    network_service_client::NetworkServiceClient, NetworkStatusResponse,
};
use cita_cloud_proto::storage::{storage_service_client::StorageServiceClient, Content, ExtKey};
use log::{info, warn};
use tonic::Request;

use crate::error::Error;
use cita_cloud_proto::blockchain::raw_transaction::Tx;
use cita_cloud_proto::blockchain::RawTransaction;
use cita_cloud_proto::common::{
    Address, ConsensusConfiguration, Empty, Proposal, ProposalWithProof,
};
use prost::Message;
use std::path::Path;
use tokio::fs;
use tokio::sync::OnceCell;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tonic::Code;

pub const ADDR_BYTES_LEN: usize = 20;
pub const HASH_BYTES_LEN: usize = 32;
pub const SM2_SIGNATURE_BYTES_LEN: usize = 128;

pub static CONSENSUS_CLIENT: OnceCell<ConsensusServiceClient<Channel>> = OnceCell::const_new();
pub static STORAGE_CLIENT: OnceCell<StorageServiceClient<Channel>> = OnceCell::const_new();
pub static EXECUTOR_CLIENT: OnceCell<ExecutorServiceClient<Channel>> = OnceCell::const_new();
pub static NETWORK_CLIENT: OnceCell<NetworkServiceClient<Channel>> = OnceCell::const_new();

// This must be called before access to clients.
pub fn init_grpc_client(
    consensus_port: u16,
    storage_port: u16,
    executor_port: u16,
    network_port: u16,
) {
    CONSENSUS_CLIENT
        .set({
            let addr = format!("http://127.0.0.1:{}", consensus_port);
            let channel = Endpoint::from_shared(addr).unwrap().connect_lazy().unwrap();
            ConsensusServiceClient::new(channel)
        })
        .unwrap();
    STORAGE_CLIENT
        .set({
            let addr = format!("http://127.0.0.1:{}", storage_port);
            let channel = Endpoint::from_shared(addr).unwrap().connect_lazy().unwrap();
            StorageServiceClient::new(channel)
        })
        .unwrap();
    EXECUTOR_CLIENT
        .set({
            let addr = format!("http://127.0.0.1:{}", executor_port);
            let channel = Endpoint::from_shared(addr).unwrap().connect_lazy().unwrap();
            ExecutorServiceClient::new(channel)
        })
        .unwrap();
    NETWORK_CLIENT
        .set({
            let addr = format!("http://127.0.0.1:{}", network_port);
            let channel = Endpoint::from_shared(addr).unwrap().connect_lazy().unwrap();
            NetworkServiceClient::new(channel)
        })
        .unwrap();
}

pub fn consensus_client() -> ConsensusServiceClient<Channel> {
    CONSENSUS_CLIENT.get().cloned().unwrap()
}

pub fn storage_client() -> StorageServiceClient<Channel> {
    STORAGE_CLIENT.get().cloned().unwrap()
}

pub fn executor_client() -> ExecutorServiceClient<Channel> {
    EXECUTOR_CLIENT.get().cloned().unwrap()
}

pub fn network_client() -> NetworkServiceClient<Channel> {
    NETWORK_CLIENT.get().cloned().unwrap()
}

pub fn unix_now() -> u64 {
    let d = ::std::time::UNIX_EPOCH.elapsed().unwrap();
    d.as_secs() * 1_000 + u64::from(d.subsec_millis())
}

pub async fn reconfigure(
    consensus_config: ConsensusConfiguration,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let mut client = consensus_client();

    let request = Request::new(consensus_config);

    let response = client.reconfigure(request).await?;
    Ok(response.into_inner().is_success)
}

pub async fn check_block(
    height: u64,
    data: Vec<u8>,
    proof: Vec<u8>,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let mut client = consensus_client();

    let proposal = Some(Proposal { height, data });
    let request = Request::new(ProposalWithProof { proposal, proof });

    let response = client.check_block(request).await?;
    Ok(response.into_inner().is_success)
}

pub fn verify_tx_signature(
    // kms_port: u16,
    tx_hash: &[u8],
    signature: &[u8],
) -> Result<Vec<u8>, Error> {
    // let kms_addr = format!("http://127.0.0.1:{}", kms_port);
    // let mut client = KmsServiceClient::connect(kms_addr).await?;
    //
    // let request = Request::new(RecoverSignatureRequest {
    //     msg: tx_hash,
    //     signature,
    // });
    //
    // match client.recover_signature(request).await {
    //     Ok(response) => Ok(response.into_inner().address),
    //     Err(e) => {
    //         if e.code() == Code::InvalidArgument {
    //             Ok(vec![])
    //         } else {
    //             Err(Box::new(e))
    //         }
    //     }
    // }

    if signature.len() != SM2_SIGNATURE_BYTES_LEN {
        warn!(
            "signature len is not correct, item len: {}, correct len: {}",
            signature.len(),
            SM2_SIGNATURE_BYTES_LEN
        );
        Err(Error::SigLenError)
    } else {
        Ok(pk2address(&sm2_recover(signature, tx_hash)?))
    }
}

fn sm2_recover(signature: &[u8], message: &[u8]) -> Result<Vec<u8>, Error> {
    let r = &signature[0..32];
    let s = &signature[32..64];
    let pk = &signature[64..];

    let signature =
        efficient_sm2::Signature::new(r, s).map_err(|e| Error::ExpectError(e.to_string()))?;
    let public_key = efficient_sm2::PublicKey::new(&pk[..32], &pk[32..]);

    signature.verify(&public_key, message).map_or_else(
        |e| Err(Error::ExpectError(e.to_string())),
        |_| Ok(pk.to_vec()),
    )
}

pub fn pk2address(pk: &[u8]) -> Vec<u8> {
    hash_data(pk)[HASH_BYTES_LEN - ADDR_BYTES_LEN..].to_vec()
}

pub fn verify_tx_hash(
    // kms_port: u16,
    tx_hash: &[u8],
    tx_bytes: &[u8],
) -> Result<(), Error> {
    // let kms_addr = format!("http://127.0.0.1:{}", kms_port);
    // let mut client = KmsServiceClient::connect(kms_addr).await?;
    //
    // let request = Request::new(VerifyDataHashRequest {
    //     data: tx_bytes,
    //     hash: tx_hash,
    // });
    //
    // let response = client.verify_data_hash(request).await?;
    // Ok(response.into_inner().is_success)

    if tx_hash.len() != HASH_BYTES_LEN {
        warn!(
            "tx_hash len is not correct, item len: {}, correct len: {}",
            tx_hash.len(),
            HASH_BYTES_LEN
        );
        Err(Error::HashLenError)
    } else {
        let computed_hash = hash_data(tx_bytes);
        if tx_hash != computed_hash {
            warn!(
                "tx_hash is not consistent, item hash: {}, computed hash: {}",
                hex::encode(tx_hash),
                hex::encode(&hash_data(tx_bytes))
            );
            Err(Error::HashCheckError)
        } else {
            Ok(())
        }
    }
}

// todo wrap in library
pub fn hash_data(
    // kms_port: u16,
    data: &[u8],
) -> Vec<u8> {
    // let kms_addr = format!("http://127.0.0.1:{}", kms_port);
    // let mut client = KmsServiceClient::connect(kms_addr).await?;
    //
    // let request = Request::new(HashDataRequest { data });
    //
    // let response = client.hash_data(request).await?;
    // Ok(response.into_inner().hash)

    libsm::sm3::hash::Sm3Hash::new(data).get_hash().to_vec()
}

pub async fn store_data(
    region: u32,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let mut client = storage_client();
    let request = Request::new(Content { region, key, value });
    let response = client.store(request).await?;
    Ok(response.into_inner().is_success)
}

pub async fn load_data(
    region: u32,
    key: Vec<u8>,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let mut client = storage_client();
    let request = Request::new(ExtKey { region, key });
    let response = client.load(request).await?;
    Ok(response.into_inner().value)
}

pub async fn load_data_maybe_empty(
    region: u32,
    key: Vec<u8>,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let mut client = storage_client();

    let request = Request::new(ExtKey { region, key });

    match client.load(request).await {
        Ok(response) => Ok(response.into_inner().value),
        Err(e) => {
            if e.code() == Code::NotFound {
                Ok(vec![])
            } else {
                Err(Box::new(e))
            }
        }
    }
}

pub async fn get_full_block(
    compact_block: CompactBlock,
    proof: Vec<u8>,
) -> Result<Block, Box<dyn std::error::Error + Send + Sync>> {
    let mut body = Vec::new();
    if let Some(compact_body) = compact_block.body {
        for hash in compact_body.tx_hashes {
            if let Some(tx) = db_get_tx(&hash).await {
                body.push(tx);
            } else {
                panic!("can't get tx: {}", hex::encode(hash));
            }
        }
    }

    Ok(Block {
        version: compact_block.version,
        header: compact_block.header,
        body: Some(RawTransactions { body }),
        proof,
    })
}

pub fn full_to_compact(block: Block) -> CompactBlock {
    let mut compact_body = CompactBlockBody { tx_hashes: vec![] };

    if let Some(body) = block.body {
        for raw_tx in body.body {
            match raw_tx.tx {
                Some(Tx::NormalTx(normal_tx)) => {
                    compact_body.tx_hashes.push(normal_tx.transaction_hash)
                }
                Some(Tx::UtxoTx(utxo_tx)) => compact_body.tx_hashes.push(utxo_tx.transaction_hash),
                None => {}
            }
        }
    }

    CompactBlock {
        version: block.version,
        header: block.header,
        body: Some(compact_body),
    }
}

pub async fn exec_block(block: Block) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let mut client = executor_client();
    let request = Request::new(block);
    let response = client.exec(request).await?;
    Ok(response.into_inner().hash)
}

pub async fn get_network_status(
) -> Result<NetworkStatusResponse, Box<dyn std::error::Error + Send + Sync>> {
    let mut client = network_client();
    let request = Request::new(Empty {});
    let response = client.get_network_status(request).await?;
    Ok(response.into_inner())
}

pub fn print_main_chain(chain: &[Vec<u8>], block_number: u64) {
    for (i, hash) in chain.iter().enumerate() {
        info!(
            "height: {} hash 0x{}",
            i as u64 + block_number + 1,
            hex::encode(&hash)
        );
    }
}

pub async fn write_tx(tx_hash: &[u8], data: &[u8]) {
    let filename = format!("{}", hex::encode(tx_hash));

    let root_path = Path::new(".");
    let file_path = root_path.join("txs").join(filename);
    let _ = fs::write(file_path, data).await;
}

#[allow(dead_code)]
pub fn check_tx_exists(tx_hash: &[u8]) -> bool {
    let filename = hex::encode(tx_hash);
    let new_filename = format!("new_{}", filename);

    let root_path = Path::new(".");
    let file_path = root_path.join("txs").join(filename);
    let new_file_path = root_path.join("txs").join(new_filename);

    file_path.exists() || new_file_path.exists()
}

pub async fn db_get_tx(tx_hash: &[u8]) -> Option<RawTransaction> {
    let filename = hex::encode(tx_hash);
    let root_path = Path::new(".");
    let tx_path = root_path.join("txs").join(filename);

    let ret = fs::read(tx_path).await;
    if ret.is_err() {
        warn!("read tx file failed: {:?}", ret);
        return None;
    }
    let content = ret.unwrap();
    let ret = RawTransaction::decode(content.as_slice());
    if ret.is_err() {
        warn!("decode tx file failed: {:?}", ret);
        return None;
    }
    Some(ret.unwrap())
}

pub fn get_tx_hash(raw_tx: &RawTransaction) -> Result<Vec<u8>, Error> {
    match raw_tx.tx {
        Some(Tx::NormalTx(ref normal_tx)) => Ok(normal_tx.transaction_hash.clone()),

        Some(Tx::UtxoTx(ref utxo_tx)) => Ok(utxo_tx.transaction_hash.clone()),

        None => return Err(Error::NoneBlockBody),
    }
}

#[allow(dead_code)]
pub fn get_tx_hash_list(raw_txs: &RawTransactions) -> Result<Vec<Vec<u8>>, Error> {
    let mut hashes = Vec::new();
    for raw_tx in &raw_txs.body {
        hashes.push(get_tx_hash(raw_tx)?)
    }
    Ok(hashes)
}

pub fn get_block_hash(header: Option<&BlockHeader>) -> Result<Vec<u8>, Error> {
    match header {
        Some(header) => {
            let mut block_header_bytes = Vec::new();
            header
                .encode(&mut block_header_bytes)
                .expect("encode block header failed");
            let block_hash = hash_data(&block_header_bytes);
            Ok(block_hash)
        }

        None => return Err(Error::NoneBlockHeader),
    }
}

#[allow(dead_code)]
pub async fn remove_tx(filename: &str) {
    let root_path = Path::new(".");
    let tx_path = root_path.join("txs").join(filename);
    let _ = fs::remove_file(tx_path).await;
}

pub async fn store_tx_info(tx_hash: &[u8], block_height: u64, tx_index: usize) {
    let filename = hex::encode(tx_hash);
    let root_path = Path::new(".");
    let tx_info_path = root_path.join("tx_infos").join(filename);

    let mut data = block_height.to_be_bytes().to_vec();
    data.extend_from_slice(&tx_index.to_be_bytes().to_vec());

    let _ = fs::write(tx_info_path, data).await;
}

pub async fn load_tx_info(tx_hash: &[u8]) -> Option<(u64, u64)> {
    let filename = hex::encode(tx_hash);
    let root_path = Path::new(".");
    let tx_info_path = root_path.join("tx_infos").join(filename);

    let ret = fs::read(tx_info_path).await;
    if ret.is_err() {
        warn!("read tx info file failed: {:?}", ret);
        return None;
    }
    let data = ret.unwrap();

    if data.len() != 16 {
        warn!("tx info data invalid");
        return None;
    }

    let mut buf: [u8; 8] = [0; 8];

    buf[..8].clone_from_slice(&data[..8]);
    let block_height = u64::from_be_bytes(buf);

    buf[..8].clone_from_slice(&data[8..]);
    let tx_index = u64::from_be_bytes(buf);

    Some((block_height, tx_index))
}

pub async fn write_block(
    height: u64,
    block_header_bytes: &[u8],
    block_body_bytes: &[u8],
    proof_bytes: &[u8],
) {
    let filename = format!("{}", height);
    let root_path = Path::new(".");
    let block_path = root_path.join("blocks").join(filename);

    let header_len = block_header_bytes.len();
    let body_len = block_body_bytes.len();
    let proof_len = proof_bytes.len();

    let mut bytes = header_len.to_be_bytes().to_vec();
    bytes.extend_from_slice(&body_len.to_be_bytes().to_vec());
    bytes.extend_from_slice(&proof_len.to_be_bytes().to_vec());
    bytes.extend_from_slice(&block_header_bytes);
    bytes.extend_from_slice(&block_body_bytes);
    bytes.extend_from_slice(&proof_bytes);

    let _ = fs::write(block_path, bytes).await;
}

pub fn check_block_exists(height: u64) -> bool {
    let filename = format!("{}", height);
    let root_path = Path::new(".");
    let file_path = root_path.join("blocks").join(filename);

    file_path.exists()
}

pub async fn get_compact_block(height: u64) -> Option<(CompactBlock, Vec<u8>)> {
    let filename = format!("{}", height);
    let root_path = Path::new(".");
    let block_path = root_path.join("blocks").join(filename);

    let ret = fs::read(block_path).await;
    if ret.is_err() {
        return None;
    }
    let bytes = ret.unwrap();

    let bytes_len = bytes.len();
    if bytes_len <= 24 {
        warn!("get_compact_block {} bad bytes length", height);
        return None;
    }

    let mut len_bytes: [u8; 8] = [0; 8];
    // parse header_len
    len_bytes[..8].clone_from_slice(&bytes[..8]);
    let header_len = usize::from_be_bytes(len_bytes);
    // parse body_len
    len_bytes[..8].clone_from_slice(&bytes[8..16]);
    let body_len = usize::from_be_bytes(len_bytes);
    // parse proof_len
    len_bytes[..8].clone_from_slice(&bytes[16..24]);
    let proof_len = usize::from_be_bytes(len_bytes);

    if bytes_len != 24 + header_len + body_len + proof_len {
        warn!("get_compact_block {} bad bytes total length", height);
        return None;
    }

    let mut start: usize = 24;
    let header_slice = &bytes[start..start + header_len];
    start += header_len;
    let body_slice = &bytes[start..start + body_len];
    start += body_len;
    let proof_slice = &bytes[start..start + proof_len];

    let ret = BlockHeader::decode(header_slice);
    if ret.is_err() {
        warn!("get_compact_block {} decode BlockHeader failed", height);
        return None;
    }
    let block_header = ret.unwrap();

    let ret = CompactBlockBody::decode(body_slice);
    if ret.is_err() {
        warn!(
            "get_compact_block {} decode CompactBlockBody failed",
            height
        );
        return None;
    }
    let block_body = ret.unwrap();

    let block = CompactBlock {
        version: 0,
        header: Some(block_header),
        body: Some(block_body),
    };

    Some((block, proof_slice.to_vec()))
}

#[macro_export]
macro_rules! impl_multicast {
    ($func_name:ident, $type:ident, $name:expr) => {
        pub async fn $func_name(&self, item: $type) -> Vec<tokio::task::JoinHandle<()>> {
            let nodes = self.node_manager.grab_node().await;

            let mut buf = Vec::new();

            item.encode(&mut buf)
                .expect(&($name.to_string() + " encode failed"));

            let mut handle_vec = Vec::new();

            for node in nodes {
                log::debug!(
                    "multicast {} len: {} to 0x{}",
                    $name,
                    buf.len(),
                    hex::encode(&node.address)
                );

                let mut client = crate::util::network_client();

                let origin = self.node_manager.get_origin(&node).await.expect(
                    format!("not get address: 0x{} origin", hex::encode(&node.address)).as_str(),
                );

                let msg = cita_cloud_proto::network::NetworkMsg {
                    module: "controller".to_string(),
                    r#type: $name.into(),
                    origin,
                    msg: buf.clone(),
                };

                let request = tonic::Request::new(msg);

                let handle = tokio::spawn(async move {
                    match client.send_msg(request).await {
                        Ok(_) => {
                            log::debug!("multicast {} ok", $name)
                        }
                        Err(status) => {
                            log::warn!(
                                "multicast {} to 0x{} failed: {:?}",
                                $name,
                                hex::encode(&node.address),
                                status
                            )
                        }
                    }
                });

                handle_vec.push(handle);
            }
            handle_vec
        }
    };
}

// todo change return to handle panic & unwrap
#[macro_export]
macro_rules! impl_unicast {
    ($func_name:ident, $type:ident, $name:expr) => {
        pub async fn $func_name(&self, origin: u64, item: $type) -> tokio::task::JoinHandle<()> {
            let mut client = crate::util::network_client();

            let mut buf = Vec::new();

            item.encode(&mut buf)
                .expect(&($name.to_string() + " encode failed"));

            log::debug!("unicast {} len: {} to origin[{}]", $name, buf.len(), origin);

            let msg = cita_cloud_proto::network::NetworkMsg {
                module: "controller".to_string(),
                r#type: $name.into(),
                origin,
                msg: buf,
            };

            let request = tonic::Request::new(msg);

            tokio::spawn(async move {
                match client.send_msg(request).await {
                    Ok(_) => {}
                    Err(status) => {
                        log::warn!(
                            "unicast {} to origin[{}] failed: {:?}",
                            $name,
                            origin,
                            status
                        )
                    }
                }
            })
        }
    };
}

#[macro_export]
macro_rules! impl_broadcast {
    ($func_name:ident, $type:ident, $name:expr) => {
        pub async fn $func_name(&self, item: $type) -> tokio::task::JoinHandle<()> {
            let mut client = crate::util::network_client();

            let mut buf = Vec::new();

            item.encode(&mut buf)
                .expect(&($name.to_string() + " encode failed"));

            log::debug!("broadcast {} buf len: {}", $name, buf.clone().len());

            let msg = cita_cloud_proto::network::NetworkMsg {
                module: "controller".to_string(),
                r#type: $name.into(),
                origin: 0,
                msg: buf,
            };

            let request = tonic::Request::new(msg);

            tokio::spawn(async move {
                match client.broadcast(request).await {
                    Ok(_) => {}
                    Err(status) => {
                        log::warn!("broadcast {} failed: {:?}", $name, status)
                    }
                }
            })
        }
    };
}

pub fn clean_0x(s: &str) -> &str {
    if s.starts_with("0x") {
        &s[2..]
    } else {
        s
    }
}

pub fn h160_address_check(address: Option<&Address>) -> Result<(), Error> {
    match address {
        Some(addr) => {
            if addr.address.len() == 20 {
                Ok(())
            } else {
                Err(Error::ProvideAddressError)
            }
        }
        None => Err(Error::NoProvideAddress),
    }
}

pub fn check_sig(_sig: &[u8], _pubk: &[u8]) -> Result<(), Error> {
    return Ok(());
}
