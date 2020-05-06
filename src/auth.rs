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

use crate::util::{verify_tx_hash, verify_tx_signature};
use cita_ng_proto::controller::raw_transaction::Tx::{NormalTx, UtxoTx};
use cita_ng_proto::controller::{raw_transaction::Tx, RawTransaction};
use prost::Message;

#[derive(Clone)]
pub struct Authentication {
    kms_port: String,
}

impl Authentication {
    pub fn new(kms_port: String) -> Self {
        Authentication { kms_port }
    }

    pub async fn check_raw_tx(&self, raw_tx: RawTransaction) -> Result<Vec<u8>, String> {
        if let Some(tx) = raw_tx.tx {
            match tx {
                NormalTx(normal_tx) => {
                    // todo check transaction
                    if normal_tx.witness.is_none() {
                        return Err("witness is none".to_owned());
                    }

                    let witness = normal_tx.witness.unwrap();
                    let signature = witness.signature;
                    let sender = witness.sender;

                    let mut tx_bytes: Vec<u8> = Vec::new();
                    if let Some(tx) = normal_tx.transaction {
                        let ret = tx.encode(&mut tx_bytes);
                        if ret.is_err() {
                            return Err("encode tx failed".to_owned());
                        }
                    } else {
                        return Err("tx is none".to_owned());
                    }

                    let tx_hash = normal_tx.transaction_hash;

                    if let Ok(is_ok) =
                        verify_tx_hash(self.kms_port.clone(), tx_hash.clone(), tx_bytes).await
                    {
                        if !is_ok {
                            return Err("Invalid tx_hash".to_owned());
                        }
                    }

                    if let Ok(address) =
                        verify_tx_signature(self.kms_port.clone(), tx_hash.clone(), signature).await
                    {
                        if address == sender {
                            Ok(tx_hash)
                        } else {
                            Err("Invalid sender".to_owned())
                        }
                    } else {
                        Err("kms recover signature failed".to_owned())
                    }
                }
                UtxoTx(utxo_tx) => {
                    // todo check transaction
                    let witnesses = utxo_tx.witnesses;

                    let mut tx_bytes: Vec<u8> = Vec::new();
                    if let Some(tx) = utxo_tx.transaction {
                        let ret = tx.encode(&mut tx_bytes);
                        if ret.is_err() {
                            return Err("encode utxo tx failed".to_owned());
                        }
                    } else {
                        return Err("utxo tx is none".to_owned());
                    }

                    let tx_hash = utxo_tx.transaction_hash;
                    if let Ok(is_ok) =
                        verify_tx_hash(self.kms_port.clone(), tx_hash.clone(), tx_bytes).await
                    {
                        if !is_ok {
                            return Err("Invalid utxo tx hash".to_owned());
                        }
                    }

                    for (i, w) in witnesses.into_iter().enumerate() {
                        let signature = w.signature;
                        let sender = w.sender;

                        if let Ok(address) =
                            verify_tx_signature(self.kms_port.clone(), tx_hash.clone(), signature)
                                .await
                        {
                            if address != sender {
                                let err_str = format!("Invalid sender index: {}", i);
                                return Err(err_str);
                            }
                        } else {
                            let err_str = format!("kms recover signature failed index: {}", i);
                            return Err(err_str);
                        }
                    }
                    Ok(tx_hash)
                }
            }
        } else {
            Err("Invalid raw tx".to_owned())
        }
    }
}
