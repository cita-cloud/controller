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

pub const GLOBAL: u32 = 0;
pub const TRANSACTIONS: u32 = 1;
#[allow(dead_code)]
pub const HEADERS: u32 = 2;
#[allow(dead_code)]
pub const BODIES: u32 = 3;
pub const BLOCK_HASH: u32 = 4;
pub const PROOF: u32 = 5;
pub const RESULT: u32 = 6;
pub const TRANSACTION_HASH2BLOCK_HEIGHT: u32 = 7;
pub const BLOCK_HASH2BLOCK_HEIGHT: u32 = 8; // In SQL db, reuse 4
pub const TRANSACTION_INDEX: u32 = 9;
pub const COMPAT_BLOCK: u32 = 10;
pub const FULL_BLOCK: u32 = 11;
#[allow(dead_code)]
pub const BUTTON: u32 = 12;
