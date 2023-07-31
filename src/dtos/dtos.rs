use serde::{Deserialize, Serialize};
use sqlx::FromRow;

use crate::models::{EpochData, SlotData};

#[derive(Debug, Serialize, Deserialize)]
pub struct Epoch {
    pub status: String,
    pub data: Vec<SlotDataDto>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EpochInfo {
    pub status: String,
    pub data: EpochDataDto,
}

#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct EpochDataDto {
    pub attestationscount: i64,
    pub attesterslashingscount: i64,
    pub averagevalidatorbalance: i64,
    pub blockscount: i64,
    pub depositscount: i64,
    pub eligibleether: i64,
    pub epoch: i64,
    pub finalized: bool,
    pub globalparticipationrate: f64,
    pub missedblocks: i64,
    pub orphanedblocks: i64,
    pub proposedblocks: i64,
    pub proposerslashingscount: i64,
    pub rewards_exported: bool,
    pub scheduledblocks: i64,
    pub totalvalidatorbalance: i64,
    pub ts: String,
    pub validatorscount: i64,
    pub voluntaryexitscount: i64,
    pub votedether: i64,
    pub withdrawalcount: i64,
}

impl From<EpochData> for EpochDataDto {
    fn from(value: EpochData) -> Self {
        EpochDataDto {
            attestationscount: value.attestationscount,
            attesterslashingscount: value.attesterslashingscount,
            averagevalidatorbalance: value.averagevalidatorbalance,
            blockscount: value.blockscount,
            depositscount: value.depositscount,
            eligibleether: value.eligibleether,
            epoch: value.epoch,
            finalized: if value.finalized == 1 { true } else { false },
            globalparticipationrate: value.globalparticipationrate,
            missedblocks: value.missedblocks,
            orphanedblocks: value.orphanedblocks,
            proposedblocks: value.proposedblocks,
            proposerslashingscount: value.proposerslashingscount,
            rewards_exported: if value.rewards_exported == 1 {
                true
            } else {
                false
            },
            scheduledblocks: value.scheduledblocks,
            totalvalidatorbalance: value.totalvalidatorbalance,
            ts: value.ts,
            validatorscount: value.validatorscount,
            voluntaryexitscount: value.voluntaryexitscount,
            votedether: value.votedether,
            withdrawalcount: value.withdrawalcount,
        }
    }
}

#[derive(Debug, Default, FromRow, Serialize, Deserialize, Clone)]
pub struct SlotDataDto {
    pub attestationscount: i64,
    pub attesterslashingscount: i64,
    pub blockroot: String,
    pub depositscount: i64,
    pub epoch: i64,
    pub eth1data_blockhash: Option<String>,
    pub eth1data_depositcount: i64,
    pub eth1data_depositroot: Option<String>,
    pub exec_base_fee_per_gas: Option<i64>,
    pub exec_block_hash: Option<String>,
    pub exec_block_number: Option<i64>,
    pub exec_extra_data: Option<String>,
    pub exec_fee_recipient: Option<String>,
    pub exec_gas_limit: Option<i64>,
    pub exec_gas_used: Option<i64>,
    pub exec_logs_bloom: Option<String>,
    pub exec_parent_hash: Option<String>,
    pub exec_random: Option<String>,
    pub exec_receipts_root: Option<String>,
    pub exec_state_root: Option<String>,
    pub exec_timestamp: Option<i64>,
    pub exec_transactions_count: i64,
    pub graffiti: Option<String>,
    pub graffiti_text: String,
    pub parentroot: Option<String>,
    pub proposer: i64,
    pub proposerslashingscount: i64,
    pub randaoreveal: Option<String>,
    pub signature: Option<String>,
    pub slot: i64,
    pub stateroot: Option<String>,
    pub status: String,
    pub syncaggregate_bits: Option<String>,
    pub syncaggregate_participation: f64,
    pub syncaggregate_signature: Option<String>,
    pub voluntaryexitscount: i64,
    pub withdrawalcount: i64,
}

impl From<SlotData> for SlotDataDto {
    fn from(value: SlotData) -> Self {
        SlotDataDto {
            attestationscount: value.attestationscount,
            attesterslashingscount: value.attesterslashingscount,
            blockroot: value.blockroot,
            depositscount: value.depositscount,
            epoch: value.epoch,
            eth1data_blockhash: Some(value.eth1data_blockhash),
            eth1data_depositcount: value.eth1data_depositcount,
            eth1data_depositroot: Some(value.eth1data_depositroot),
            exec_base_fee_per_gas: value.exec_base_fee_per_gas,
            exec_block_hash: value.exec_block_hash,
            exec_block_number: value.exec_block_number,
            exec_extra_data: value.exec_extra_data,
            exec_fee_recipient: value.exec_fee_recipient,
            exec_gas_limit: value.exec_gas_limit,
            exec_gas_used: value.exec_gas_used,
            exec_logs_bloom: value.exec_logs_bloom,
            exec_parent_hash: value.exec_parent_hash,
            exec_random: value.exec_random,
            exec_receipts_root: value.exec_receipts_root,
            exec_state_root: value.exec_state_root,
            exec_timestamp: value.exec_timestamp,
            exec_transactions_count: value.exec_transactions_count,
            graffiti: Some(value.graffiti),
            graffiti_text: value.graffiti_text,
            parentroot: Some(value.parentroot),
            proposer: value.proposer,
            proposerslashingscount: value.proposerslashingscount,
            randaoreveal: Some(value.randaoreveal),
            signature: Some(value.signature),
            slot: value.slot,
            stateroot: Some(value.stateroot),
            status: value.status,
            syncaggregate_bits: value.syncaggregate_bits,
            syncaggregate_participation: value.syncaggregate_participation,
            syncaggregate_signature: value.syncaggregate_signature,
            voluntaryexitscount: value.voluntaryexitscount,
            withdrawalcount: value.withdrawalcount,
        }
    }
}
