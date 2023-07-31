use super::BEACON_CHAIN_API_URL;
use reqwest::get;
use sqlx::SqlitePool;

use crate::{
    db_ops,
    dtos::{Epoch, EpochDataDto, EpochInfo, SlotDataDto},
    models::SlotData,
    AppResult,
};

pub async fn get_specific_epoch_data(epoch_number: &str) -> AppResult<EpochDataDto> {
    println!("Fetching {epoch_number} epoch from chain");
    let api_key = env!("API_KEY");
    let url = format!("{BEACON_CHAIN_API_URL}/epoch/{epoch_number}?apikey={api_key}");
    let response = get(url).await?.text().await?;
    println!("{epoch_number} epoch fetched successfully");
    println!("Starting to parse fetched data into structure");
    let latest_epoch_info = serde_json::from_str::<EpochInfo>(&response)?;
    let latest_epoch_data = latest_epoch_info.data;
    println!("{epoch_number} epoch data is being forwarded");
    Ok(latest_epoch_data)
}

pub async fn get_specific_epoch_slots(epoch_number: i64) -> AppResult<Vec<SlotDataDto>> {
    println!("Fetching {epoch_number} epoch slots from chain now");
    let api_key = env!("API_KEY");
    let url = format!("{BEACON_CHAIN_API_URL}/epoch/{epoch_number}/slots?apikey={api_key}");
    let response = get(url).await?.text().await?;
    println!("Starting to parse fetched data into structure");
    let epoch_data = serde_json::from_str::<Epoch>(&response)?;
    let slots = epoch_data.data;
    println!("Slots for {epoch_number} epoch fetched successfully!");
    // println!("Epoch Slots are : {:#?}", slots);
    Ok(slots)
}

pub async fn fetch_recent_epoch_slots(db_conn: SqlitePool, how_many: i64) -> AppResult<()> {
    println!("Fetching latest epoch number on chain");
    let current_epoch_number = get_specific_epoch_data("latest").await?.epoch;
    let mut epochs = Vec::with_capacity(how_many as usize);

    println!("Starting loop for fetching {how_many} epoch slots from chain");
    for epoch_number in current_epoch_number.saturating_sub(how_many - 1)..=current_epoch_number {
        println!("Fetching epoch {epoch_number} from chain");
        let epoch_slots = get_specific_epoch_slots(epoch_number).await?;
        // println!("Epoch Slots are : {:#?}", epoch_slots);
        epochs.push(epoch_slots);
        println!("{epoch_number} Epoch data pushed successfully into the vector");
    }

    // println!("Epochs : {:#?}", epochs);

    for epoch in epochs {
        // match epoch {
        //     Ok(slots) => {
        for slot in epoch {
            let db_slot: SlotData = slot.into();
            // println!("Slot is : {:?}", db_slot);
            match db_ops::insert_slot(&db_conn, db_slot.slot, &db_slot).await {
                Ok(_) => println!("Slot {} inserted into database!", db_slot.slot),
                Err(_) => continue,
            }
        }
        // }
        // Err(e) => eprintln!("Fetch Recent Epoch Task Panicked: {:?}", e),
        // }
    }

    Ok(())
}

pub async fn get_specific_slot(slot_number: i64) -> AppResult<SlotDataDto> {
    let api_key = env!("API_KEY");
    let url = format!("{BEACON_CHAIN_API_URL}/slot/{slot_number}?apikey={api_key}");
    let response = get(url).await?.text().await?;
    println!("Slot {slot_number} fetched successfully from chain");
    let slot: SlotDataDto = serde_json::from_str(&response)?;
    println!("Slot deserialized successfully");
    println!("Slot {slot_number} inserted into db successfully");
    Ok(slot)
}
