pub mod external_api;
pub mod scheduler;
pub use external_api::*;
pub use scheduler::*;

pub static BEACON_CHAIN_API_URL: &str = "https://beaconcha.in/api/v1";
