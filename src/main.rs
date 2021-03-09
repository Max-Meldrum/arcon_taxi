use arcon::prelude::*;
use arcon::arcon_decoder;
use chrono::NaiveDateTime;

#[arcon_decoder(,)]
#[derive(Arcon, Arrow, prost::Message, Clone)]
#[arcon(unsafe_ser_id = 12, reliable_ser_id = 13, version = 1, keys = "vendor_id")]
pub struct TaxiRideData {
    #[prost(uint64)]
    pub vendor_id: u64,
    #[prost(string)]
    pub tpep_pickup_datetime: String,
    #[prost(string)]
    pub tpep_dropoff_datetime: String,
    #[prost(uint64)]
    pub passenger_count: u64,
    #[prost(float)]
    pub trip_distance: f32,
    #[prost(uint64)]
    pub rate_code_id: u64,
    #[prost(string)]
    pub store_and_fwd_flag: String,
    #[prost(uint64)]
    pub pu_location_id: u64,
    #[prost(uint64)]
    pub du_location_id: u64,
    #[prost(uint64)]
    pub payment_type: u64,
    #[prost(uint64)]
    pub fare_amount: u64,
    #[prost(uint64)]
    pub extra: u64,
    #[prost(float)]
    pub mta_tax: f32,
    #[prost(float)]
    pub tip_amount: f32,
    #[prost(uint64)]
    pub tolls_amount: u64,
    #[prost(float)]
    pub improvement_surcharge: f32,
    #[prost(float)]
    pub total_amount: f32,
    #[prost(float)]
    pub congestion_surcharge: f32,
}

fn datetime_to_u64(datetime: &str) -> u64 {
    let s = NaiveDateTime::parse_from_str(datetime, "%Y-%m-%d %H:%M:%S").unwrap();
    s.timestamp() as u64
}

fn main() {
    let mut pipeline = Pipeline::default()
        .file("test_data", |conf| {
            conf.set_arcon_time(ArconTime::Event);
            conf.set_timestamp_extractor(|x: &TaxiRideData| datetime_to_u64(&x.tpep_dropoff_datetime));
        })
        .operator(OperatorBuilder {
            constructor: Arc::new(|_| Filter::new(|x: &TaxiRideData| x.tip_amount < 10000.0)),
            conf: Default::default(),
        })
        .to_console()
        .build();

    pipeline.start();
    pipeline.await_termination();
}
