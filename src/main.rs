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

#[arcon_decoder(,)]
#[derive(Arcon, Arrow, prost::Message, Clone)]
#[arcon(unsafe_ser_id = 12, reliable_ser_id = 13, version = 1, keys = "pu_location_id")]
pub struct RideData {
    #[prost(uint64)]
    pub pu_location_id: u64,
    #[prost(uint64)]
    pub fare_amount: u64,
    #[prost(float)]
    pub tip_amount: f32,
}

impl RideData {
    fn from(t: TaxiRideData) -> Self {
        Self {
            pu_location_id: t.pu_location_id,
            fare_amount: t.fare_amount,
            tip_amount: t.tip_amount,
        }
    }
}

#[derive(Arcon, Arrow, prost::Message, Clone)]
#[arcon(unsafe_ser_id = 12, reliable_ser_id = 13, version = 1)]
pub struct RideWindowedData {
    #[prost(uint64)]
    pub pu_location_id: u64,
    #[prost(uint64)]
    pub fare_amount: u64,
}

fn window_sum(buffer: &[RideData]) -> RideWindowedData {
    RideWindowedData{
        pu_location_id: buffer[0].pu_location_id,
        fare_amount: buffer.iter().map(|x| x.fare_amount).sum(),
    }
}

fn datetime_to_u64(datetime: &str) -> u64 {
    let s = NaiveDateTime::parse_from_str(datetime, "%Y-%m-%d %H:%M:%S").unwrap();
    s.timestamp() as u64
}

fn main() {
    let mut pipeline = Pipeline::default()
        .file("yellow_tripdata_2020-01.csv", |conf| {
            conf.set_arcon_time(ArconTime::Event);
            conf.set_timestamp_extractor(|x: &TaxiRideData| datetime_to_u64(&x.tpep_pickup_datetime));
        })
        .operator(OperatorBuilder {
            constructor: Arc::new(|_| Map::new(|x: TaxiRideData| RideData::from(x))),
            conf: Default::default(),
        })
        .operator(OperatorBuilder {
            constructor: Arc::new(|backend| {
                let function = AppenderWindow::new(backend.clone(), &window_sum);
                WindowAssigner::tumbling(function, backend, 24*60*60, 0, true)
            }),
            conf: OperatorConf {
                parallelism_strategy: ParallelismStrategy::Static(1),
                ..Default::default()
            }
        })
        .to_console()
        .build();
    pipeline.start();
    pipeline.await_termination();
}