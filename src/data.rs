use arcon::arcon_decoder;
use arcon::prelude::*;
use chrono::NaiveDateTime;

#[derive(Arcon, Arrow, Clone, prost::Message)]
#[arcon(unsafe_ser_id = 12, reliable_ser_id = 13, version = 1)]
pub struct Unit {}

#[arcon_decoder(,)]
#[macros::proto]
#[derive(Arcon, Arrow, Clone)]
#[arcon(
    unsafe_ser_id = 12,
    reliable_ser_id = 13,
    version = 1,
    keys = "vendor_id"
)]
pub struct TaxiRideData {
    /// A code indicating the TPEP provider that provided the record.
    /// 1 = Creative Mobile Technologies, LLC; 2 = VeriFone Inc,
    pub vendor_id: u64,
    /// The date and time when the meter was engaged.
    pub tpep_pickup_datetime: String,
    /// The date and time when the meter was disengaged.
    pub tpep_dropoff_datetime: String,
    /// The number of passengers in the vehicle. This is a driver-entered value.
    pub passenger_count: u64,
    /// The elapsed trip distance in miles reported by the taximeter.
    pub trip_distance: f32,
    /// The final rate code in effect at the end of the trip.
    ///
    /// * 1 = Standard rate
    /// * 2 = JFK
    /// * 3 = Newark
    /// * 4 = Nassau or Westchester
    /// * 5 = Negotiated fare
    /// * 6 = Group ride
    pub rate_code_id: u64,
    /// This flag indicates whether the trip record was held in vehicle
    /// memory before sending to the vendor, aka “store and forward,”
    /// because the vehicle did not have a connection to the server.
    ///
    /// * Y = store and forward trip
    /// * N = not a store and forward trip
    pub store_and_fwd_flag: String,
    /// TLC Taxi Zone in which the taximeter was engaged
    pub pu_location_id: u64,
    /// TLC Taxi Zone in which the taximeter was disengaged
    pub du_location_id: u64,
    /// A numeric code signifying how the passenger paid for the trip.
    ///
    /// * 1 = Credit card
    /// * 2 = Cash
    /// * 3 = No charge
    /// * 4 = Dispute
    /// * 5 = Unknown
    /// * 6 = Voided trip
    pub payment_type: u64,
    /// The time-and-distance fare calculated by the meter.
    pub fare_amount: u64,
    /// Miscellaneous extras and surcharges. Currently, this only includes
    /// the $0.50 and $1 rush hour and overnight charges
    pub extra: u64,
    /// $0.50 MTA tax that is automatically triggered based on the metered
    /// rate in use.
    pub mta_tax: f32,
    /// Tip amount – This field is automatically populated for credit card
    /// tips. Cash tips are not included.
    pub tip_amount: f32,
    /// Total amount of all tolls paid in trip.
    pub tolls_amount: u64,
    /// $0.30 improvement surcharge assessed trips at the flag drop. The
    /// improvement surcharge began being levied in 2015
    pub improvement_surcharge: f32,
    /// The total amount charged to passengers. Does not include cash tips.
    pub total_amount: f32,
    /// This field is not documented.
    pub congestion_surcharge: f32,
}
/// A cleaned up version of TaxiRideData.
#[arcon_decoder(,)]
#[macros::proto]
#[derive(Arcon, Arrow, Clone)]
#[arcon(
    unsafe_ser_id = 12,
    reliable_ser_id = 13,
    version = 1,
    keys = "pu_location_id"
)]
pub struct RideData {
    /// Keys
    pub pu_location_id: u64,
    pub pu_time: u64,
    pub do_time: u64,
    /// Values
    pub fare_amount: u64,
    pub tip_amount: f32,
    pub trip_distance: f32,
    pub passenger_count: u64,
}

impl From<TaxiRideData> for RideData {
    fn from(t: TaxiRideData) -> Self {
        Self {
            pu_location_id: t.pu_location_id,
            pu_time: datetime_to_u64(&t.tpep_pickup_datetime),
            do_time: datetime_to_u64(&t.tpep_dropoff_datetime),
            fare_amount: t.fare_amount,
            tip_amount: t.tip_amount,
            trip_distance: t.trip_distance,
            passenger_count: t.passenger_count,
        }
    }
}

/// Aggregate of TaxiRideData, produced by window_sum
#[macros::proto]
#[derive(Arcon, Arrow, Clone, Copy)]
#[arcon(
    unsafe_ser_id = 12,
    reliable_ser_id = 13,
    version = 1,
    keys = "pu_location_id, pu_time"
)]
pub struct RideWindowedData {
    /// Keys
    pub pu_location_id: u64,
    pub pu_time: u64,
    /// Aggregates
    pub count: u64,

    pub sum_fare_amount: u64,
    pub max_fare_amount: u64,
    pub avg_fare_amount: u64,
    pub min_fare_amount: u64,

    pub sum_trip_distance: f32,
    pub avg_trip_distance: f32,

    pub sum_passenger_count: u64,
    pub max_passenger_count: u64,
    pub avg_passenger_count: u64,
    pub min_passenger_count: u64,

    pub sum_duration: u64,
    pub max_duration: u64,
    pub avg_duration: u64,
    pub min_duration: u64,
}

pub fn datetime_to_u64(datetime: &str) -> u64 {
    let s = NaiveDateTime::parse_from_str(datetime, "%Y-%m-%d %H:%M:%S").unwrap();
    s.timestamp() as u64
}

#[derive(ArconState)]
pub struct RideState<B: Backend> {
    #[table = "rides"]
    rides: EagerValue<RideWindowedData, B>,
}

impl<B: Backend> StateConstructor for RideState<B> {
    type BackendType = B;

    fn new(backend: Arc<Self::BackendType>) -> Self {
        Self {
            rides: EagerValue::new("_rides", backend),
        }
    }
}
