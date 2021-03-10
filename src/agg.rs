use crate::data::RideData;
use crate::data::RideWindowedData;

fn agg_u64(buffer: &[RideData], f: impl FnMut(&RideData) -> u64 + Copy) -> (u64, u64, u64, u64) {
    let count = buffer.len() as u64;
    let sum = buffer.iter().map(f).sum();
    let max = buffer.iter().map(f).max().unwrap();
    let min = buffer.iter().map(f).min().unwrap();
    let avg = sum / count;
    (sum, max, min, avg)
}

fn agg_f32(buffer: &[RideData], f: impl FnMut(&RideData) -> f32) -> (f32, f32) {
    let count = buffer.len() as f32;
    let sum = buffer.iter().map(f).sum();
    let avg = sum / count;
    (sum, avg)
}

pub fn window_sum(buffer: &[RideData]) -> RideWindowedData {
    let count = buffer.len() as u64;

    let (sum_fare_amount, max_fare_amount, avg_fare_amount, min_fare_amount) =
        agg_u64(buffer, |x| x.fare_amount);

    let (sum_trip_distance, avg_trip_distance) = agg_f32(buffer, |x| x.trip_distance);

    let (sum_passenger_count, max_passenger_count, avg_passenger_count, min_passenger_count) =
        agg_u64(buffer, |x| x.passenger_count);

    let (sum_duration, max_duration, avg_duration, min_duration) =
        agg_u64(buffer, |x| x.do_time.checked_sub(x.pu_time).unwrap_or(0));

    RideWindowedData {
        pu_location_id: buffer[0].pu_location_id,
        pu_time: buffer[0].pu_time,

        count,

        sum_fare_amount,
        max_fare_amount,
        avg_fare_amount,
        min_fare_amount,

        sum_trip_distance,
        avg_trip_distance,

        sum_passenger_count,
        max_passenger_count,
        avg_passenger_count,
        min_passenger_count,

        sum_duration,
        max_duration,
        avg_duration,
        min_duration,
    }
}
