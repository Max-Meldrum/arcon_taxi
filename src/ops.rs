use arcon::prelude::*;
use elasticsearch::Elasticsearch;
use serde_json::json;
use tokio::runtime::Runtime;

use crate::data;

pub struct Kibana {
    state: (),
    client: Elasticsearch,
    runtime: Runtime,
}

impl Default for Kibana {
    fn default() -> Self {
        Self {
            state: (),
            client: Elasticsearch::default(),
            runtime: Runtime::new().unwrap(),
        }
    }
}

impl Operator for Kibana {
    type IN = data::RideWindowedData;
    type OUT = ArconNever;
    type TimerState = ArconNever;
    type OperatorState = ();

    fn handle_element(
        &mut self,
        element: ArconElement<Self::IN>,
        _ctx: OperatorContext<Self, impl Backend, impl ComponentDefinition>,
    ) -> OperatorResult<()> {
        self.runtime.block_on(send(
            &mut self.client,
            element.data,
            element.timestamp.unwrap(),
        ));
        Ok(())
    }

    arcon::ignore_timeout!();
    arcon::ignore_persist!();

    fn state(&mut self) -> &mut Self::OperatorState {
        &mut self.state
    }
}

async fn send(client: &mut elasticsearch::Elasticsearch, data: data::RideWindowedData, time: u64) {
    // Data to be posted to Kibana
    // Send data and block
    let time = chrono::NaiveDateTime::from_timestamp(time as i64, 0).to_string();
    println!("Time: {}", time);
    client
        .index(elasticsearch::IndexParts::IndexTypeId(
            "arcon_data_stream",
            "_doc",
            &format!("location_{}", data.pu_location_id),
        ))
        .body(json!({
            "time": time,
            // Keys
            "pu_location_id": data.pu_location_id,
            // Aggregates
            "count": data.count,

            "sum_fare_amount": data.sum_fare_amount,
            "max_fare_amount": data.max_fare_amount,
            "avg_fare_amount": data.avg_fare_amount,
            "min_fare_amount": data.min_fare_amount,

            "sum_trip_distance": data.sum_trip_distance,
            "avg_trip_distance": data.avg_trip_distance,

            "sum_passenger_count": data.sum_passenger_count,
            "max_passenger_count": data.max_passenger_count,
            "avg_passenger_count": data.avg_passenger_count,
            "min_passenger_count": data.min_passenger_count,

            "sum_duration": data.sum_duration,
            "max_duration": data.max_duration,
            "avg_duration": data.avg_duration,
            "min_duration": data.min_duration,
        }))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
}
