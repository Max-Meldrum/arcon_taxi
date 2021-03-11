use arcon::prelude::*;

pub mod agg;
pub mod data;
pub mod ops;

use agg::window_sum;
use data::datetime_to_u64;
use data::RideData;
use data::RideState;
use data::RideWindowedData;
use data::TaxiRideData;

const DAY_DURATION: u64 = 24 * 60 * 60;

fn main() {
    let conf = ArconConf {
        epoch_interval: 20_000,
        ctrl_system_host: Some("127.0.0.1:2000".to_string()),
        allocator_capacity: 2147483648,
        ..Default::default()
    };

    let mut pipeline = Pipeline::with_conf(conf)
        .file("data/sorted_yellow_tripdata_2020.csv", |conf| {
            conf.set_arcon_time(ArconTime::Event);
            conf.set_timestamp_extractor(|x: &TaxiRideData| {
                datetime_to_u64(&x.tpep_pickup_datetime)
            });
        })
        .operator(OperatorBuilder {
            constructor: Arc::new(|_| Map::new(|x: TaxiRideData| RideData::from(x))),
            conf: Default::default(),
        })
        .operator(OperatorBuilder {
            constructor: Arc::new(|backend| {
                let function = AppenderWindow::new(backend.clone(), &window_sum);
                WindowAssigner::tumbling(function, backend, DAY_DURATION, DAY_DURATION, true)
            }),
            conf: OperatorConf {
                parallelism_strategy: ParallelismStrategy::Static(1),
                ..Default::default()
            },
        })
        .operator(OperatorBuilder {
            constructor: Arc::new(|backend| {
                Map::stateful(
                    RideState::new(backend),
                    |ride_per_location: RideWindowedData, state| {
                        state.rides().put(ride_per_location.clone())?;
                        Ok(ride_per_location)
                    },
                )
            }),
            conf: Default::default(),
        })
        .operator(OperatorBuilder {
            constructor: Arc::new(|_| ops::Kibana::default()),
            conf: OperatorConf {
                parallelism_strategy: ParallelismStrategy::Static(1),
                ..Default::default()
            },
        })
        .to_console()
        .build();
    pipeline.start();
    pipeline.await_termination();
}
