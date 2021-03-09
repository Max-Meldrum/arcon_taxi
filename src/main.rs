use arcon::prelude::*;
use arcon::arcon_decoder;

#[arcon_decoder(,)]
#[derive(Arcon, Arrow, prost::Message, Clone)]
#[arcon(unsafe_ser_id = 12, reliable_ser_id = 13, version = 1, keys = "id")]
pub struct TestData {
    #[prost(uint64)]
    pub id: u64,
    #[prost(string)]
    pub msg: String,
    #[prost(float)]
    pub f: f32,
}

fn main() {

    let mut pipeline = Pipeline::default()
        .file("test_data", |conf| {
            conf.set_arcon_time(ArconTime::Event);
            conf.set_timestamp_extractor(|x: &TestData| x.id);
        })
        .operator(OperatorBuilder {
            constructor: Arc::new(|_| Filter::new(|x: &TestData| x.f > 2.5)),
            conf: Default::default(),
        })
        .to_console()
        .build();

    pipeline.start();
    pipeline.await_termination();
}
