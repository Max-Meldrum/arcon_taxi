use arcon::prelude::*;
use elasticsearch::{http::headers::HeaderMap, http::Method, Elasticsearch, SearchParts};
use serde_json::json;
use serde_json::Value;
use tokio::runtime::Runtime;

use crate::data;

struct Kibana {
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
        let body = json!({
            "pu_location_id": element.data.pu_location_id,
            "avg_duration": element.data.avg_duration,
        });
        let body: &[u8] = body.as_str().unwrap().as_ref();
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(self.client.send(
                Method::Post,
                SearchParts::Index(&[""]).url().as_ref(),
                HeaderMap::new(),
                Option::<&Value>::None,
                Some(body),
                None,
            ))
            .unwrap();
        Ok(())
    }

    arcon::ignore_timeout!();
    arcon::ignore_persist!();

    fn state(&mut self) -> &mut Self::OperatorState {
        &mut self.state
    }
}
