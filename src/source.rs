use arcon::prelude::*;
use std::{
    cell::RefCell,
    fs::File,
    io::{BufRead, BufReader},
    str::FromStr,
};

const RESCHEDULE_EVERY: usize = 10000;

pub struct TaxiSource<A>
where
    A: ArconType + FromStr,
{
    lines: RefCell<Vec<String>>,
    conf: SourceConf<A>,
}

impl<A> TaxiSource<A>
where
    A: ArconType + FromStr,
{
    pub fn new(file_path: impl Into<String>, conf: SourceConf<A>) -> Self {
        let f = File::open(file_path.into()).expect("failed to open file");
        let reader = BufReader::new(f);
        let lines = reader
            .lines()
            .collect::<std::io::Result<Vec<String>>>()
            .expect("");
        TaxiSource {
            lines: RefCell::new(lines),
            conf,
        }
    }
}

impl<A> Source for TaxiSource<A>
where
    A: ArconType + FromStr,
{
    type Data = A;

    fn process_batch(&self, mut ctx: SourceContext<Self, impl ComponentDefinition>) {
        let drain_to = RESCHEDULE_EVERY.min(self.lines.borrow().len());
        for line in self.lines.borrow_mut().drain(..drain_to) {
            if let Ok(record) = line.parse::<A>() {
                match &self.conf.time {
                    ArconTime::Event => match &self.conf.extractor {
                        Some(extractor) => {
                            let timestamp = extractor(&record);
                            ctx.output_with_timestamp(record, timestamp);
                        }
                        None => {
                            panic!("Cannot use ArconTime::Event without an timestamp extractor")
                        }
                    },
                    ArconTime::Process => ctx.output(record),
                }
            }
            // TODO: log error
        }

        if self.lines.borrow().is_empty() {
            ctx.signal_end();
        }
    }
}
