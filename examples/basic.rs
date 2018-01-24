// Copyright 2016 Chris MacNaughton <chris.macnaughton@canonical.com>
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

extern crate flux;

use std::env;

use flux::{Influx, Query};

fn main() {
    let host = if env::args().count() > 1 {
        env::args().nth(1).unwrap()
    } else {
        "127.0.0.1:8086".to_string()
    };

    let influx = Influx { host: host };
    // select mean(value) from cpu_usage_idle WHERE {} group by host, time(10s) fill(100)
    let query = Query::new()
        .select("mean(value)")
        .from("cpu_usage_idle")
        .query_where("time > now() - 15m AND time <= now()")
        .group_by("host, time(10s)")
        .fill("100");

    assert_eq!(
        format!("{}", query),
        "SELECT mean(value) FROM cpu_usage_idle WHERE time > now() - 15m AND time <= now() GROUP BY host, time(10s) FILL(100)"
    );

    let result = influx.query("telegraf", query);

    println!("{:?}", result);
}
