// Copyright 2016 Chris MacNaughton <chris.macnaughton@canonical.com>
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

extern crate hyper;
extern crate json;
#[macro_use] extern crate log;

use std::fmt;
use std::fmt::Display;

use std::collections::BTreeMap;
use std::io::Read;

use hyper::Client;

mod query;

pub use query::Query;

#[cfg(test)]
mod tests {
    use super::{InfluxResponse, InfluxValue};

    #[test]
    fn it_parses_a_response() {
        let src = r#"{
            "results": [{
                "series": [{
                    "name": "status",
                    "columns": ["time", "write_speed"],
                    "values": [
                        ["2016-03-29T13:58:20.000000001Z", 0]
                    ]
                }]
            }]
        }"#;

        let response = InfluxResponse::from_json(&src);
        // println!("Response is {:?}", response);
        let data = response.results.first().unwrap().series.first().unwrap();
        // let data = InfluxData::from_influx_result(result);
        println!("Data is: {:?}", data);
        let result = data.results.first().unwrap();
        println!("Result is {:?}", result);
        let actual = result.get("write_speed").unwrap();
        println!("Actual: {:?}", actual);
        assert_eq!(*actual, InfluxValue::Float(0f64));
    }

    #[test]
    fn it_parses_a_response_with_many_values() {
        let src = r#"{
            "results": [{
                "series": [{
                    "name": "mon_daemon",
                    "columns": ["time", "avail", "hostname", "monitors", "monitors_quorum", "objects", "objects_degraded", "objects_unfound", "osd_epoch", "osds", "osds_in", "osds_up", "pgs", "pgs_active_clean", "pgs_peering", "ppgs_active", "total", "type", "used"],
                    "values": [
                        ["2016-03-29T18:21:38Z", 9284852, "ip-172-31-21-156", 3, 3, 0, 0, 0, 9, 3, 3, 3, 192, 0, 0, 0, 9386436, "monitor", 101584]
                    ]
                }]
            }]
        }"#;

        let response = InfluxResponse::from_json(&src);
        // println!("Response is {:?}", response);
        let data = response.results.first().unwrap().series.first().unwrap();
        println!("Data is: {:?}", data);
        let result = data.results.first().unwrap();
        println!("Result is {:?}", result);
        let actual = result.get("monitors").unwrap();
        println!("Actual: {:?}", actual);
        assert_eq!(*actual, InfluxValue::Float(3f64));
    }

    #[test]
    fn it_parses_a_response_with_tags() {
        let src = r#"{
            "results": [{
                "series": [{
                    "name": "disk_total",
                    "tags": {
                        "path": "/var/lib/ceph/osd/ceph-0"
                    },
                    "columns": ["time", "fstype", "host", "value"],
                    "values": [
                        ["2016-03-29T19:03:03Z", "xfs", "ip-172-31-29-130", 3203903488]
                    ]
                }, {
                    "name": "disk_total",
                    "tags": {
                        "path": "/var/lib/ceph/osd/ceph-1"
                    },
                    "columns": ["time", "fstype", "host", "value"],
                    "values": [
                        ["2016-03-29T19:03:03Z", "xfs", "ip-172-31-9-120", 3203903488]
                    ]
                }, {
                    "name": "disk_total",
                    "tags": {
                        "path": "/var/lib/ceph/osd/ceph-2"
                    },
                    "columns": ["time", "fstype", "host", "value"],
                    "values": [
                        ["2016-03-29T19:03:04Z", "xfs", "ip-172-31-60-174", 3203903488]
                    ]
                }, {
                    "name": "disk_used",
                    "tags": {
                        "path": "/var/lib/ceph/osd/ceph-0"
                    },
                    "columns": ["time", "fstype", "host", "value"],
                    "values": [
                        ["2016-03-29T19:03:03Z", "xfs", "ip-172-31-29-130", 35545088]
                    ]
                }, {
                    "name": "disk_used",
                    "tags": {
                        "path": "/var/lib/ceph/osd/ceph-1"
                    },
                    "columns": ["time", "fstype", "host", "value"],
                    "values": [
                        ["2016-03-29T19:03:03Z", "xfs", "ip-172-31-9-120", 34238464]
                    ]
                }, {
                    "name": "disk_used",
                    "tags": {
                        "path": "/var/lib/ceph/osd/ceph-2"
                    },
                    "columns": ["time", "fstype", "host", "value"],
                    "values": [
                        ["2016-03-29T19:03:04Z", "xfs", "ip-172-31-60-174", 34238464]
                    ]
                }]
            }]
        }"#;

        let response = InfluxResponse::from_json(&src);
        // println!("Response is {:?}", response);
        let data = response.results.first().unwrap().series.first().unwrap().clone();

        println!("Data is: {:?}", &data);
        let tags = &data.tags.unwrap();
        println!("Tags are: {:?}", tags);
        let result = data.results.first().unwrap();
        println!("Result is {:?}", result);
        let actual = result.get("fstype").unwrap();
        println!("Actual: {:?}", actual);
        assert_eq!(*actual, InfluxValue::String("xfs".to_string()));
    }
}


#[derive(Debug, Clone)]
pub struct InfluxResponse {
    pub results: Vec<InfluxSeries>,
}

#[derive(Debug, Clone)]
pub struct InfluxSeries {
    pub series: Vec<InfluxData>
}

impl InfluxResponse {
    fn from_json(input: &str) -> InfluxResponse {
        let mut response = InfluxResponse {
            results: vec![],
        };
        let data = match json::parse(input) {
            Ok(json) => json,
            Err(e) => {
                warn!("Error parsing JSON: {:?}", e);
                return response;
            }
        };

        if !data["results"].is_array() {
            return response;
        }

        
        for series in data["results"].members() {
            let mut influx_series = InfluxSeries {
                series: vec![],
            };
            for ref series in series["series"].members() {
                influx_series.series.push(InfluxData::from_json(series));
            }

            response.results.push(influx_series)
        }
        response
    }
}

// #[derive(Debug)]
// pub struct InfluxSeries {
//     pub series: InfluxData,
// }

/// InfluxData is the data struct that holds the result for a single query to Influx
///
/// The JSON for this looks like:
///
/// {
///     "name": "disk_total",
///     "tags": {
///         "path": "/var/lib/ceph/osd/ceph-0"
///     },
///     "columns": ["time", "fstype", "host", "value"],
///     "values": [
///         ["2016-03-29T19:03:03Z", "xfs", "ip-172-31-29-130", 3203903488]
///     ]
/// }

#[derive(Debug, Clone)]
pub struct InfluxData {
    pub name: String,
    pub tags: Option<BTreeMap<String, String>>,
    pub results: Vec<BTreeMap<String, InfluxValue>>,
}

impl InfluxData {
    fn from_json(value: &json::JsonValue) -> InfluxData {

        let len = value["columns"].len();
        let tags: Option<BTreeMap<String, String>> = if value["tags"].is_null() {
            None
        } else {
            let mut tags = BTreeMap::new();
            for (key, value) in value["tags"].entries() {
                tags.insert(key.to_string(), value.to_string());
            }
            Some(tags)
        };
        let mut result_values: Vec<BTreeMap<String, InfluxValue>> = Vec::new();
        for ref row in value["values"].members() {

            let mut values: BTreeMap<String, InfluxValue> = BTreeMap::new();
            for id in 0..len {
                let key = format!("{}", value["columns"][id]);
                let value = match row[id] {
                    json::JsonValue::String(ref s) => InfluxValue::String(s.to_string()),
                    json::JsonValue::Number(ref n) => InfluxValue::Float(n.clone()),
                    json::JsonValue::Array(_) |
                    json::JsonValue::Boolean(_) |
                    json::JsonValue::Null |
                    json::JsonValue::Object(_) => continue,
                };
                values.insert(key, value);
            }

            result_values.push(values);
        }

        InfluxData {
            name: format!("{}", value["name"]),
            tags: tags,
            results: result_values,
        }
    }
}


#[derive(PartialEq, Debug, Clone)]
pub enum InfluxValue {
    String(String),
    Float(f64)
}

impl InfluxValue {
    pub fn to_float(&self) -> f64 {
        match self.clone() {
            InfluxValue::Float(f) => f,
            _ => 0.0
        }
    }

    pub fn to_i(&self) -> u64 {
        match self.clone() {
            InfluxValue::Float(f) => f as u64,
            _ => 0u64
        }
    }
}

impl fmt::Display for InfluxValue{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.clone() {
            InfluxValue::String(s) => write!(f, "{}", s),
            // InfluxValue::Int(n) => write!(f, "{}", n),
            InfluxValue::Float(n) => write!(f, "{}", n)
        }
    }
}


#[derive(Debug)]
pub struct Influx {
    pub host: String,
}

impl Influx {
    pub fn query<T: Display>(&self, db: &str, queries: T) -> InfluxSeries {
        let result = self.query_batch(db, vec![queries]);
        for row in result {
            return row;
        }
        InfluxSeries {
            series: vec![],
        }
    }

    pub fn query_batch<T: Display>(&self, db: &str, queries: Vec<T>) -> Vec<InfluxSeries> {
        let mut full_query = format!("{}/query?db={}&q=", self.host, db);
        full_query.push_str(
            &queries
                .iter()
                .map(|q| format!("{}", q))
                .collect::<Vec<String>>()
                .join(";")[..]);


        let client = Client::new();

        match client.get(&full_query)
            .send() {
                Ok(mut res) => {
                    let mut body = String::new();
                    match res.read_to_string(&mut body) {
                        Ok(_) =>{},
                        Err(_) => return vec![],
                    }; 

                    InfluxResponse::from_json(&body).results
                },
                Err(e) => {
                    error!("Influx Error: {:?}", e);
                    vec![]
                }
            }
    }
}
