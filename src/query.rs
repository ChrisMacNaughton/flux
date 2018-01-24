// Copyright 2016 Chris MacNaughton <chris.macnaughton@canonical.com>
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::fmt;
use std::fmt::Display;

pub struct Query {
    select: Vec<String>,
    from: Vec<String>,
    query_where: Vec<String>,
    group_by: Vec<String>,
    order: Vec<String>,
    fill: Option<String>,
    limit: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::Query;

    #[test]
    fn it_builds_a_query() {
        let query = Query::new().select("*").from("io_reads").query_where("time > now() - 30m");

        assert_eq!(format!("{}", query), "SELECT * FROM io_reads WHERE time > now() - 30m");
    }

    #[test]
    fn it_handles_empty_where() {
        let query = Query::new().select("*").from("io_reads").query_where("time > now() - 30m").query_where("");

        assert_eq!(format!("{}", query), "SELECT * FROM io_reads WHERE time > now() - 30m");
    }

    #[test]
    fn it_builds_a_query_with_multiple_wheres() {
        let query = Query::new().select("*").from("io_reads").query_where("time > now() - 30m").query_where("a").query_where("b");

        assert_eq!(format!("{}", query), "SELECT * FROM io_reads WHERE time > now() - 30m AND a AND b");
    }

    #[test]
    fn it_builds_complex_queries() {
        let query = Query::new().select("*")
            .from("io_reads").query_where("time > now() - 30m")
            .query_where("a").query_where("b")
            .order("time desc")
            .group_by("b")
            .limit(10);

        assert_eq!(format!("{}", query), "SELECT * FROM io_reads WHERE time > now() - 30m AND a AND b GROUP BY b ORDER BY time desc LIMIT 10");
    }

    #[test]
    fn it_sets_up_limits() {
        let query = Query::new().limit(1);

        assert_eq!(format!("{}", query), "SELECT * LIMIT 1");
    }
}

impl Query {
    pub fn new() -> Query {
        Query {
            select: vec![],
            from: vec![],
            query_where: vec![],
            group_by: vec![],
            order: vec![],
            fill: None,
            limit: None,
        }
    }

    pub fn select<T: Display>(mut self, part: T) -> Query {
        self.select.push(part.to_string());
        self
    }

    pub fn filter<T: Display>(mut self, part: T) -> Query {
        self.query_where.push(part.to_string());
        self
    }

    pub fn query_where<T: Display>(mut self, part: T) -> Query {
        self.query_where.push(part.to_string());
        self
    }

    pub fn from<T: Display>(mut self, part: T) -> Query {
        self.from.push(part.to_string());
        self
    }

    pub fn group_by<T: Display>(mut self, part: T) -> Query {
        self.group_by.push(part.to_string());
        self
    }

    pub fn order<T: Display>(mut self, part: T) -> Query {
        self.order.push(part.to_string());
        self
    }

    pub fn fill<T: Display>(mut self, part: T) -> Query {
        self.fill = Some(part.to_string());
        self
    }

    pub fn limit(mut self, part: usize) -> Query {
        self.limit = Some(part);
        self
    }
}

impl fmt::Display for Query{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // match self.clone() {
        //     InfluxValue::String(s) => write!(f, "{}", s),
        //     InfluxValue::Int(n) => write!(f, "{}", n),
        //     InfluxValue::Float(n) => write!(f, "{}", n)
        // }
        let _ = write!(f, "SELECT ");
        let count = self.select.len();
        if count > 0 {
            let mut idx = 1;
            for select in &self.select {
                let _ = write!(f, "{}", select);
                if count != idx {
                    let _ = write!(f, ", ");
                }

                idx += 1;
            }
        } else {
            let _ = write!(f, "*");
        }

        let count = self.from.len();
        if count > 0 {
            let _ = write!(f, " FROM ");
            let mut idx = 1;
            for from in &self.from {
                let _ = write!(f, "{}", from);
                if count != idx {
                    let _ = write!(f, ", ");
                }

                idx += 1;
            }
        }


        let count = self.query_where.len();
        if count > 0 {
            let _ = write!(f, " WHERE ");
            let mut idx = 1;
            for query_where in &self.query_where {
                if query_where.len() == 0 {
                    continue
                }
                if idx > 1 {
                    let _ = write!(f, " AND ");
                }
                let _ = write!(f, "{}", query_where);

                idx += 1;
            }
        }

        let count = self.group_by.len();
        if count > 0 {
            let _ = write!(f, " GROUP BY ");
            let mut idx = 1;
            for group_by in &self.group_by {
                if group_by.len() == 0 {
                    continue
                }
                let _ = write!(f, "{}", group_by);
                if count != idx {
                    let _ = write!(f, ", ");
                }

                idx += 1;
            }
        }

        let count = self.order.len();
        if count > 0 {
            let _ = write!(f, " ORDER BY ");
            let mut idx = 1;
            for order in &self.order {
                if order.len() == 0 {
                    continue
                }
                let _ = write!(f, "{}", order);
                if count != idx {
                    let _ = write!(f, ", ");
                }

                idx += 1;
            }
        }

        if let Some(ref fill) = self.fill {
            let _ = write!(f, " FILL({})", fill);
        }

        if let Some(ref limit) = self.limit {
            let _ = write!(f, " LIMIT {}", limit);
        }
        write!(f, "")
    }
}
