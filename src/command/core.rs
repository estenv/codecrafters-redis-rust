use super::handlers::get_timestamp;
use crate::{
    common::parse_string_args,
    protocol::{Data, RedisArray},
    store::{coords::Point, value::Value},
};
use rust_decimal::Decimal;

#[derive(Clone)]
pub enum Command {
    Ping,
    Echo(String),
    Get(String),
    Set {
        key: String,
        value: Value,
        expiry: Option<u64>,
        raw_command: String,
    },
    ConfigGet(String),
    Keys(String),
    Info,
    Psync(String, String),
    Replconf,
    ReplconfGetAck(String),
    ReplconfAck(usize),
    Wait {
        num_replicas: i64,
        timeout: u64,
    },
    Type(String),
    XAdd {
        key: String,
        id: String,
        entry: (String, String),
    },
    XRange {
        key: String,
        start: String,
        end: String,
    },
    XRead {
        streams: Vec<(String, String)>,
        block: Option<u64>,
    },
    Incr {
        key: String,
        raw_command: String,
    },
    Multi,
    Exec,
    Invalid,
    Discard,
    ListPush {
        key: String,
        values: Vec<String>,
        is_left: bool,
        raw_command: String,
    },
    LRange {
        key: String,
        start: isize,
        end: isize,
    },
    LLen(String),
    LPop(String, usize),
    BLPop(Vec<String>, u64),
    Transaction(Vec<Command>),
    Subscribe(String),
    Publish(String, String),
    ZAdd {
        key: String,
        score: Decimal,
        member: String,
    },
    ZRank {
        key: String,
        member: String,
    },
    ZRange {
        key: String,
        start: isize,
        end: isize,
    },
    ZCard(String),
    ZScore(String, String),
    ZRem(String, String),
    Geoadd {
        key: String,
        point: Point,
        member: String,
    },
    Geopos {
        key: String,
        members: Vec<String>,
    },
    Geodist {
        key: String,
        from: String,
        to: String,
    },
    Geosearch {
        key: String,
        point: Point,
        radius: f64,
        unit: String,
    },
    Acl(String, Vec<String>),
}

impl From<Data> for Command {
    fn from(val: Data) -> Self {
        match val {
            Data::Array(arr) => Command::from(arr.as_slice()),
            _ => Command::Invalid,
        }
    }
}

impl From<&[Data]> for Command {
    fn from(val: &[Data]) -> Self {
        let Some(Data::BStr(command)) = val.first() else {
            return Command::Invalid;
        };
        match (command.to_uppercase().as_str(), &val[1..]) {
            ("PING", []) => Command::Ping,
            ("ECHO", [Data::BStr(value)]) => Command::Echo(value.into()),
            ("GET", [Data::BStr(key)]) => Command::Get(key.into()),
            (
                "SET",
                [Data::BStr(key), Data::BStr(value), Data::BStr(param), Data::BStr(expiry_ms)],
            ) if param.eq_ignore_ascii_case("PX") => Command::Set {
                key: key.into(),
                value: value.to_string().into(),
                expiry: get_timestamp(expiry_ms),
                raw_command: get_raw_array_command(val),
            },
            ("SET", [Data::BStr(key), Data::BStr(value)]) => Command::Set {
                key: key.into(),
                value: value.to_string().into(),
                expiry: None,
                raw_command: get_raw_array_command(val),
            },
            ("CONFIG", [Data::BStr(arg), Data::BStr(key)]) if arg.eq_ignore_ascii_case("GET") => {
                Command::ConfigGet(key.into())
            }
            ("KEYS", [Data::BStr(pattern)]) => Command::Keys(pattern.into()),
            ("INFO", [Data::BStr(section)]) if section.eq_ignore_ascii_case("REPLICATION") => {
                Command::Info
            }
            ("PSYNC", [Data::BStr(replica_id), Data::BStr(offset)]) => {
                Command::Psync(replica_id.into(), offset.into())
            }
            ("REPLCONF", [Data::BStr(subcmd), Data::BStr(arg), ..])
                if subcmd.eq_ignore_ascii_case("GETACK") && arg == "*" =>
            {
                Command::ReplconfGetAck(arg.into())
            }
            ("REPLCONF", [Data::BStr(subcmd), Data::BStr(offset), ..])
                if subcmd.eq_ignore_ascii_case("ACK") && is_number(offset) =>
            {
                Command::ReplconfAck(offset.parse::<usize>().unwrap())
            }
            ("REPLCONF", [..]) => Command::Replconf,
            ("WAIT", [Data::BStr(num), Data::BStr(timeout)])
                if is_number(num) && is_number(timeout) =>
            {
                Command::Wait {
                    num_replicas: num.parse::<i64>().unwrap(),
                    timeout: timeout.parse::<u64>().unwrap(),
                }
            }
            ("TYPE", [Data::BStr(key)]) => Command::Type(key.into()),
            (
                "XADD",
                [Data::BStr(key), Data::BStr(id), Data::BStr(entry_key), Data::BStr(entry_value)],
            ) => Command::XAdd {
                key: key.into(),
                id: id.into(),
                entry: (entry_key.into(), entry_value.into()),
            },
            ("XRANGE", [Data::BStr(key), Data::BStr(start), Data::BStr(end)]) => Command::XRange {
                key: key.into(),
                start: start.into(),
                end: end.into(),
            },
            ("XREAD", ..) => parse_xread(val),
            ("INCR", [Data::BStr(key)]) => Command::Incr {
                key: key.into(),
                raw_command: get_raw_array_command(val),
            },
            ("MULTI", ..) => Command::Multi,
            ("EXEC", ..) => Command::Exec,
            ("DISCARD", ..) => Command::Discard,
            ("RPUSH", [Data::BStr(key), ..]) => Command::ListPush {
                key: key.into(),
                raw_command: get_raw_array_command(val),
                values: parse_string_args(&val[2..]),
                is_left: false,
            },
            ("LPUSH", [Data::BStr(key), ..]) => Command::ListPush {
                key: key.into(),
                raw_command: get_raw_array_command(val),
                values: parse_string_args(&val[2..]),
                is_left: true,
            },
            ("LRANGE", [Data::BStr(key), Data::BStr(start), Data::BStr(end)])
                if is_number(start) && is_number(end) =>
            {
                Command::LRange {
                    key: key.into(),
                    start: start.parse::<isize>().unwrap(),
                    end: end.parse::<isize>().unwrap(),
                }
            }
            ("LLEN", [Data::BStr(key)]) => Command::LLen(key.into()),
            ("LPOP", [Data::BStr(key), Data::BStr(count)]) if is_number(count) => {
                Command::LPop(key.into(), count.parse::<usize>().unwrap())
            }
            ("LPOP", [Data::BStr(key)]) => Command::LPop(key.into(), 1),
            ("BLPOP", [..]) => parse_blpop(&val[1..]),
            ("SUBSCRIBE", [Data::BStr(channel)]) => Command::Subscribe(channel.into()),
            ("PUBLISH", [Data::BStr(channel), Data::BStr(message)]) => {
                Self::Publish(channel.to_string(), message.to_string())
            }
            ("ZADD", [Data::BStr(key), Data::BStr(score), Data::BStr(member)]) => Self::ZAdd {
                key: key.clone(),
                score: Decimal::from_str_exact(score).unwrap_or_default(),
                member: member.clone(),
            },
            ("ZRANK", [Data::BStr(key), Data::BStr(member)]) => Self::ZRank {
                key: key.into(),
                member: member.into(),
            },
            ("ZRANGE", [Data::BStr(key), Data::BStr(start), Data::BStr(end)])
                if is_number(start) && is_number(end) =>
            {
                Command::ZRange {
                    key: key.into(),
                    start: start.parse::<isize>().unwrap(),
                    end: end.parse::<isize>().unwrap(),
                }
            }
            ("ZCARD", [Data::BStr(key)]) => Command::ZCard(key.into()),
            ("ZSCORE", [Data::BStr(key), Data::BStr(member)]) => {
                Command::ZScore(key.into(), member.into())
            }
            ("ZREM", [Data::BStr(key), Data::BStr(member)]) => {
                Command::ZRem(key.into(), member.into())
            }
            (
                "GEOADD",
                [Data::BStr(key), Data::BStr(long), Data::BStr(lat), Data::BStr(member)],
            ) => Self::Geoadd {
                key: key.into(),
                point: Point::new(lat, long),
                member: member.into(),
            },
            ("GEOPOS", [Data::BStr(key), ..]) => Self::Geopos {
                key: key.into(),
                members: parse_string_args(&val[2..]),
            },
            ("GEODIST", [Data::BStr(key), Data::BStr(member), Data::BStr(member_two)]) => {
                Self::Geodist {
                    key: key.into(),
                    from: member.into(),
                    to: member_two.into(),
                }
            }
            (
                "GEOSEARCH",
                [Data::BStr(key), _, Data::BStr(long), Data::BStr(lat), _, Data::BStr(radius), Data::BStr(unit)],
            ) => Self::Geosearch {
                key: key.into(),
                point: Point::new(lat, long),
                radius: radius.parse::<f64>().unwrap_or_default(),
                unit: unit.into(),
            },
            ("ACL", [Data::BStr(cmd), ..]) => Self::Acl(cmd.into(), parse_string_args(&val[2..])),
            _ => Command::Invalid,
        }
    }
}

impl Command {
    pub fn is_blocking(&self) -> bool {
        matches!(
            self,
            Command::XRead { block: Some(_), .. } | Command::BLPop(..)
        )
    }
}

fn is_number(val: &str) -> bool {
    val.chars()
        .all(|c| c.is_ascii_digit() || c == '.' || c == '-')
}

fn parse_xread(val: &[Data]) -> Command {
    let mut stream_start = 2usize;
    let block = match &val[1..=2] {
        [Data::BStr(arg), Data::BStr(ms)] if arg.eq_ignore_ascii_case("BLOCK") && is_number(ms) => {
            stream_start = 4;
            Some(ms.parse::<u64>().unwrap())
        }
        _ => None,
    };
    let mid = val[stream_start..].len() / 2;
    let (left, right) = val[stream_start..].split_at(mid);
    let streams = left
        .iter()
        .zip(right.iter())
        .filter_map(|(l, r)| match (l, r) {
            (Data::BStr(stream), Data::BStr(id)) => Some((stream.into(), id.into())),
            _ => None,
        })
        .collect();
    Command::XRead { streams, block }
}

fn parse_blpop(val: &[Data]) -> Command {
    let mut keys = vec![];
    for d in &val[0..val.len() - 1] {
        if let Data::BStr(arg) = d {
            keys.push(arg.to_string());
        }
    }
    let block = match val.last() {
        Some(Data::BStr(timeout)) if is_number(timeout) => {
            timeout.parse::<f64>().unwrap() * 1000f64
        }
        _ => return Command::Invalid,
    };
    Command::BLPop(keys, block as u64)
}

fn get_raw_array_command(val: &[Data]) -> String {
    RedisArray(val.to_vec()).into()
}
