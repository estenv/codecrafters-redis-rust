use super::{
    replica::{replica_stream_handler, ReplicaManager, ReplicaState},
    stream_reader::StreamReader,
};
use crate::{
    command::{
        core::Command,
        handlers::{self},
        response::{
            array_of_arrays_response, array_response, bstring_response, error_response,
            int_response, null_array_response, null_response, sstring_response, CommandResponse,
        },
        stream_handlers,
    },
    server::{config, state::ServerState},
    store::{core::InMemoryStore, list::blpop_handler},
};
use std::sync::Arc;
use tokio::{
    net::TcpStream,
    sync::{mpsc, Mutex},
};

#[derive(Clone, Default)]
pub struct ServerContext {
    pub store: InMemoryStore,
    pub state: Arc<Mutex<ServerState>>,
    pub replicas: Arc<Mutex<ReplicaManager>>,
    pub channels: crate::channel::ChannelManager,
}

impl ServerContext {
    pub async fn execute_command(&self, request: Command) -> CommandResponse {
        match request {
            Command::Ping => sstring_response("PONG"),
            Command::Echo(val) => bstring_response(&val),
            Command::Get(key) => CommandResponse::Single(handlers::get(&key, &self.store).await),
            Command::Set {
                key,
                value,
                expiry,
                raw_command,
            } => {
                self.store.set(key.to_string(), value.clone(), expiry).await;
                self.propagate(raw_command).await;
                sstring_response("OK")
            }
            Command::ConfigGet(key) => match config::get_config_value(&key) {
                Some(value) => array_response(vec![key, value]),
                _ => null_response(),
            },
            Command::Keys(pattern) => {
                CommandResponse::Single(handlers::keys(&pattern, &self.store).await)
            }
            Command::Info => CommandResponse::Single(handlers::info(self).await),
            Command::Replconf => sstring_response("OK"),
            Command::ReplconfGetAck(_) => CommandResponse::ReplconfAck,
            Command::Wait {
                num_replicas,
                timeout,
            } => int_response(handlers::wait(self, num_replicas, timeout).await),
            Command::Type(key) => sstring_response(
                handlers::type_handler(key.as_str(), &self.store)
                    .await
                    .as_ref(),
            ),
            Command::XAdd { key, id, entry } => match self.store.add_stream(key, id, entry).await {
                Ok(res) => bstring_response(&res),
                Err(e) => error_response(&e.to_string()),
            },
            Command::XRange { key, start, end } => {
                stream_handlers::xrange(key, start, end, &self.store)
                    .await
                    .unwrap_or(null_response())
            }
            Command::XRead { streams, block } => {
                stream_handlers::xread(streams, block, &self.store)
                    .await
                    .unwrap_or(null_array_response())
            }
            Command::Incr { key, raw_command } => {
                let value = self.store.incr(key.clone()).await;
                match value {
                    0 => error_response("value is not an integer or out of range"),
                    _ => {
                        self.propagate(raw_command).await;
                        int_response(value)
                    }
                }
            }
            Command::ListPush {
                key,
                values,
                raw_command,
                is_left,
            } => match self.store.list_push(key, values, is_left).await {
                Ok(len) => {
                    self.propagate(raw_command).await;
                    int_response(len as i64)
                }
                Err(e) => error_response(&e.to_string()),
            },
            Command::LRange { key, start, end } => {
                array_response(self.store.list_range(key, start, end).await)
            }
            Command::LLen(key) => int_response(self.store.list_len(key).await as i64),
            Command::LPop(key, count) => match self.store.list_pop(key, count).await {
                Some(values) if values.len() == 1 => bstring_response(&values[0]),
                Some(values) => array_response(values),
                None => null_response(),
            },
            Command::BLPop(keys, block_ms) => {
                match blpop_handler(&self.store, keys, block_ms).await {
                    Some(value) => array_response(value),
                    None => null_array_response(),
                }
            }
            Command::Multi => sstring_response("OK"),
            Command::Publish(channel, message) => {
                int_response(self.channels.publish(channel, message).await as i64)
            }
            Command::ZAdd { key, score, member } => {
                int_response(self.store.zadd(key, score, member).await)
            }
            Command::ZRank { key, member } => match self.store.zrank(key, member).await {
                Some(rank) => int_response(rank as i64),
                _ => null_response(),
            },
            Command::ZRange { key, start, end } => {
                array_response(self.store.zrange(key, start, end).await.unwrap_or(vec![]))
            }
            Command::ZCard(key) => int_response(self.store.zcard(key).await.unwrap_or(0)),
            Command::ZScore(key, member) => match self.store.zscore(key, member).await {
                Some(score) => bstring_response(score.to_string().as_ref()),
                _ => null_response(),
            },
            Command::ZRem(key, member) => int_response(self.store.zrem(key, member).await),
            Command::Geoadd { key, point, member } => {
                match crate::store::coords::validate_coords(&point) {
                    None => int_response(self.store.geoadd(key, point, member).await),
                    Some(err) => error_response(&err),
                }
            }
            Command::Geopos { key, members } => match self.store.geopos(key, members).await {
                arr if arr.is_empty() => null_array_response(),
                arr => array_of_arrays_response(arr),
            },
            Command::Geodist { key, from, to } => match self.store.geodist(key, from, to).await {
                Some(dist) => bstring_response(&dist),
                None => null_response(),
            },
            Command::Geosearch {
                key,
                point,
                radius,
                unit,
            } => array_response(self.store.geosearch(key, point, radius, unit).await),

            Command::Acl(command, args) => {
                bstring_response(crate::auth::acl(command, args).as_str())
            }
            _ => null_response(),
        }
    }

    pub async fn process_transaction(&self, commands: Vec<Command>) -> CommandResponse {
        let mut responses = Vec::new();
        for command in commands {
            if let CommandResponse::Single(res) = self.execute_command(command).await {
                responses.push(res);
            }
        }
        CommandResponse::Multiple(responses)
    }

    pub async fn add_replica(&self, reader: StreamReader<TcpStream>) {
        let (tx, rx) = mpsc::channel::<Vec<u8>>(100);
        let replica_id = uuid::Uuid::new_v4().to_string();
        let replica_state = Arc::new(Mutex::new(ReplicaState::new(tx.clone())));
        let state_clone = replica_state.clone();
        if handlers::psync(&tx, self).await.is_err() {
            eprintln!("Replica sync failed");
        }
        self.replicas
            .lock()
            .await
            .add_channel(replica_id, replica_state);

        tokio::spawn(async move {
            replica_stream_handler(reader, rx, state_clone).await;
        });
    }

    async fn propagate(&self, command: String) {
        self.replicas
            .lock()
            .await
            .broadcast(command.into_bytes())
            .await;
    }
}
