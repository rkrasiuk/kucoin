extern crate reqwest;
extern crate tungstenite;
extern crate url;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate futures;
extern crate futures_backoff;
#[macro_use]
extern crate failure;

use url::Url;
use tungstenite::{WebSocket, Message, connect, client::AutoStream};
use serde::{Deserialize, Deserializer};
use std::io::Read;
use std::time::Duration;
use reqwest::StatusCode;
use futures::{Future, future};
use futures_backoff::retry;
use failure::Error;

const ACQUIRE_SERVER_URL: &str =
    "https://kitchen.kucoin.com/v1/bullet/usercenter/loginUser?protocol=websocket&encrypt=true";
const MARKET: &str = "ETH-BTC";

fn main() {
    match run_websocket() {
        Ok(_) => (),
        Err(e) => panic!("{}", e),
    };
}

fn run_websocket() -> Result<(), Error> {
    let bullet_token = get_token()?;

    let web_socket_url = format!(
        "wss://push1.kucoin.com/endpoint?bulletToken={}&format=json&resource=api",
        bullet_token
    );
    let (mut socket, _) = connect(Url::parse(&web_socket_url)?)?;

    println!("Connected to the server");

    socket.acknowledge()?;
    socket.subscribe(MARKET)?;

    loop {
        let msg = match socket.read_message() {
            Ok(msg) => msg,
            Err(err) => {
                println!("Error: {}", err);
                socket = reconnect(&web_socket_url)?;
                continue;
            },
        };
        if let Some(market_data) = parse_message(&msg)? {
            println!("Received: {:?}", market_data);
        }
    }
}

fn get_token() -> Result<String, Error> {
    let mut acquire_response = reqwest::get(ACQUIRE_SERVER_URL)?;

    match acquire_response.status() {
        StatusCode::OK => (),
        status => {
            return Err(format_err!(
                "Could not acquire websocket servers: {}",
                status
            ))
        }
    }

    let mut acquire_body = String::new();
    acquire_response.read_to_string(&mut acquire_body)?;
    parse_bullet_token(&acquire_body)
}

fn parse_bullet_token(res: &str) -> Result<String, Error> {
    let body: Response = serde_json::from_str(&res)?;
    if let Data::BulletToken {bullet_token} = body.data {
        return Ok(bullet_token.to_owned());
    }
    Err(
        format_err!("Failed to receive bullet token")
    )
}

#[derive(Deserialize, Debug)]
struct Response {
    data: Data
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum Data {
    BulletToken { 
        #[serde(rename = "bulletToken")]
        bullet_token: String
    },

    MarketData(MarketData),
}

#[derive(Deserialize, Debug)]
struct MarketData {
    #[serde(rename = "lastDealPrice")]
    price: f64,

    #[serde(rename = "vol")]
    volume: f64,

    #[serde(default = "default_exchange")]
    exchange: String,

    #[serde(rename = "datetime", deserialize_with = "duration_from_u64")]
    ts: Duration,

    #[serde(rename = "symbol")]
    market: String,
}

fn default_exchange() -> String {
    "kucoin".to_string()
}

fn duration_from_u64<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where D: Deserializer<'de>
{
    let s: u64 = Deserialize::deserialize(deserializer)?;
    Ok(Duration::from_secs(s))
}

trait SocketTask {
    fn subscribe(&mut self, &str) -> Result<(), Error>;
    fn acknowledge(&mut self) -> Result<(), Error>;
    fn ping(&mut self) -> Result<(), Error>;
}

impl SocketTask for WebSocket<AutoStream> {
    fn subscribe(&mut self, market: &str) -> Result<(), Error> {
        let sub = format!(r#"{{
            "id": 1,
            "type": "subscribe",
            "topic": "/market/{}_TICK",
            "req": 0,
        }}"#, market);

        self.write_message(Message::Text(sub))?;
        Ok(())
    }

    fn acknowledge(&mut self) -> Result<(), Error> {
        let ack = self.read_message()?;
        println!("Acknowledged: {}", ack);
        Ok(())
    }

    fn ping(&mut self) -> Result<(), Error> {
        let ping = r#"{
            "id": 1,
            "type": "ping",
        }"#;

        self.write_message(Message::Text(String::from(ping)))?;
        self.acknowledge()?;
        Ok(())
    }
}

fn reconnect(url: &str) -> Result<WebSocket<AutoStream>, Error> {
    println!("Attempting reconnection...");
    let url = Url::parse(url)?;
    let future = retry(|| {
        let socket = match connect(url.clone()) {
            Ok((sc, _)) => sc,
            Err(err) => {
                return future::err::<WebSocket<AutoStream>, tungstenite::Error>(err);
            },
        };
        future::ok::<WebSocket<AutoStream>, tungstenite::Error>(socket)
    });

    let mut socket = future.wait()?;
    println!("Reconnected.");

    socket.acknowledge()?;
    socket.subscribe(MARKET)?;
    Ok(socket)
}

fn parse_message(msg: &tungstenite::Message) -> Result<Option<MarketData>, Error> {
    let market_data: MarketData = parse_data(&msg)?;

    if market_data.price <= 0.0 || market_data.volume <= 0.0 {
        return Ok(None);
    }

    Ok(Some(market_data))
}

fn parse_data(msg: &tungstenite::Message) -> Result<MarketData, Error> {
    let body: Response = serde_json::from_str(msg.to_text()?)?;
    if let Data::MarketData (market_data) = body.data {
        return Ok(market_data)
    }

    Err(
        format_err!("Error parsing market data")
    )
}