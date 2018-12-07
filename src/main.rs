extern crate reqwest;
extern crate tungstenite;
extern crate url;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;

use url::Url;
use tungstenite::{WebSocket, Message, connect, client::AutoStream};
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use std::io::Read;
use std::time::Duration;
use std::thread;
use reqwest::StatusCode;

fn main() {
    const ACQUIRE_SERVER_URL: &str = "https://kitchen.kucoin.com/v1/bullet/usercenter/loginUser?protocol=websocket&encrypt=true";
    const MARKET: &str = "ETH-BTC";

    let mut acquire_response = reqwest::get(ACQUIRE_SERVER_URL).expect("Acquire bullet token request failed");

    match acquire_response.status() {
        StatusCode::OK => (),
        status => panic!("Could not acquire websocket servers: {}", status),
    }

    let mut acquire_body = String::new();
    acquire_response.read_to_string(&mut acquire_body).expect("Failed to parse acquire response");
    let bullet_token = parse_bullet_token(&acquire_body).expect("Failed to parse bullet token");

    let web_socket_url = format!("wss://push1.kucoin.com/endpoint?bulletToken={}&format=json&resource=api", bullet_token);
    let (mut socket, _) = connect(Url::parse(&web_socket_url).unwrap())
        .expect("Can't connect to websocket");

    println!("Connected to the server");

    socket.acknowledge();

    let subscription = format!(r#"{{
        "id": 1,
        "type": "subscribe",
        "topic": "/market/{}_TICK",
        "req": 1,
    }}"#, MARKET);
    socket.subscribe(&subscription);
    
    loop {
        let msg = match socket.read_message() {
            Ok(msg) => msg,
            Err(err) => {
                println!("error: {}", err);
                println!("Attempting reconnection...");
                thread::sleep(Duration::from_millis(5000));
                if let Ok((mut re_socket, _)) = connect(Url::parse(&web_socket_url).unwrap()) {
                    socket = re_socket;
                    socket.acknowledge();
                    socket.subscribe(&subscription);
                    println!("Reconnected.");
                    continue;
                }
                println!("Closing the connection...");
                socket.close(None).expect("Failed to close the connection");
                break;
            },
        };
        if let Some(market_data) = parse_data(&msg) {
            println!("Received: {:?}", market_data);
        }
    }
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
    fn subscribe(&mut self, &str);
    fn acknowledge(&mut self);
}

impl SocketTask for WebSocket<AutoStream> {
    fn subscribe(&mut self, sub: &str) {
        self.write_message(Message::Text(sub.into())).unwrap();
        self.acknowledge();
    }

    fn acknowledge(&mut self) {
        let ack = self.read_message().expect("Error receiving ack");
        println!("Acknowledged: {}", ack);
    }
}

fn parse_bullet_token(res: &str) -> Result<String, serde_json::Error> {
    let bullet_token: Value = serde_json::from_str(&res)?;
    let bullet_token = bullet_token["data"]["bulletToken"].as_str().unwrap();
    Ok(bullet_token.to_owned())
}

fn parse_data(msg: &tungstenite::Message) -> Option<MarketData> {
    let body: serde_json::value::Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
    let market_data: MarketData = serde_json::from_str(&serde_json::to_string(&body["data"]).unwrap()).unwrap();
    if market_data.price == 0.0 || market_data.volume == 0.0 {
        return None;
    }
    Some(market_data)
}