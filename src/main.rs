extern crate reqwest;
extern crate serde_json;

use std::io::{Read};
use std::time::Duration;
use reqwest::{StatusCode};

#[derive(Debug)]
struct MarketData {
    market: String,
    price: f64,
    volume: f64,
    exchange: String,
    ts: Duration,
}

fn main() {
    let market = String::from("ETH-BTC");
    let request_url = String::from(format!("https://api.kucoin.com/v1/open/tick?symbol={}", market));

    let mut res = reqwest::get(&request_url).expect("response failed");
    match res.status() {
        StatusCode::OK => (),
        status => panic!("failed to get the response: {}", status),
    }

    let mut body = String::new();
    res.read_to_string(&mut body).unwrap();
    let body: serde_json::value::Value = serde_json::from_str(&mut body).unwrap();

    let market_data: MarketData = match parse_data(&body) {
        Some(md) => md,
        None => panic!("failed to parse response")
    };
    println!("{:?}", market_data);
}

fn parse_data(body: &serde_json::value::Value) -> Option<MarketData> {
    let timestamp = Duration::from_secs(body["timestamp"].as_u64().expect("no timestamp provided"));
    let data = body["data"].as_object().expect("no data in body");
    let price = match data["lastDealPrice"].as_f64() {
        Some(n) => {
            if n == 0.0 {
                return None
            }
            n
        },
        None => return None,
    };
    let volume = match data["vol"].as_f64() {
        Some(n) => {
            if n == 0.0 {
                return None
            }
            n
        },
        None => return None,
    };
    let coin_type = data["coinType"].as_str().unwrap();
    let coin_type_pair = data["coinTypePair"].as_str().unwrap();
    Some(
        MarketData {
            market: String::from(format!("{}{}", coin_type, coin_type_pair)),
            price,
            volume,
            exchange: String::from("kucoin"),
            ts: timestamp,
        }
    )
}