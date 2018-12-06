extern crate reqwest;
extern crate tungstenite;
extern crate url;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;


use url::Url;
use tungstenite::{Message, connect};
use serde::{Deserialize, Deserializer};
use serde_json::{Value, Error};
use std::io::{Read};
use std::time::Duration;
use reqwest::{StatusCode};

fn main() {
    let acquire_server_url = String::from("https://kitchen.kucoin.com/v1/bullet/usercenter/loginUser?protocol=websocket&encrypt=true");
    let mut acquire_response = reqwest::get(&acquire_server_url).expect("request failed");

    match acquire_response.status() {
        StatusCode::OK => (),
        status => panic!("could not acquire websocket servers: {}", status),
    }

    let mut acquire_body = String::new();
    acquire_response.read_to_string(&mut acquire_body).expect("failed to parse acquire response");
    let bullet_token = parse_bullet_token(&acquire_body).expect("failed to parse bullet token");

    let web_socket_url = format!("wss://push1.kucoin.com/endpoint?bulletToken={}&format=json&resource=api", bullet_token);
    let (mut socket, response) = connect(Url::parse(&web_socket_url).unwrap())
        .expect("can't connect to websocket");

    println!("Connected to the server");
    println!("Response HTTP code: {}", response.code);
    println!("Response contains the following headers:");
    for &(ref header, _ /*value*/) in response.headers.iter() {
        println!("* {}", header);
    }

    let ack = socket.read_message().expect("Error reading message");
    println!("ack: {}", ack);

    let subscription = r#"{
        "id": 1,
        "type": "subscribe",
        "topic": "/market/ETH-BTC_TICK",
        "req": 1,
    }"#;
    socket.write_message(Message::Text(subscription.into())).unwrap();
    
    loop {
        let msg = socket.read_message().expect("Error reading message");
        println!("Received: {}", msg);
    }

    // socket.close(None);
}


fn parse_bullet_token(res: &String) -> Result<String, serde_json::Error> {
    let bullet_token: Value = serde_json::from_str(&res)?;
    let bullet_token = bullet_token["data"]["bulletToken"].as_str().unwrap();
    Ok(bullet_token.to_owned())
}