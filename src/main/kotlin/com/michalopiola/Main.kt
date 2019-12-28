package com.michalopiola

import com.michalopiola.kafka.withavro.MyProcessor
import com.michalopiola.kafka.withavro.MyProducer

fun main() {
    //MyProducer("localhost:9092", "http://localhost:8081").produce()
    MyProcessor("localhost:9092", "http://localhost:8081").process()
}