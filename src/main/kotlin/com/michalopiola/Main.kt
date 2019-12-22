package com.michalopiola

import com.michalopiola.kafka.MyProducer

fun main() {
    MyProducer("localhost:9092").produce()
}