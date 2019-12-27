package com.michalopiola

import com.michalopiola.kafka.withcustomserde.MyProcessor
import com.michalopiola.kafka.withcustomserde.MyProducer

fun main() {
    //MyProducer("localhost:9092").produce()
    MyProcessor("localhost:9092").process()
}