package com.michalopiola

import com.michalopiola.kafka.MyProcessor
import com.michalopiola.kafka.MyProducer

fun main() {
    //MyProducer("localhost:9092").produce()
    MyProcessor("localhost:9092").process()
}