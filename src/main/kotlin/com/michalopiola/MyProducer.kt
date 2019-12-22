package com.michalopiola


import com.michalopiola.model.Person
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.util.StdDateFormat
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.github.javafaker.Faker
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

class MyProducer(brokers: String) {

    private val producer = createProducer(brokers)

    private fun createProducer(brokers: String): Producer<String, String> {
        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["key.serializer"] = StringSerializer::class.java.canonicalName
        props["value.serializer"] = StringSerializer::class.java.canonicalName
        return KafkaProducer(props)
    }

    fun produce() {
        val faker = Faker()
        val fakePerson = Person(
            faker.name().firstName(),
            faker.name().lastName(),
            faker.date().birthday()
        )
        val fakePersonJson = jsonMapper.writeValueAsString(fakePerson)
        val futureResult = producer.send(ProducerRecord(personsTopic, fakePersonJson))
        futureResult.get()
    }
}