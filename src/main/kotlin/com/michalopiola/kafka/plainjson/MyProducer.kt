package com.michalopiola.kafka.plainjson


import com.michalopiola.model.Person
import com.github.javafaker.Faker
import com.michalopiola.util.jsonMapper
import com.michalopiola.util.logger
import com.michalopiola.util.personsTopic
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
        while (true) {
            val faker = Faker()
            val fakePerson = Person(
                faker.name().firstName(),
                faker.name().lastName(),
                faker.date().birthday()
            )
            val fakePersonJson = jsonMapper.writeValueAsString(fakePerson)
            val futureResult = producer.send(ProducerRecord(personsTopic, fakePersonJson))
            logger.info("Record: $fakePerson has been produced to the topic")
            futureResult.get()
        }
    }
}