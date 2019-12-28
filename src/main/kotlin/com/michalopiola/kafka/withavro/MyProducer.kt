package com.michalopiola.kafka.withavro

import com.github.javafaker.Faker
import com.michalopiola.kafka.withcustomserde.PersonSerializer
import com.michalopiola.model.Person
import com.michalopiola.util.logger
import com.michalopiola.util.personsTopic
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.io.File
import java.util.*

class MyProducer(brokers: String, schemaRegistryUrl: String) {

    private val producer = createProducer(brokers, schemaRegistryUrl)
    private val schema = Schema.Parser().parse(File("src/main/resources/persons.avsc"))

    private fun createProducer(brokers: String, schemaRegistryUrl: String): Producer<String, GenericRecord> {
        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["key.serializer"] = StringSerializer::class.java.canonicalName
        props["value.serializer"] = KafkaAvroSerializer::class.java.canonicalName
        props["schema.registry.url"] = schemaRegistryUrl
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

            val avroPerson = GenericRecordBuilder(schema).apply {
                set("firstName", fakePerson.firstName)
                set("lastName", fakePerson.lastName)
                set("birthDate", fakePerson.birthDate.time)
            }.build()

            val futureResult = producer.send(ProducerRecord(personsTopic, avroPerson))
            logger.info("Record: $fakePerson has been produced to the topic")
            futureResult.get()
        }
    }
}