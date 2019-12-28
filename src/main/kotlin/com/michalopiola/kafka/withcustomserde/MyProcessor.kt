package com.michalopiola.kafka.withcustomserde

import com.michalopiola.model.Person
import com.michalopiola.util.agesTopic
import com.michalopiola.util.jsonMapper
import com.michalopiola.util.logger
import com.michalopiola.util.personsTopic
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Duration
import java.time.LocalDate
import java.time.Period
import java.time.ZoneId
import java.util.*

class MyProcessor(brokers: String) {

    private val consumer = createConsumer(brokers)
    private val producer = createProducer(brokers)

    private fun createConsumer(brokers: String): Consumer<String, Person> {
        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["group.id"] = "person-processor"
        props["key.deserializer"] = StringDeserializer::class.java.canonicalName
        props["value.deserializer"] = PersonDeserializer::class.java.canonicalName
        return KafkaConsumer(props)
    }

    private fun createProducer(brokers: String): Producer<String, String> {
        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["key.serializer"] = StringSerializer::class.java.canonicalName
        props["value.serializer"] = StringSerializer::class.java.canonicalName
        return KafkaProducer<String, String>(props)
    }

    fun process() {
        consumer.subscribe(listOf(personsTopic))
        while(true) {
            val records = consumer.poll(Duration.ofSeconds(1))
            logger.info("The number of consumed records: ${records.count()}")
            records.forEach {
                val person = it.value()
                val birthLocalDate = person.birthDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate()
                val age = Period.between(birthLocalDate, LocalDate.now()).years
                val future = producer.send(ProducerRecord(agesTopic, "${person.firstName} ${person.lastName}", "$age"))
                future.get()
            }
        }
    }
}