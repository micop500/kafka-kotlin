package com.michalopiola.kafka.withcustomserde

import com.michalopiola.model.Person
import com.michalopiola.util.jsonMapper
import org.apache.kafka.common.serialization.Deserializer

class PersonDeserializer : Deserializer<Person> {
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {}

    override fun deserialize(topic: String?, data: ByteArray?): Person {
        return jsonMapper.readValue(data, Person::class.java)
    }

    override fun close() {}
}