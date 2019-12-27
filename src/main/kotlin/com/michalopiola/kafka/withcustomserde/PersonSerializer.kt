package com.michalopiola.kafka.withcustomserde

import com.michalopiola.model.Person
import com.michalopiola.util.jsonMapper
import org.apache.kafka.common.serialization.Serializer

class PersonSerializer : Serializer<Person> {
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {}

    override fun serialize(topic: String?, data: Person?): ByteArray {
        return jsonMapper.writeValueAsBytes(data)
    }

    override fun close() {}
}