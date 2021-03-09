package org.akhq.models;

import org.akhq.utils.AvroToJsonSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

public class AvroKeySchemaRecord extends Record {
    private final Record record;
    private final Deserializer kafkaAvroDeserializer;

    public AvroKeySchemaRecord(Record record, Deserializer kafkaAvroDeserializer) {
        this.record = record;
        this.kafkaAvroDeserializer = kafkaAvroDeserializer;
    }

    @Override
    public String getKey() {
        if(this.key != null) {
            return this.key;
        }

        try {
            GenericRecord record = (GenericRecord) kafkaAvroDeserializer.deserialize(topic, bytesKey);
            return AvroToJsonSerializer.toJson(record);
        } catch (Exception exception) {
            this.exceptions.add(exception.getMessage());

            return new String(bytesKey);
        }
    }
}