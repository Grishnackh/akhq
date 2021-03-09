package org.akhq.models;

import org.akhq.utils.AvroToJsonSerializer;
import org.akhq.utils.ProtobufToJsonDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

public class ProtoBufValueSchemaRecord extends Record {
    private final Record record;
    private final ProtobufToJsonDeserializer protoBufDeserializer;

    public ProtoBufValueSchemaRecord(Record record, ProtobufToJsonDeserializer protoBufDeserializer) {
        this.record = record;
        this.protoBufDeserializer = protoBufDeserializer;
    }

    @Override
    public String getValue() {
        if(this.value == null) {
            try {
                String record = protoBufDeserializer.deserialize(topic, bytesValue, false);
                if (record != null) {
                    this.value = record;
                }
            } catch (Exception exception) {
                this.exceptions.add(exception.getMessage());

                this.value = new String(bytesValue);
            }
        }

        return this.value;
    }
}