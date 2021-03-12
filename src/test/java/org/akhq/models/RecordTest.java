package org.akhq.models;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.akhq.Breed;
import org.akhq.Cat;
import org.akhq.models.decorators.AvroKeySchemaRecord;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class RecordTest {

    @Test
    public void testKeyByteArrayIsNull() {

        // GIVEN a record with null in key
        byte[] keyBytes = null;
        byte[] valueBytes = "".getBytes(StandardCharsets.UTF_8); // value property does not matter
        ConsumerRecord<byte[], byte[]> kafkaRecord = new ConsumerRecord<>("topic", 0, 0, keyBytes, valueBytes);
        Record record = new Record(kafkaRecord, null, null);

        // WHEN getKey() method is called
        String keyString = record.getKey();

        // EXPECT NULL result
        Assertions.assertNull(keyString);
    }

    @Test
    public void testKeyIsAvroSerialized() {

        // GIVEN a record with avro serialized key
        // WHEN getKey() method is called
        // EXPECT a string representation of the key bytes
        Assertions.fail();
    }

    @Test
    public void testKeyIsAvroSerializedWithDecorator() {

        // Testdata
        String avroCatExampleJson = "{\"id\":10,\"name\":\"Tom\",\"breed\":\"SPHYNX\"}";
        GenericRecord avroCatExample = aCatExample(10, "Tom", Breed.SPHYNX);

        // Mocks
        Deserializer<Object> aMockedAvroDeserializer = Mockito.mock(KafkaAvroDeserializer.class);
        Mockito.when(aMockedAvroDeserializer.deserialize(Mockito.any(), Mockito.any())).thenReturn(avroCatExample);

        // GIVEN a decorated record with an avro serialized key and a schema id
        byte[] keyBytes = avroCatExampleJson.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = "".getBytes(StandardCharsets.UTF_8); // value property does not matter

        ConsumerRecord<byte[], byte[]> kafkaRecord = new ConsumerRecord<>("topic", 0, 0, keyBytes, valueBytes);
        Record record = new Record(kafkaRecord, 1, null);
        record = new AvroKeySchemaRecord(record, aMockedAvroDeserializer);

        // WHEN getKey() method is called
        String keyString = record.getKey();

        // EXPECT a json result in String type
        assertThat(keyString, is(avroCatExampleJson));
    }

    @Test
    public void testKeyIsAvroSerializedFallbackWithDecorator() {

        // GIVEN a record with an avro serialized key
        // AND a schema id is given
        // WHEN getKey() method is called
        // AND kafka avro deserializer throws an exception
        // EXPECT a string representation of the key bytes as result
        Assertions.fail();
    }

    @Test
    public void testKeyIsProtobufSerializedWithDecorator() {

        // GIVEN a record with a protobuf serialized key
        // AND no schema id is given
        // AND protobufToJsonDeserializer exists
        // WHEN getKey() method is called
        // EXPECT a protobuf deserialized value in String type as result
        Assertions.fail();
    }

    @Test
    public void testKeyIsProtobufSerializedFallbackWithDecorator() {

        // GIVEN a record with a protobuf serialized key
        // AND no schema id is given
        // AND protobufToJsonDeserializer exists
        // WHEN getKey() method is called
        // AND protobuf deserializer throws an exception
        // EXPECT a string representation of the key bytes as result
        Assertions.fail();
    }

    @Test
    public void testKeyDefaultFallbackWithDecorator() {

        // GIVEN a record with a protobuf serialized key
        // AND no schema id exists
        // AND no protobufToJsonDeserializer exists
        // WHEN getKey() method is called
        // EXPECT a string representation of the key bytes as result
        Assertions.fail();
    }

    /**
     * Method returns an avro example data object with a cat schema
     */
    private GenericRecord aCatExample(int id, String name, Breed breed) {
        return new GenericRecordBuilder(Cat.SCHEMA$)
                .set("id", id)
                .set("name", name)
                .set("breed", breed)
                .build();
    }
}
