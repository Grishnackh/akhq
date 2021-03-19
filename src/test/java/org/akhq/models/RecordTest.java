package org.akhq.models;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.akhq.Breed;
import org.akhq.Cat;
import org.akhq.models.decorators.AvroKeySchemaRecord;
import org.akhq.utils.Album;
import org.akhq.utils.AlbumProto;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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

        // Testdata
        String avroCatExampleJson = "{\"id\":10,\"name\":\"Tom\",\"breed\":\"SPHYNX\"}";

        // GIVEN a record with avro serialized key
        byte[] keyBytes = avroCatExampleJson.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = "".getBytes(StandardCharsets.UTF_8); // value property does not matter

        ConsumerRecord<byte[], byte[]> kafkaRecord = new ConsumerRecord<>("topic", 0, 0, keyBytes, valueBytes);
        Record record = new Record(kafkaRecord, 1, null);

        // WHEN getKey() method is called
        String key = record.getKey();

        // EXPECT a string representation of the key bytes
        assertThat(key, is(avroCatExampleJson));
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
    public void testKeyIsProtobufSerialized() {

        // GIVEN a record with a serialized key
        byte[] keyBytes = anAlbumExample().toByteArray();
        byte[] valueBytes = "".getBytes(StandardCharsets.UTF_8); // value property does not matter

        ConsumerRecord<byte[], byte[]> kafkaRecord = new ConsumerRecord<>("topic", 0, 0, keyBytes, valueBytes);
        Record record = new Record(kafkaRecord, 1, null);

        // WHEN getKey() method is called
        String key = record.getKey();

        // EXPECT a string representation of the key bytes
        assertThat(key, is(new String(anAlbumExample().toByteArray())));
    }

    @Test
    public void testValueByteArrayIsNull() {

        // GIVEN a record with null value
        byte[] keyBytes = "".getBytes(StandardCharsets.UTF_8); // key property does not matter
        byte[] valueBytes = null;
        ConsumerRecord<byte[], byte[]> kafkaRecord = new ConsumerRecord<>("topic", 0, 0, keyBytes, valueBytes);
        Record record = new Record(kafkaRecord, null, null);

        // WHEN getValue() is called
        String value = record.getValue();

        // EXPECT NULL result
        Assertions.assertNull(value);
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

    /**
     * Method returns a protobuf example data object with an album schema
     */
    private AlbumProto.Album anAlbumExample() {
        List<String> artists = Collections.singletonList("Imagine Dragons");
        List<String> songTitles = Arrays.asList("Birds", "Zero", "Natural", "Machine");
        Album album = new Album("Origins", artists, 2018, songTitles);
        return AlbumProto.Album.newBuilder()
                .setTitle(album.getTitle())
                .addAllArtist(album.getArtists())
                .setReleaseYear(album.getReleaseYear())
                .addAllSongTitle(album.getSongsTitles())
                .build();
    }
}
