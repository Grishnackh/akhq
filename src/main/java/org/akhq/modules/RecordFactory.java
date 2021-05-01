package org.akhq.modules;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.akhq.configs.SchemaRegistryType;
import org.akhq.models.Record;
import org.akhq.models.decorators.*;
import org.akhq.repositories.CustomDeserializerRepository;
import org.akhq.repositories.RecordRepository;
import org.akhq.repositories.SchemaRegistryRepository;
import org.akhq.utils.ProtobufToJsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;

import javax.inject.Singleton;
import java.util.Optional;

@Singleton
public class RecordFactory {

    private final KafkaModule kafkaModule;
    private final CustomDeserializerRepository customDeserializerRepository;
    private final SchemaRegistryRepository schemaRegistryRepository;
    private final AvroContentTypeParser avroContentTypeParser;

    public RecordFactory(KafkaModule kafkaModule,
                         CustomDeserializerRepository customDeserializerRepository,
                         SchemaRegistryRepository schemaRegistryRepository,
                         AvroContentTypeParser avroContentTypeParser) {
        this.kafkaModule = kafkaModule;
        this.customDeserializerRepository = customDeserializerRepository;
        this.schemaRegistryRepository = schemaRegistryRepository;
        this.avroContentTypeParser = avroContentTypeParser;
    }

    public Record newRecord(ConsumerRecord<byte[], byte[]> record, String clusterId) {
        SchemaRegistryType schemaRegistryType = this.schemaRegistryRepository.getSchemaRegistryType(clusterId);
        SchemaRegistryClient registryClient = kafkaModule.getRegistryClient(clusterId);
        Integer keySchemaId = schemaRegistryRepository.determineAvroSchemaForPayload(schemaRegistryType, record.key());
        Integer valueSchemaId = schemaRegistryRepository.determineAvroSchemaForPayload(schemaRegistryType, record.value());

        // base record (default: string)
        Record akhqRecord = new Record(record, keySchemaId, valueSchemaId);

        akhqRecord = handleAvroWireFormat(record, akhqRecord, schemaRegistryType, registryClient);
        akhqRecord = deserialize(akhqRecord, clusterId, keySchemaId, valueSchemaId);
        akhqRecord = obfuscate(akhqRecord);

        return akhqRecord;
    }

    private Record obfuscate(Record akhqRecord) {
        Optional<ObfuscationSearchStrategy> searchStrategy = resolveSearchStrategy();
        Optional<ObfuscationReplaceStrategy> replaceStrategy = resolveReplaceStrategy();

        if (searchStrategy.isPresent() && replaceStrategy.isPresent()) {
            return new ObfuscatedRecord(akhqRecord, searchStrategy.get(), replaceStrategy.get());
        }

        return akhqRecord;
    }

    private Optional<ObfuscationSearchStrategy> resolveSearchStrategy() {
        Optional<ObfuscationSearchStrategy> searchStrategy;
        if ("config.key.regex".contains("regex")) {
            searchStrategy = Optional.of(new RegexObfuscationSearchStrategy("abc"));
        } else if ("config.key.regex".contains("jsonPath")) {
            searchStrategy = Optional.of(new JsonPathObfuscationSearchStrategy("path"));
        } else {
            searchStrategy = Optional.empty();
        }
        return searchStrategy;
    }

    private Optional<ObfuscationReplaceStrategy> resolveReplaceStrategy() {
        Optional<ObfuscationReplaceStrategy> replaceStrategy;
        if ("config.value.regex".contains("${sadf}")) {
            replaceStrategy = Optional.empty();
        } else {
            replaceStrategy = Optional.empty();
        }
        return replaceStrategy;
    }

    private Record deserialize(Record akhqRecord, String clusterId, Integer keySchemaId, Integer valueSchemaId) {
        Deserializer kafkaAvroDeserializer = this.schemaRegistryRepository.getKafkaAvroDeserializer(clusterId);
        ProtobufToJsonDeserializer protobufToJsonDeserializer = customDeserializerRepository.getProtobufToJsonDeserializer(clusterId);
        akhqRecord = deserializeKey(akhqRecord, keySchemaId, kafkaAvroDeserializer, protobufToJsonDeserializer);
        akhqRecord = deserializeValue(akhqRecord, valueSchemaId, kafkaAvroDeserializer, protobufToJsonDeserializer);
        return akhqRecord;
    }

    private Record deserializeValue(Record akhqRecord, Integer valueSchemaId, Deserializer kafkaAvroDeserializer, ProtobufToJsonDeserializer protobufToJsonDeserializer) {
        if (valueSchemaId != null) {
            akhqRecord = new AvroValueSchemaRecord(akhqRecord, kafkaAvroDeserializer);
        } else {
            if (protobufToJsonDeserializer != null) {
                var protoBufValue = new ProtoBufValueSchemaRecord(akhqRecord, protobufToJsonDeserializer);
                if (protoBufValue.getValue() != null) {
                    akhqRecord = protoBufValue;
                }
            }
        }
        return akhqRecord;
    }

    private Record deserializeKey(Record akhqRecord, Integer keySchemaId, Deserializer kafkaAvroDeserializer, ProtobufToJsonDeserializer protobufToJsonDeserializer) {
        if (keySchemaId != null) {
            akhqRecord = new AvroKeySchemaRecord(akhqRecord, kafkaAvroDeserializer);
        } else {
            if (protobufToJsonDeserializer != null) {
                var protoBufKey = new ProtoBufKeySchemaRecord(akhqRecord, protobufToJsonDeserializer);
                if (protoBufKey.getKey() != null) {
                    akhqRecord = protoBufKey;
                }
            }
        }
        return akhqRecord;
    }

    private Record handleAvroWireFormat(ConsumerRecord<byte[], byte[]> record, Record akhqRecord, SchemaRegistryType schemaRegistryType, SchemaRegistryClient registryClient) {
        Optional<AvroContentTypeMetaData> avroContentTypeMetaData = avroContentTypeParser.parseAvroContentTypeMetaData(record, schemaRegistryType);
        if (avroContentTypeMetaData.isPresent()) {
            AvroContentTypeMetaData avrometa = avroContentTypeMetaData.get();
            akhqRecord = new AvroWireFormattedRecord(akhqRecord, registryClient, avrometa, schemaRegistryType.getMagicByte());
        }
        return akhqRecord;
    }

    public Record newRecord(ConsumerRecord<byte[], byte[]> record, RecordRepository.BaseOptions options) {
        return this.newRecord(record, options.getClusterId());
    }
}