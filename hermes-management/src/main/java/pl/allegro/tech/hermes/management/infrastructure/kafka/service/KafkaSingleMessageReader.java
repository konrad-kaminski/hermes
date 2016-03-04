package pl.allegro.tech.hermes.management.infrastructure.kafka.service;

import pl.allegro.tech.hermes.api.ContentType;
import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.common.kafka.KafkaTopic;
import pl.allegro.tech.hermes.common.message.wrapper.SchemaAwareSerializer;
import pl.allegro.tech.hermes.domain.topic.schema.SchemaRepository;
import pl.allegro.tech.hermes.management.domain.topic.SingleMessageReader;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

import java.nio.charset.Charset;

public class KafkaSingleMessageReader implements SingleMessageReader {
    private final KafkaRawMessageReader kafkaRawMessageReader;
    private final SchemaRepository schemaRepository;
    private final JsonAvroConverter converter;

    public KafkaSingleMessageReader(KafkaRawMessageReader kafkaRawMessageReader,
                                    SchemaRepository schemaRepository,
                                    JsonAvroConverter converter) {
        this.kafkaRawMessageReader = kafkaRawMessageReader;
        this.schemaRepository = schemaRepository;
        this.converter = converter;
    }

    @Override
    public String readMessageAsJson(Topic topic, KafkaTopic kafkaTopic, int partition, long offset) {
        byte[] bytes = kafkaRawMessageReader.readMessage(kafkaTopic, partition, offset);
        if (topic.getContentType() == ContentType.AVRO) {
            bytes = convertAvroToJson(topic, bytes);
        }
        return new String(bytes, Charset.forName("UTF-8"));
    }

    private byte[] convertAvroToJson(Topic topic, byte[] bytes) {
        if (topic.isSchemaVersionAwareSerializationEnabled()) {
            SchemaAwareSerializer.DeserializedPayload payload = SchemaAwareSerializer.deserialize(bytes);
            return converter.convertToJson(payload.getPayload(), schemaRepository.getAvroSchema(topic, payload.getSchemaVersion()).getSchema());
        }

        return converter.convertToJson(bytes, schemaRepository.getAvroSchema(topic).getSchema());
    }

}
