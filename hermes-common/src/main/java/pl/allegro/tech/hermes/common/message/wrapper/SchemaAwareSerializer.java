package pl.allegro.tech.hermes.common.message.wrapper;

import pl.allegro.analytics.commons.avrocodecs.AvroCodecException;
import pl.allegro.analytics.commons.avrocodecs.internal.AvroSerDe;
import pl.allegro.tech.hermes.domain.topic.schema.SchemaVersion;

import java.util.Arrays;

public class SchemaAwareSerializer {

    public static byte[] serialize(SchemaVersion version, byte[] data) {
        return AvroSerDe.serialize(version.value(), data);
    }

    public static DeserializedPayload deserialize(byte[] data) {
        try {
            pl.allegro.analytics.commons.avrocodecs.internal.DeserializedPayload payload = AvroSerDe.deserialize(data);
            return new DeserializedPayload(Arrays.copyOfRange(data, payload.getAvroBinaryOffset(), payload.getPayload().length),
                    SchemaVersion.valueOf(payload.getWriterSchemaVersion()));
        } catch (AvroCodecException e) {
            throw new UnwrappingException("Failed to deserialize message", e);
        }
    }

    public static class DeserializedPayload {
        private byte[] payload;
        private SchemaVersion schemaVersion;

        public DeserializedPayload(byte[] payload, SchemaVersion schemaVersion) {
            this.payload = payload;
            this.schemaVersion = schemaVersion;
        }

        public byte[] getPayload() {
            return payload;
        }

        public SchemaVersion getSchemaVersion() {
            return schemaVersion;
        }
    }

}
