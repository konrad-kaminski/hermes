package pl.allegro.tech.hermes.frontend.publishing;

import org.apache.avro.Schema;
import pl.allegro.tech.hermes.frontend.publishing.avro.AvroMessage;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

public class MessageContentTypeEnforcer {

    private final JsonAvroConverter converter = new JsonAvroConverter();

    private static final String APPLICATION_JSON_WITH_DELIM = APPLICATION_JSON + ";";

    public AvroMessage enforceAvro(String payloadContentType, AvroMessage message) {
        if (isJSON(payloadContentType)) {
            byte[] converted = converter.convertToAvro(message.getData(), message.<Schema>getSchema().get().getSchema());
            return message.withDataReplaced(converted);
        }
        return message;
    }

    private boolean isJSON(String contentType) {
        return contentType != null && (contentType.length() > APPLICATION_JSON.length() ?
                contentType.startsWith(APPLICATION_JSON_WITH_DELIM) : contentType.equals(APPLICATION_JSON));
    }
}
