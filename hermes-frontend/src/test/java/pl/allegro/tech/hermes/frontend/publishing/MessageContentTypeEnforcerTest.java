package pl.allegro.tech.hermes.frontend.publishing;

import org.apache.avro.Schema;
import org.junit.Test;
import pl.allegro.tech.hermes.domain.topic.schema.SchemaVersion;
import pl.allegro.tech.hermes.domain.topic.schema.CompiledSchema;
import pl.allegro.tech.hermes.frontend.publishing.avro.AvroMessage;
import pl.allegro.tech.hermes.frontend.publishing.message.Message;
import pl.allegro.tech.hermes.test.helper.avro.AvroUser;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class MessageContentTypeEnforcerTest {

    private MessageContentTypeEnforcer enforcer = new MessageContentTypeEnforcer();

    private AvroUser avroMessage = new AvroUser("Bob", 30, "black");
    private CompiledSchema<Schema> schema = new CompiledSchema<>(avroMessage.getSchema(), SchemaVersion.valueOf(0));

    @Test
    public void shouldConvertToAvroWhenReceivedJSONOnAvroTopic() throws IOException {
        // given
        AvroMessage message = new AvroMessage("1", avroMessage.asJson().getBytes(), 1234, schema);

        // when
        Message enforcedMessage = enforcer.enforceAvro("application/json", message);

        // then
        assertThat(enforcedMessage.getData()).isEqualTo(avroMessage.asBytes());
    }

    @Test
    public void shouldStringContentTypeOfAdditionalOptionsWhenInterpretingIt() throws IOException {
        // given
        AvroMessage message = new AvroMessage("1", avroMessage.asJson().getBytes(), 1234, schema);

        // when
        Message enforcedMessage = enforcer.enforceAvro("application/json;encoding=utf-8", message);

        // then
        assertThat(enforcedMessage.getData()).isEqualTo(avroMessage.asBytes());
    }

    @Test
    public void shouldNotConvertWhenReceivingAvroOnAvroTopic() throws IOException {
        // given
        AvroMessage message = new AvroMessage("1", avroMessage.asBytes(), 1234, schema);

        // when
        Message enforcedMessage = enforcer.enforceAvro("avro/binary", message);

        // then
        assertThat(enforcedMessage.getData()).isEqualTo(avroMessage.asBytes());
    }

}