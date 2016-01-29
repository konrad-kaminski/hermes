package pl.allegro.tech.hermes.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.integration.env.SharedServices;
import pl.allegro.tech.hermes.test.helper.endpoint.RemoteServiceEndpoint;
import pl.allegro.tech.hermes.test.helper.message.TestMessage;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.stream;
import static javax.ws.rs.core.Response.Status.CREATED;
import static pl.allegro.tech.hermes.integration.test.HermesAssertions.assertThat;

public class BatchDeliveryTest extends IntegrationTest {
    private static final int LARGE_MESSAGE_TTL = 100 * 1000;
    private RemoteServiceEndpoint remoteService;

    private ObjectMapper mapper = new ObjectMapper();

    @BeforeMethod
    public void initializeAlways() {
        this.remoteService = new RemoteServiceEndpoint(SharedServices.services().serviceMock());
    }

    private static final int LARGE_BATCH_TIME = Integer.MAX_VALUE;
    private static final int LARGE_BATCH_SIZE = 100;
    private static final int LARGE_BATCH_VOLUME = 1024;

    private static final int SMALL_BATCH_TIME = 1;
    private static final int SMALL_BATCH_SIZE = 2;
    private static final TestMessage[] SMALL_BATCH = TestMessage.simpleMessages(SMALL_BATCH_SIZE);

    private static final TestMessage SINGLE_MESSAGE = TestMessage.simple();

    @Test
    public void shouldDeliverMessagesInBatch() throws IOException {
        // given
        Topic topic = operations.buildTopic("batchSizeTest", "topic");
        operations.createBatchSubscription(topic, HTTP_ENDPOINT_URL, LARGE_MESSAGE_TTL, SMALL_BATCH_SIZE, LARGE_BATCH_TIME, LARGE_BATCH_VOLUME);
        remoteService.expectMessages(SMALL_BATCH);

        // when
        stream(SMALL_BATCH).forEach(m -> publish(topic, m));

        // then
        expectSingleBatch(SMALL_BATCH);
    }

    @Test
    public void shouldDeliverBatchInGivenTimePeriod() throws IOException {
        // given
        Topic topic = operations.buildTopic("deliverBatchInGivenTimePeriod", "topic");
        operations.createBatchSubscription(topic, HTTP_ENDPOINT_URL, LARGE_MESSAGE_TTL, LARGE_BATCH_SIZE, SMALL_BATCH_TIME, LARGE_BATCH_VOLUME);
        remoteService.expectMessages(SINGLE_MESSAGE);

        // when
        publish(topic, SINGLE_MESSAGE);

        // then
        expectSingleBatch(SINGLE_MESSAGE);
    }

    @Test
    public void shouldDeliverBatchInGivenVolume() throws IOException, InterruptedException {
        // given
        Topic topic = operations.buildTopic("deliverBatchInGivenVolume", "topic");
        int batchVolumeThatFitsOneMessageOnly = SINGLE_MESSAGE.wrap().toString().getBytes().length + 5;
        operations.createBatchSubscription(topic, HTTP_ENDPOINT_URL, LARGE_MESSAGE_TTL, LARGE_BATCH_SIZE, LARGE_BATCH_TIME, batchVolumeThatFitsOneMessageOnly);
        remoteService.expectMessages(SINGLE_MESSAGE);

        // when publishing more than buffer capacity
        publish(topic, SINGLE_MESSAGE);
        publish(topic, SINGLE_MESSAGE);

        // then we expect to receive batch that has desired batch volume (one message only)
        expectSingleBatch(SINGLE_MESSAGE);
    }

    private void publish(Topic topic, TestMessage m) {
        assertThat(publisher.publish(topic.getQualifiedName(), m.body())).hasStatus(CREATED);
    }

    private void expectSingleBatch(TestMessage... expectedContents) {
        remoteService.waitUntilReceived(1000, 1, message -> {
            List<Map<String, Object>> batch = readBatch(message);
            assertThat(batch).hasSize(expectedContents.length);
            for (int i = 0; i < expectedContents.length; i++) {
                assertThat(batch.get(i).get("content")).isEqualTo(expectedContents[i].getContent());
                assertThat((String) batch.get(i).get("message_id")).isNotEmpty();
            }
        });
    }
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> readBatch(String message) {
        try {
            return mapper.readValue(message, List.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
