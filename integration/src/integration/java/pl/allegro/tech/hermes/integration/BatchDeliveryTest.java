package pl.allegro.tech.hermes.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.awaitility.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.integration.env.SharedServices;
import pl.allegro.tech.hermes.test.helper.endpoint.RemoteServiceEndpoint;
import pl.allegro.tech.hermes.test.helper.message.TestMessage;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static javax.ws.rs.core.Response.Status.CREATED;
import static pl.allegro.tech.hermes.integration.test.HermesAssertions.assertThat;

public class BatchDeliveryTest extends IntegrationTest {
    private RemoteServiceEndpoint remoteService;

    private ObjectMapper mapper = new ObjectMapper();

    @BeforeMethod
    public void initializeAlways() {
        this.remoteService = new RemoteServiceEndpoint(SharedServices.services().serviceMock());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldDeliverMessagesInBatch() throws IOException {
        // given
        Topic topic = operations.buildTopic("batchSizeTest", "topic");

        int batchSize = 2;
        operations.createBatchSubscription(topic, "deliverMessagesInBatch", HTTP_ENDPOINT_URL, batchSize, Integer.MAX_VALUE, 1024);

        TestMessage[] testMessages = TestMessage.simpleMessages(batchSize);

        // when
        remoteService.expectMessages(testMessages);
        Arrays.stream(testMessages).forEach(m ->
                assertThat(publisher.publish(topic.getQualifiedName(), m.body())).hasStatus(CREATED)
        );

        // then
        remoteService.waitUntilReceived(60, 1, message -> {
            List<Map<String, Object>> batch = readBatch(message);
            assertThat(batch).hasSize(batchSize);

            for (int i = 0; i < batchSize; i++) {
                assertThat(batch.get(i).get("content")).isEqualTo(testMessages[i].getContent());
                assertThat((String) batch.get(i).get("message_id")).isNotEmpty();
            }
        });
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldDeliverBatchInGivenTimePeriod() throws IOException {
        // given
        Topic topic = operations.buildTopic("batchSizeTest", "topic");

        int batchTime = 500;
        operations.createBatchSubscription(topic, "deliverBatchInGivenTimePeriod", HTTP_ENDPOINT_URL, 10, batchTime, 1024);

        TestMessage message = TestMessage.simple();

        // when
        remoteService.expectMessages(message);
        assertThat(publisher.publish(topic.getQualifiedName(), message.body())).hasStatus(CREATED);

        // then
        remoteService.waitUntilReceived(5, 1, m -> {
            List<Map<String, Object>> batch = readBatch(m);
            assertThat(batch).hasSize(1);
            assertThat(batch.get(0).get("content")).isEqualTo(message.getContent());
        });
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldDeliverBatchInVolume() throws IOException, InterruptedException {
        // given
        Topic topic = operations.buildTopic("deliverBatchInGivenVolume", "topic");

        TestMessage message = TestMessage.wrappedSingleBatchMessage("message1", "message1");

        operations.createBatchSubscription(topic, "deliverBatchInGivenVolume", HTTP_ENDPOINT_URL, 100, Integer.MAX_VALUE, message.toString().getBytes().length + 5);

        TestMessage message1 = TestMessage.of("message1", "message1");
        TestMessage message2 = TestMessage.of("message2", "message2");

        // when
        remoteService.expectMessages(message2);
        assertThat(publisher.publish(topic.getQualifiedName(), message1.body())).hasStatus(CREATED);
        Thread.sleep(1000);
        assertThat(publisher.publish(topic.getQualifiedName(), message2.body())).hasStatus(CREATED);

        // then
        remoteService.waitUntilReceived(100, 1, m -> {
            List<Map<String, Object>> batch = readBatch(m);
            assertThat(batch).hasSize(1);
            assertThat(batch.get(0).get("content")).isEqualTo(message1.getContent());
        });
    }

    private List<Map<String, Object>> readBatch(String message) {
        try {
            return mapper.readValue(message, List.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
