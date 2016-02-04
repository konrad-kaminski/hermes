package pl.allegro.tech.hermes.integration;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.UrlMatchingStrategy;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.test.helper.message.TestMessage;
import pl.allegro.tech.hermes.test.helper.util.Ports;

import java.util.List;
import java.util.stream.IntStream;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.findAll;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.resetAllScenarios;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static javax.ws.rs.core.Response.Status.CREATED;
import static pl.allegro.tech.hermes.integration.test.HermesAssertions.assertThat;

public class BatchRetryPolicyTest extends IntegrationTest {

    private WireMockServer wireMockRule;

    @BeforeMethod
    public void beforeMethod() {
        resetAllScenarios();
    }

    @BeforeClass
    public void beforeClass() {
        wireMockRule = new WireMockServer(Ports.nextAvailable());
        wireMockRule.start();
        WireMock.configureFor("localhost", wireMockRule.port());
    }

    @AfterClass
    public void afterClass() {
        wireMockRule.stop();
    }

    @Test
    public void shouldRetryUntilRequestSuccessful() throws Throwable {
        //given
        String topicName = "retryUntilRequestSuccessful";
        Topic topic = operations.buildTopic(topicName, "topic");
        createSingleMessageBatchSubscription(topic);

        stubFor(post(topicUrl(topicName))
                .inScenario(topicName)
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withStatus(500))
                .willSetStateTo("healthy"));

        stubFor(post(topicUrl(topicName))
                .inScenario(topicName)
                .whenScenarioStateIs("healthy")
                .willReturn(aResponse().withStatus(200)));

        //when
        publish(topic, TestMessage.simple());

        //then
        wait.until(() -> assertThat(recordedRequests(topicName)).hasSize(2));

    }

    private UrlMatchingStrategy topicUrl(String topicName) {
        return urlEqualTo("/" + topicName);
    }

    @Test
    public void shouldNotRetryIfRequestSuccessful() throws Throwable {
        //given
        String topicName = "notRetryIfRequestSuccessful";
        Topic topic = operations.buildTopic(topicName, "topic");
        createSingleMessageBatchSubscription(topic);

        stubFor(post(topicUrl(topicName)).willReturn(aResponse().withStatus(200)));

        //when
        publish(topic, TestMessage.simple());

        //then
        wait.until(() -> assertThat(recordedRequests(topicName)).hasSize(1));
    }

    @Test
    public void shouldRetryUntilTtlExceeded() throws Throwable {
        //given
        String topicName = "retryUntilTtlExciteed";
        Topic topic = operations.buildTopic(topicName, "topic");
        createSingleMessageBatchSubscription(topic, 110, 20);

        String failedRequestBody = "failed";
        String successfulRequestBody = "successful";

        stubFor(post(topicUrl(topicName))
                .withRequestBody(containing(failedRequestBody))
                .willReturn(aResponse().withStatus(500)));

        stubFor(post(topicUrl(topicName))
                .withRequestBody(containing(successfulRequestBody))
                .willReturn(aResponse().withStatus(200)));

        //when
        assertThat(publisher.publish(topic.getQualifiedName(), failedRequestBody)).hasStatus(CREATED);
        Thread.sleep(500);
        assertThat(publisher.publish(topic.getQualifiedName(), successfulRequestBody)).hasStatus(CREATED);

        //then
        wait.until(() -> {
            List<LoggedRequest> requests = recordedRequests(topicName);

            assertThat(requests.size()).isEqualTo(6);
            IntStream.range(0, 4).forEach(i -> assertThat(requests.get(i).getBodyAsString()).contains(failedRequestBody));
            assertThat(requests.get(5).getBodyAsString()).contains(successfulRequestBody);
        });
    }

    @Test
    public void shouldRetryOnClientErrors() throws Throwable {
        //given
        String topicName = "retryOnClientErrors";
        boolean retryOnClientErrors = true;
        Topic topic = operations.buildTopic(topicName, "topic");
        createSingleMessageBatchSubscription(topic, retryOnClientErrors);

        stubFor(post(topicUrl(topicName))
                .inScenario(topicName)
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withStatus(400))
                .willSetStateTo("healthy"));

        stubFor(post(topicUrl(topicName))
                .inScenario(topicName)
                .whenScenarioStateIs("healthy")
                .willReturn(aResponse().withStatus(200)));

        //when
        publish(topic, TestMessage.simple());

        //then
        wait.until(() -> assertThat(recordedRequests(topicName)).hasSize(2));
    }



    @Test
    public void shouldNotRetryOnClientErrors() throws Throwable {
        //given
        String topicName = "retryOnClientErrors";
        Topic topic = operations.buildTopic(topicName, "topic");
        operations.createBatchSubscription(topic, subscriptionEndpoint(topicName), 100, 10, 1, 1, 100, false);


        stubFor(post(topicUrl(topicName))
                .inScenario(topicName)
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withStatus(400))
                .willSetStateTo("healthy"));

        stubFor(post(topicUrl(topicName))
                .inScenario(topicName)
                .whenScenarioStateIs("healthy")
                .willReturn(aResponse().withStatus(200)));

        //when
        publish(topic, TestMessage.simple());

        //then
        wait.until(() -> assertThat(recordedRequests(topicName)).hasSize(1));
    }

    private List<LoggedRequest> recordedRequests(String topicName) {
        return findAll(postRequestedFor(urlMatching("/" + topicName)));
    }

    private void publish(Topic topic, TestMessage m) {
        assertThat(publisher.publish(topic.getQualifiedName(), m.body())).hasStatus(CREATED);
    }

    private void createSingleMessageBatchSubscription(Topic topic) {
        operations.createBatchSubscription(topic, subscriptionEndpoint(topic.getName().getName()), 100, 10, 1, 1, 100, false);
    }

    private void createSingleMessageBatchSubscription(Topic topic, int messageTtl, int messageBackoff) {
        operations.createBatchSubscription(topic, subscriptionEndpoint(topic.getName().getName()), messageTtl, messageBackoff, 1, 1, 100, false);
    }

    private void createSingleMessageBatchSubscription(Topic topic, boolean retryOnClientErrors) {
        operations.createBatchSubscription(topic, subscriptionEndpoint(topic.getName().getName()), 100, 10, 1, 1, 100, retryOnClientErrors);
    }

    private String subscriptionEndpoint(String topicName) {
        return "http://localhost:" + wireMockRule.port() + "/" + topicName;
    }

}
