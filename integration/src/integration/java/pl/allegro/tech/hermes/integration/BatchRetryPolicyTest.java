package pl.allegro.tech.hermes.integration;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.client.UrlMatchingStrategy;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.http.RequestMethod;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import com.jayway.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.test.helper.message.TestMessage;
import pl.allegro.tech.hermes.test.helper.util.Ports;

import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.findAll;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.resetAllScenarios;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static java.util.Arrays.stream;
import static javax.ws.rs.core.Response.Status.CREATED;
import static pl.allegro.tech.hermes.integration.test.HermesAssertions.assertThat;

public class BatchRetryPolicyTest extends IntegrationTest {

    private static final TestMessage[] SMALL_BATCH = TestMessage.simpleMessages(1);
    private WireMockServer wireMockRule;

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
        operations.createBatchSubscription(topic, "http://localhost:" + wireMockRule.port() + "/" + topicName, 100, 1, 1, 100);

        stubFor(post(urlEqualTo("/" + topicName))
                .inScenario(topicName)
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withStatus(500))
                .willSetStateTo("healthy"));

        stubFor(post(urlEqualTo("/" + topicName))
                .inScenario(topicName)
                .whenScenarioStateIs("healthy")
                .willReturn(aResponse().withStatus(200)));

        //when
        stream(SMALL_BATCH).forEach(m -> publish(topic, m));

        resetAllScenarios();

        //then
        UrlMatchingStrategy urlMatchingStrategy = new UrlMatchingStrategy();
        urlMatchingStrategy.setUrl("/" + topicName);
        RequestPatternBuilder requestPattern = new RequestPatternBuilder(RequestMethod.POST, urlMatchingStrategy);
        Awaitility.await().until(() -> {
            List<LoggedRequest> found = findAll(requestPattern);
            assertThat(found).hasSize(2);
        });
    }

    @Test
    public void shouldNotRetryIfRequestSuccessful() throws Throwable {
        //given
        String topicName = "notRetryIfRequestSuccessful";
        Topic topic = operations.buildTopic(topicName, "topic");
        operations.createBatchSubscription(topic, "http://localhost:" + wireMockRule.port() + "/" + topicName, 100, 1, 1, 100);

        stubFor(post(urlEqualTo("/" + topicName))
                .willReturn(aResponse().withStatus(200)));

        //when
        stream(SMALL_BATCH).forEach(m -> publish(topic, m));

        resetAllScenarios();

        //then
        UrlMatchingStrategy urlMatchingStrategy = new UrlMatchingStrategy();
        urlMatchingStrategy.setUrl("/" + topicName);
        RequestPatternBuilder requestPattern = new RequestPatternBuilder(RequestMethod.POST, urlMatchingStrategy);
        Awaitility.await().until(() -> {
            List<LoggedRequest> found = findAll(requestPattern);
            assertThat(found).hasSize(1);
        });
    }

    @Test
    public void shouldRetryUntilTtlExceeded() throws Throwable {
        //given
        String topicName = "retryUntilTtlExciteed";
        Topic topic = operations.buildTopic(topicName, "topic");
        operations.createBatchSubscription(topic, "http://localhost:" + wireMockRule.port() + "/" + topicName, 100, 20, 1, 1, 100, false);

        stubFor(post(urlEqualTo("/" + topicName))
                .withRequestBody(containing("damian"))
                .willReturn(aResponse().withStatus(500)));

        stubFor(post(urlEqualTo("/" + topicName))
                .withRequestBody(containing("beata"))
                .willReturn(aResponse().withStatus(200)));

        //when
        assertThat(publisher.publish(topic.getQualifiedName(), "damian")).hasStatus(CREATED);
        assertThat(publisher.publish(topic.getQualifiedName(), "beata")).hasStatus(CREATED);

        resetAllScenarios();

        //then
        UrlMatchingStrategy urlMatchingStrategy = new UrlMatchingStrategy();
        urlMatchingStrategy.setUrl("/" + topicName);
        RequestPatternBuilder requestPattern = new RequestPatternBuilder(RequestMethod.POST, urlMatchingStrategy);
        Awaitility.await().until(() -> {
            List<LoggedRequest> found = findAll(requestPattern);
            assertThat(found.size()).isEqualTo(6);

            for (int i = 0; i < found.size() - 2; i++) {
                assertThat(found.get(i).getBodyAsString()).contains("damian");
            }

            assertThat(found.get(found.size() - 1).getBodyAsString()).contains("beata");
        });
    }

    @Test
    public void shouldRetryOnClientErrors() throws Throwable {
        //given
        String topicName = "retryOnClientErrors";
        Topic topic = operations.buildTopic(topicName, "topic");
        operations.createBatchSubscription(topic, "http://localhost:" + wireMockRule.port() + "/" + topicName, 100, 10, 1, 1, 100, true);


        stubFor(post(urlEqualTo("/" + topicName))
                .inScenario(topicName)
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withStatus(400))
                .willSetStateTo("healthy"));

        stubFor(post(urlEqualTo("/" + topicName))
                .inScenario(topicName)
                .whenScenarioStateIs("healthy")
                .willReturn(aResponse().withStatus(200)));

        //when
        stream(SMALL_BATCH).forEach(m -> publish(topic, m));

        resetAllScenarios();

        //then
        UrlMatchingStrategy urlMatchingStrategy = new UrlMatchingStrategy();
        urlMatchingStrategy.setUrl("/" + topicName);
        RequestPatternBuilder requestPattern = new RequestPatternBuilder(RequestMethod.POST, urlMatchingStrategy);
        Awaitility.await().until(() -> {
            List<LoggedRequest> found = findAll(requestPattern);
            assertThat(found).hasSize(2);
        });
    }

    @Test
    public void shouldNotRetryOnClientErrors() throws Throwable {
        //given
        String topicName = "retryOnClientErrors";
        Topic topic = operations.buildTopic(topicName, "topic");
        operations.createBatchSubscription(topic, "http://localhost:" + wireMockRule.port() + "/" + topicName, 100, 10, 1, 1, 100, false);


        stubFor(post(urlEqualTo("/" + topicName ))
                .inScenario(topicName)
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withStatus(400))
                .willSetStateTo("healthy"));

        stubFor(post(urlEqualTo("/" + topicName))
                .inScenario(topicName)
                .whenScenarioStateIs("healthy")
                .willReturn(aResponse().withStatus(200)));

        //when
        stream(SMALL_BATCH).forEach(m -> publish(topic, m));

        resetAllScenarios();

        //then
        UrlMatchingStrategy urlMatchingStrategy = new UrlMatchingStrategy();
        urlMatchingStrategy.setUrl("/" + topicName);
        RequestPatternBuilder requestPattern = new RequestPatternBuilder(RequestMethod.POST, urlMatchingStrategy);
        Awaitility.await().until(() -> {
            List<LoggedRequest> found = findAll(requestPattern);
            assertThat(found).hasSize(1);
        });
    }

    private void publish(Topic topic, TestMessage m) {
        assertThat(publisher.publish(topic.getQualifiedName(), m.body())).hasStatus(CREATED);
    }
}
