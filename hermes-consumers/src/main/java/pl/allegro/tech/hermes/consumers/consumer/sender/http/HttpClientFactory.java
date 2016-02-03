package pl.allegro.tech.hermes.consumers.consumer.sender.http;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.HttpCookieStore;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.glassfish.hk2.api.Factory;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.metric.executor.InstrumentedExecutorServiceFactory;

import javax.inject.Inject;
import java.security.KeyStore;
import java.util.concurrent.ExecutorService;

import static pl.allegro.tech.hermes.common.config.Configs.CONSUMER_HTTP_CLIENT_MAX_CONNECTIONS_PER_DESTINATION;
import static pl.allegro.tech.hermes.common.config.Configs.CONSUMER_HTTP_CLIENT_THREAD_POOL_MONITORING;
import static pl.allegro.tech.hermes.common.config.Configs.CONSUMER_HTTP_CLIENT_THREAD_POOL_SIZE;
import static pl.allegro.tech.hermes.common.config.Configs.CONSUMER_INFLIGHT_SIZE;

public class HttpClientFactory implements Factory<HttpClient> {

    private final ConfigFactory configFactory;
    private final InstrumentedExecutorServiceFactory executorFactory;

    @Inject
    public HttpClientFactory(ConfigFactory configFactory, InstrumentedExecutorServiceFactory executorFactory) {
        this.configFactory = configFactory;
        this.executorFactory = executorFactory;
    }

    @Override
    public HttpClient provide() {
        HttpClient client = new HttpClient(sslContextFactory());
        client.setMaxConnectionsPerDestination(configFactory.getIntProperty(CONSUMER_HTTP_CLIENT_MAX_CONNECTIONS_PER_DESTINATION));
        client.setMaxRequestsQueuedPerDestination(configFactory.getIntProperty(CONSUMER_INFLIGHT_SIZE));
        client.setExecutor(getExecutor());
        client.setCookieStore(new HttpCookieStore.Empty());
        return client;
    }

    private ExecutorService getExecutor() {
        return executorFactory.getExecutorService("jetty-http-client", configFactory.getIntProperty(CONSUMER_HTTP_CLIENT_THREAD_POOL_SIZE),
                                                  configFactory.getBooleanProperty(CONSUMER_HTTP_CLIENT_THREAD_POOL_MONITORING)
        );
    }

    @Override
    public void dispose(HttpClient instance) {
    }

    private SslContextFactory sslContextFactory() {
        SslContextFactory sslContextFactory = new SslContextFactory();

        //TODO initialize and provide default trust store instance?
        sslContextFactory.setTrustStorePath(System.getProperty("javax.net.ssl.trustStore", System.getProperty("java.home") + "/lib/security/cacerts"));
        sslContextFactory.setTrustStoreType(System.getProperty("javax.net.ssl.trustStoreType", KeyStore.getDefaultType()));

        sslContextFactory.setValidatePeerCerts(true);

        return sslContextFactory;
    }
}
