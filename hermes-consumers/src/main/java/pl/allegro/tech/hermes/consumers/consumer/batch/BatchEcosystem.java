package pl.allegro.tech.hermes.consumers.consumer.batch;

import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.common.metric.Counters;
import pl.allegro.tech.hermes.common.metric.HermesMetrics;
import pl.allegro.tech.hermes.common.metric.Meters;
import pl.allegro.tech.hermes.consumers.consumer.sender.MessageSendingResult;
import pl.allegro.tech.hermes.tracker.consumers.Trackers;

import static pl.allegro.tech.hermes.common.metric.Meters.METER;
import static pl.allegro.tech.hermes.common.metric.Meters.SUBSCRIPTION_METER;
import static pl.allegro.tech.hermes.common.metric.Meters.TOPIC_METER;

public class BatchEcosystem {
    private HermesMetrics metrics;
    private Trackers trackers;

    public BatchEcosystem(HermesMetrics metrics, Trackers trackers) {
        this.metrics = metrics;
        this.trackers = trackers;
    }

    public void closeInflightMetrics(MessageBatch batch, Subscription subscription) {
        metrics.decrementInflightCounter(subscription, batch.size());
        metrics.inflightTimeHistogram(subscription).update(batch.getLifetime());
    }

    public void markSendingResult(MessageSendingResult result, Subscription subscription, MessageBatch batch) {
        if (result.succeeded()) {
            markDelivered(batch, subscription);
        } else {
            markDiscarded(batch, subscription, "Retry policy exhausted with status code " + result.getStatusCode());
        }
    }

    private void markDelivered(MessageBatch batch, Subscription subscription) {
        metrics.meter(METER).mark(batch.size());
        metrics.meter(TOPIC_METER, subscription.getTopicName()).mark(batch.size());
        metrics.meter(SUBSCRIPTION_METER, subscription.getTopicName(), subscription.getName()).mark(batch.size());
        metrics.counter(Counters.DELIVERED, subscription.getTopicName(), subscription.getName()).inc(batch.size());
        batch.getMessagesMetadata().forEach(m -> trackers.get(subscription).logSent(m));
    }

    public void markDiscarded(MessageBatch batch, Subscription subscription, String reason) {
        metrics.counter(Counters.DISCARDED, subscription.getTopicName(), subscription.getName()).inc(batch.size());
        metrics.meter(Meters.DISCARDED_METER).mark(batch.size());
        metrics.meter(Meters.DISCARDED_TOPIC_METER, subscription.getTopicName()).mark(batch.size());
        metrics.meter(Meters.DISCARDED_SUBSCRIPTION_METER, subscription.getTopicName(), subscription.getName()).mark(batch.size());
        metrics.decrementInflightCounter(subscription);
        batch.getMessagesMetadata().forEach(m -> trackers.get(subscription).logDiscarded(m, reason));
    }
}
