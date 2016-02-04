package pl.allegro.tech.hermes.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import pl.allegro.tech.hermes.api.helpers.Patch;

import javax.validation.constraints.Min;
import java.util.Objects;

public class SubscriptionPolicy {
    private static final Integer DEFAULT_MESSAGE_BACKOFF = 100;
    private static final Integer DEFAULT_REQUEST_TIMEOUT = 1000;

    @Min(1)
    private Integer rate;

    @Min(0)
    private Integer messageTtl;

    @Min(0)
    private Integer messageBackoff;

    private boolean retryClientErrors = false;

    private DeliveryType deliveryType;

    // TODO: think later whether to introduce SubscriptionBatchPolicy
    private Integer batchSize;
    private Integer batchTime;
    private Integer batchVolume;
    private int requestTimeout;

    private SubscriptionPolicy() { }

    @JsonCreator
    public SubscriptionPolicy(@JsonProperty("rate") int rate, @JsonProperty("messageTtl") int messageTtl,
                              @JsonProperty("retryClientErrors") boolean retryClientErrors,
                              @JsonProperty("messageBackoff") Integer messageBackoff,
                              @JsonProperty("requestTimeout") Integer requestTimeout,
                              @JsonProperty("deliveryType") DeliveryType deliveryType,
                              @JsonProperty("batchSize") Integer batchSize,
                              @JsonProperty("batchTime") Integer batchTime,
                              @JsonProperty("batchVolume") Integer batchVolume) {
        this.rate = rate;
        this.messageTtl = messageTtl;
        this.retryClientErrors = retryClientErrors;
        this.messageBackoff = messageBackoff != null ? messageBackoff : DEFAULT_MESSAGE_BACKOFF;
        this.requestTimeout = requestTimeout != null ? requestTimeout : DEFAULT_REQUEST_TIMEOUT;
        this.deliveryType = deliveryType != null ? deliveryType : DeliveryType.SERIAL;
        this.batchSize = batchSize;
        this.batchTime = batchTime;
        this.batchVolume = batchVolume;
    }

    @Override
    public int hashCode() {
        return Objects.hash(rate, messageTtl);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final SubscriptionPolicy other = (SubscriptionPolicy) obj;
        return Objects.equals(this.rate, other.rate)
                && Objects.equals(this.messageTtl, other.messageTtl)
                && Objects.equals(this.messageBackoff, other.messageBackoff)
                && Objects.equals(this.requestTimeout, other.requestTimeout)
                && Objects.equals(this.retryClientErrors, other.retryClientErrors)
                && Objects.equals(this.deliveryType, other.deliveryType)
                && Objects.equals(this.batchSize, other.batchSize)
                && Objects.equals(this.batchTime, other.batchTime)
                && Objects.equals(this.batchVolume, other.batchVolume);
    }

    @Override
    public String toString() {
        return com.google.common.base.Objects.toStringHelper(this)
                .add("rate", rate)
                .add("messageTtl", messageTtl)
                .add("messageBackoff", messageBackoff)
                .add("requestTimeout", requestTimeout)
                .add("retryClientErrors", retryClientErrors)
                .add("deliveryType", deliveryType)
                .add("batchSize", batchSize)
                .add("batchTime", batchTime)
                .add("batchVolume", batchVolume)
                .toString();
    }

    //<editor-fold desc="Getters/Setters">
    public Integer getRate() {
        return rate;
    }

    public void setRate(int rate) {
        this.rate = rate;
    }

    public Integer getMessageTtl() {
        return messageTtl;
    }

    public boolean isRetryClientErrors() {
        return retryClientErrors;
    }

    public Integer getMessageBackoff() {
        return messageBackoff;
    }
    //</editor-fold>


    public DeliveryType getDeliveryType() {
        return deliveryType;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public Integer getBatchTime() {
        return batchTime;
    }

    public Integer getBatchVolume() {
        return batchVolume;
    }

    public int getRequestTimeout() {
        return requestTimeout;
    }

    public static class Builder {
        private static final Integer DEFAULT_RATE = 400;
        private static final Integer DEFAULT_MESSAGE_TTL = 3600;

        private SubscriptionPolicy subscriptionPolicy;

        public Builder() {
            subscriptionPolicy = new SubscriptionPolicy();
        }

        public Builder applyDefaults() {
            subscriptionPolicy.rate = DEFAULT_RATE;
            subscriptionPolicy.messageTtl = DEFAULT_MESSAGE_TTL;
            subscriptionPolicy.messageBackoff = DEFAULT_MESSAGE_BACKOFF;
            subscriptionPolicy.requestTimeout = DEFAULT_REQUEST_TIMEOUT;
            subscriptionPolicy.deliveryType = DeliveryType.SERIAL;
            return this;
        }

        public Builder withRate(int rate) {
            subscriptionPolicy.rate = rate;
            return this;
        }

        public Builder withMessageTtl(int ttl) {
            subscriptionPolicy.messageTtl = ttl;
            return this;
        }

        public Builder withMessageBackoff(int backoff) {
            subscriptionPolicy.messageBackoff = backoff;
            return this;
        }

        public Builder withClientErrorRetry() {
            return withClientErrorRetry(true);
        }

        public Builder withClientErrorRetry(boolean retry) {
            subscriptionPolicy.retryClientErrors = retry;
            return this;
        }

        public Builder withDeliveryType(DeliveryType type) {
            subscriptionPolicy.deliveryType = type;
            return this;
        }

        public Builder withBatchSize(int size) {
            subscriptionPolicy.batchSize = size;
            return this;
        }

        public Builder withBatchTime(int time) {
            subscriptionPolicy.batchTime = time;
            return this;
        }

        public Builder withBatchVolume(int volume) {
            subscriptionPolicy.batchVolume = volume;
            return this;
        }

        public Builder withRequestTimeout(int requestTimeout) {
            subscriptionPolicy.requestTimeout = requestTimeout;
            return this;
        }

        public static Builder subscriptionPolicy() {
            return new Builder();
        }

        public SubscriptionPolicy build() {
            return subscriptionPolicy;
        }

        public Builder applyPatch(SubscriptionPolicy update) {
            if (update != null) {
                subscriptionPolicy = Patch.apply(subscriptionPolicy, update);
            }
            return this;
        }
    }
}
