package pl.allegro.tech.hermes.consumers.consumer.resources;

import org.apache.kafka.common.TopicPartition;
import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.consumers.consumer.Message;

import java.util.Map;

public class NoOpResourcesGuard implements ResourcesGuard {

    @Override
    public void tryAcquireFetch(Subscription subscription) {
    }

    @Override
    public void markFetchWithoutMessages(Subscription subscription) {

    }

    @Override
    public void release(Subscription subscription) {

    }

    @Override
    public void recordFetchedMessage(Message message) {

    }

    @Override
    public void registerSubscription(Subscription subscription) {

    }

    @Override
    public void unregisterSubscription(Subscription subscription) {

    }

    @Override
    public void updateSubscription(Subscription newSubscription) {

    }

    @Override
    public void record(Subscription subscription, Map<TopicPartition, Long> committedOffsets, Map<TopicPartition, Long> endOffsets) {

    }

    @Override
    public void record(Message message) {

    }
}
