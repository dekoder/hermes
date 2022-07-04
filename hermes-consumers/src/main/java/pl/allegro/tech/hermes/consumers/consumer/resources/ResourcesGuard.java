package pl.allegro.tech.hermes.consumers.consumer.resources;

import org.apache.kafka.common.TopicPartition;
import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.consumers.consumer.Message;

import java.util.Map;

public interface ResourcesGuard {

    void tryAcquireFetch(Subscription subscription) throws InterruptedException;

    void markFetchWithoutMessages(Subscription subscription);

    void release(Subscription subscription);

    void recordFetchedMessage(Message message);

    void registerSubscription(Subscription subscription);

    void unregisterSubscription(Subscription subscription);

    void updateSubscription(Subscription newSubscription);

    void record(Subscription subscription, Map<TopicPartition, Long> topicPartitionLongMap);

    void record(Message message);
}
