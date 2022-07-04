package pl.allegro.tech.hermes.consumers.consumer.offset;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import pl.allegro.tech.hermes.api.SubscriptionName;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.stream.Collectors.toSet;
import static org.slf4j.LoggerFactory.getLogger;

public class ConsumerPartitionAssignmentState {

    private static final Logger logger = getLogger(ConsumerPartitionAssignmentState.class);

    private final Map<SubscriptionName, Set<TopicPartition>> assigned = new ConcurrentHashMap<>();

    private final Map<SubscriptionName, Long> terms = new ConcurrentHashMap<>();

    public void assign(SubscriptionName name, Collection<TopicPartition> partitions) {
        incrementTerm(name);
        logger.info("Assigning partitions {} of {}, term={}", partitions, name, currentTerm(name));
        assigned.compute(name, ((subscriptionName, assigned) -> {
            HashSet<TopicPartition> extended = new HashSet<>(partitions);
            if (assigned == null) {
                return extended;
            } else {
                extended.addAll(assigned);
                return extended;
            }
        }));
    }

    private void incrementTerm(SubscriptionName name) {
        terms.compute(name, ((subscriptionName, term) -> term == null ? 0L : term + 1L));
    }

    public void revoke(SubscriptionName name, Collection<TopicPartition> partitions) {
        logger.info("Revoking partitions {} of {}", partitions, name);
        assigned.computeIfPresent(name, (subscriptionName, assigned) -> {
            Set<TopicPartition> filtered = assigned.stream().filter(p -> !partitions.contains(p)).collect(toSet());
            return filtered.isEmpty() ? null : filtered;
        });
    }

    public void revokeAll(SubscriptionName name) {
        logger.info("Revoking all partitions of {}", name);
        assigned.remove(name);
    }

    public long currentTerm(SubscriptionName name) {
        return terms.getOrDefault(name, -1L);
    }

    public boolean isAssignedPartitionAtCurrentTerm(SubscriptionPartition subscriptionPartition) {
        return currentTerm(subscriptionPartition.getSubscriptionName()) == subscriptionPartition.getPartitionAssignmentTerm()
                && isAssigned(subscriptionPartition);
    }

    private boolean isAssigned(SubscriptionPartition subscriptionPartition) {
        return assigned.containsKey(subscriptionPartition.getSubscriptionName())
                && assigned.get(subscriptionPartition.getSubscriptionName()).contains(
                        new TopicPartition(subscriptionPartition.getKafkaTopicName().asString(), subscriptionPartition.getPartition())
        );
    }

    public Set<TopicPartition> getPartitions(SubscriptionName qualifiedName) {
        Set<TopicPartition> partitions = assigned.get(qualifiedName);
        if (partitions == null) {
            return Collections.emptySet();
        }
        return partitions;
    }
}
