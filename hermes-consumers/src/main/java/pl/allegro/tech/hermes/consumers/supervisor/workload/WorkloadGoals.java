package pl.allegro.tech.hermes.consumers.supervisor.workload;

import pl.allegro.tech.hermes.api.Constraints;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.api.TopicName;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;

public class WorkloadGoals {

    private final Map<SubscriptionName, Constraints> subscriptionConstraints;
    private final Map<TopicName, Constraints> topicConstraints;
    private final int consumersPerSubscription;
    private final int maxSubscriptionsPerConsumer;
    private final Set<SubscriptionName> activeSubscriptions;
    private final Set<String> activeConsumers;

    private WorkloadGoals(Map<SubscriptionName, Constraints> subscriptionConstraints,
                          Map<TopicName, Constraints> topicConstraints,
                          int consumersPerSubscription,
                          int maxSubscriptionsPerConsumer,
                          Set<SubscriptionName> activeSubscriptions,
                          Set<String> activeConsumers) {
        this.subscriptionConstraints = subscriptionConstraints != null ? subscriptionConstraints : emptyMap();
        this.topicConstraints = topicConstraints != null ? topicConstraints : emptyMap();
        this.consumersPerSubscription = consumersPerSubscription;
        this.maxSubscriptionsPerConsumer = maxSubscriptionsPerConsumer;
        this.activeSubscriptions = activeSubscriptions;
        this.activeConsumers = activeConsumers;
    }

    public static Builder builder() {
        return new Builder();
    }

    public int getConsumerCount(SubscriptionName subscriptionName) {
        Constraints requiredConsumers = subscriptionConstraints.get(subscriptionName);
        if (requiredConsumers == null) {
            requiredConsumers = topicConstraints.get(subscriptionName.getTopicName());
        }
        if (requiredConsumers != null && requiredConsumers.getConsumersNumber() > 0) {
            return Math.min(requiredConsumers.getConsumersNumber(), activeConsumers.size());
        }
        return consumersPerSubscription;
    }

    public int getMaxSubscriptionsPerConsumer() {
        return maxSubscriptionsPerConsumer;
    }

    public Set<String> getTargetSetOfConsumers() {
        return activeConsumers;
    }

    public Set<SubscriptionName> getTargetSetOfSubscriptions() {
        return activeSubscriptions;
    }

    public int countMissingResources(SubscriptionAssignmentView state) {
        return activeSubscriptions.stream()
                .mapToInt(s -> {
                    int requiredConsumers = getConsumerCount(s);
                    int subscriptionAssignments = state.getAssignmentsCountForSubscription(s);
                    int missing = requiredConsumers - subscriptionAssignments;
                    if (missing != 0) {
                        // logger.info("Subscription {} has {} != {} (default) assignments", s, subscriptionAssignments, requiredConsumers);
                    }
                    return missing;
                })
                .sum();
    }

    public static class Builder {
        private Map<SubscriptionName, Constraints> subscriptionConstraints = new HashMap<>();
        private Map<TopicName, Constraints> topicConstraints = new HashMap<>();
        private int consumersPerSubscription;
        private int maxSubscriptionsPerConsumer;
        private Set<SubscriptionName> activeSubscriptions = new HashSet<>();
        private Set<String> activeConsumers = new HashSet<>();

        public Builder withWorkloadGoals(WorkloadGoals workloadGoals) {
            subscriptionConstraints = workloadGoals.subscriptionConstraints;
            topicConstraints = workloadGoals.topicConstraints;
            consumersPerSubscription = workloadGoals.consumersPerSubscription;
            maxSubscriptionsPerConsumer = workloadGoals.maxSubscriptionsPerConsumer;
            activeSubscriptions = workloadGoals.activeSubscriptions;
            activeConsumers = workloadGoals.activeConsumers;
            return this;
        }

        public Builder withSubscriptionConstraints(Map<SubscriptionName, Constraints> subscriptionConstraints) {
            this.subscriptionConstraints = subscriptionConstraints;
            return this;
        }

        public Builder withTopicConstraints(Map<TopicName, Constraints> topicConstraints) {
            this.topicConstraints = topicConstraints;
            return this;
        }

        public Builder withActiveSubscriptions(Set<SubscriptionName> activeSubscriptions) {
            this.activeSubscriptions = activeSubscriptions;
            return this;
        }

        public Builder withActiveConsumers(Set<String> activeConsumers) {
            this.activeConsumers = activeConsumers;
            return this;
        }

        public Builder withConsumersPerSubscription(int consumersPerSubscription) {
            this.consumersPerSubscription = consumersPerSubscription;
            return this;
        }

        public Builder withMaxSubscriptionsPerConsumer(int maxSubscriptionsPerConsumer) {
            this.maxSubscriptionsPerConsumer = maxSubscriptionsPerConsumer;
            return this;
        }

        public WorkloadGoals build() {
            return new WorkloadGoals(
                    subscriptionConstraints,
                    topicConstraints,
                    consumersPerSubscription,
                    maxSubscriptionsPerConsumer,
                    activeSubscriptions,
                    activeConsumers
            );
        }
    }
}
