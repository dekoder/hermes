package pl.allegro.tech.hermes.consumers.supervisor.workload;

import pl.allegro.tech.hermes.api.SubscriptionName;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Comparator.comparingInt;
import static java.util.stream.Collectors.toList;

public class SelectiveWorkBalancer implements WorkBalancer {

    @Override
    public SubscriptionAssignmentView balance(SubscriptionAssignmentView currentState, WorkloadGoals workloadGoals) {
        Set<String> activeConsumers = workloadGoals.getTargetSetOfConsumers();
        Set<SubscriptionName> activeSubscriptions = workloadGoals.getTargetSetOfSubscriptions();
        return currentState.transform((state, transformer) -> {
            findRemovedSubscriptions(currentState, activeSubscriptions).forEach(transformer::removeSubscription);
            findInactiveConsumers(currentState, activeConsumers).forEach(transformer::removeConsumerNode);
            findNewSubscriptions(currentState, activeSubscriptions).forEach(transformer::addSubscription);
            findNewConsumers(currentState, activeConsumers).forEach(transformer::addConsumerNode);
            minimizeWorkload(state, transformer, workloadGoals);
            AvailableWork.stream(state, workloadGoals)
                    .forEach(transformer::addAssignment);
            equalizeWorkload(state, transformer);
        });
    }

    private void minimizeWorkload(SubscriptionAssignmentView state,
                                  SubscriptionAssignmentView.Transformer transformer,
                                  WorkloadGoals workloadConstraints) {
        state.getSubscriptions()
                .stream()
                .flatMap(subscriptionName -> findRedundantAssignments(state, subscriptionName, workloadConstraints))
                .forEach(transformer::removeAssignment);
    }

    private Stream<SubscriptionAssignment> findRedundantAssignments(SubscriptionAssignmentView state,
                                                                    SubscriptionName subscriptionName,
                                                                    WorkloadGoals constraints) {
        final int assignedConsumers = state.getAssignmentsCountForSubscription(subscriptionName);
        final int requiredConsumers = constraints.getConsumerCount(subscriptionName);
        int redundantConsumers = assignedConsumers - requiredConsumers;
        if (redundantConsumers > 0) {
            Stream.Builder<SubscriptionAssignment> redundant = Stream.builder();
            Iterator<SubscriptionAssignment> iterator = state.getAssignmentsForSubscription(subscriptionName).iterator();
            while (redundantConsumers > 0 && iterator.hasNext()) {
                SubscriptionAssignment assignment = iterator.next();
                redundant.add(assignment);
                redundantConsumers--;
            }
            return redundant.build();
        }
        return Stream.empty();
    }

    private void equalizeWorkload(SubscriptionAssignmentView state,
                                  SubscriptionAssignmentView.Transformer transformer) {
        if (state.getSubscriptionsCount() > 1 && !state.getConsumerNodes().isEmpty()) {
            boolean transferred;
            do {
                transferred = false;

                String maxLoaded = maxLoadedConsumerNode(state);
                String minLoaded = minLoadedConsumerNode(state);
                int maxLoad = state.getAssignmentsCountForConsumerNode(maxLoaded);
                int minLoad = state.getAssignmentsCountForConsumerNode(minLoaded);

                while (maxLoad > minLoad + 1) {
                    Optional<SubscriptionName> subscription = getSubscriptionForTransfer(state, maxLoaded, minLoaded);
                    if (subscription.isPresent()) {
                        transformer.transferAssignment(maxLoaded, minLoaded, subscription.get());
                        transferred = true;
                    } else break;
                    maxLoad--;
                    minLoad++;
                }
            } while (transferred);
        }
    }

    private String maxLoadedConsumerNode(SubscriptionAssignmentView state) {
        return state.getConsumerNodes().stream().max(comparingInt(state::getAssignmentsCountForConsumerNode)).get();
    }

    private String minLoadedConsumerNode(SubscriptionAssignmentView state) {
        return state.getConsumerNodes().stream().min(comparingInt(state::getAssignmentsCountForConsumerNode)).get();
    }

    private Optional<SubscriptionName> getSubscriptionForTransfer(SubscriptionAssignmentView state,
                                                                  String maxLoaded,
                                                                  String minLoaded) {
        return state.getSubscriptionsForConsumerNode(maxLoaded).stream()
                .filter(s -> !state.getConsumerNodesForSubscription(s).contains(minLoaded))
                .findAny();
    }

    private List<SubscriptionName> findRemovedSubscriptions(SubscriptionAssignmentView state,
                                                            Set<SubscriptionName> subscriptions) {
        return state.getSubscriptions().stream().filter(s -> !subscriptions.contains(s)).collect(toList());
    }

    private List<String> findInactiveConsumers(SubscriptionAssignmentView state, Set<String> activeConsumers) {
        return state.getConsumerNodes().stream().filter(c -> !activeConsumers.contains(c)).collect(toList());
    }

    private List<SubscriptionName> findNewSubscriptions(SubscriptionAssignmentView state,
                                                        Set<SubscriptionName> subscriptions) {
        return subscriptions.stream().filter(s -> !state.getSubscriptions().contains(s)).collect(toList());
    }

    private List<String> findNewConsumers(SubscriptionAssignmentView state, Set<String> activeConsumers) {
        return activeConsumers.stream().filter(c -> !state.getConsumerNodes().contains(c)).collect(toList());
    }
}
