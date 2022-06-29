package pl.allegro.tech.hermes.consumers.supervisor.workload;

import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.metric.HermesMetrics;
import pl.allegro.tech.hermes.consumers.registry.ConsumerNodesRegistry;
import pl.allegro.tech.hermes.consumers.subscription.cache.SubscriptionsCache;
import pl.allegro.tech.hermes.domain.workload.constraints.ConsumersWorkloadConstraints;
import pl.allegro.tech.hermes.domain.workload.constraints.WorkloadConstraintsRepository;

import java.util.HashSet;

import static pl.allegro.tech.hermes.common.config.Configs.CONSUMER_WORKLOAD_CONSUMERS_PER_SUBSCRIPTION;
import static pl.allegro.tech.hermes.common.config.Configs.CONSUMER_WORKLOAD_MAX_SUBSCRIPTIONS_PER_CONSUMER;

class BalancingJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(BalancingJob.class);

    private final ConsumerNodesRegistry consumersRegistry;
    private final ConfigFactory configFactory;
    private final SubscriptionsCache subscriptionsCache;
    private final ClusterAssignmentCache clusterAssignmentCache;
    private final ConsumerAssignmentRegistry consumerAssignmentRegistry;
    private final WorkBalancer workBalancer;
    private final HermesMetrics metrics;
    private final String kafkaCluster;
    private final WorkloadConstraintsRepository workloadConstraintsRepository;
    private final BalancingJobMetrics balancingMetrics = new BalancingJobMetrics();

    BalancingJob(ConsumerNodesRegistry consumersRegistry,
                 ConfigFactory configFactory,
                 SubscriptionsCache subscriptionsCache,
                 ClusterAssignmentCache clusterAssignmentCache,
                 ConsumerAssignmentRegistry consumerAssignmentRegistry,
                 WorkBalancer workBalancer,
                 HermesMetrics metrics,
                 String kafkaCluster,
                 WorkloadConstraintsRepository workloadConstraintsRepository) {
        this.consumersRegistry = consumersRegistry;
        this.configFactory = configFactory;
        this.subscriptionsCache = subscriptionsCache;
        this.clusterAssignmentCache = clusterAssignmentCache;
        this.consumerAssignmentRegistry = consumerAssignmentRegistry;
        this.workBalancer = workBalancer;
        this.metrics = metrics;
        this.kafkaCluster = kafkaCluster;
        this.workloadConstraintsRepository = workloadConstraintsRepository;
        metrics.registerGauge(
                gaugeName(kafkaCluster, "selective.all-assignments"),
                () -> balancingMetrics.allAssignments
        );
        metrics.registerGauge(
                gaugeName(kafkaCluster, "selective.missing-resources"),
                () -> balancingMetrics.missingResources
        );
        metrics.registerGauge(
                gaugeName(kafkaCluster, ".selective.deleted-assignments"),
                () -> balancingMetrics.deletedAssignments
        );
        metrics.registerGauge(
                gaugeName(kafkaCluster, ".selective.created-assignments"),
                () -> balancingMetrics.createdAssignments
        );
    }

    private String gaugeName(String kafkaCluster, String name) {
        return "consumers-workload." + kafkaCluster + "." + name;
    }

    @Override
    public void run() {
        try {
            consumersRegistry.refresh();
            if (consumersRegistry.isLeader()) {
                try (Timer.Context ctx = metrics.consumersWorkloadRebalanceDurationTimer(kafkaCluster).time()) {
                    logger.info("Initializing workload balance.");
                    clusterAssignmentCache.refresh();

                    SubscriptionAssignmentView initialState = clusterAssignmentCache.createSnapshot();
                    WorkloadGoals workloadGoals = prepareWorkloadGoals();
                    SubscriptionAssignmentView balancedState = workBalancer.balance(initialState, workloadGoals);

                    if (consumersRegistry.isLeader()) {
                        logger.info("Applying workload balance changes");
                        WorkDistributionChanges changes = consumerAssignmentRegistry.updateAssignments(initialState, balancedState);
                        logger.info("Finished workload balance");

                        clusterAssignmentCache.refresh(); // refresh cache with just stored data

                        int missingResources = workloadGoals.countMissingResources(balancedState);
                        updateMetrics(balancedState, missingResources, changes);
                    } else {
                        logger.info("Lost leadership before applying changes");
                    }
                }
            } else {
                balancingMetrics.reset();
            }
        } catch (Exception e) {
            logger.error("Caught exception when running balancing job", e);
        }
    }

    private WorkloadGoals prepareWorkloadGoals() {
        ConsumersWorkloadConstraints constraints = workloadConstraintsRepository.getConsumersWorkloadConstraints();
        return WorkloadGoals.builder()
                .withSubscriptionConstraints(constraints.getSubscriptionConstraints())
                .withTopicConstraints(constraints.getTopicConstraints())
                .withActiveSubscriptions(new HashSet<>(subscriptionsCache.listActiveSubscriptionNames()))
                .withActiveConsumers(new HashSet<>(consumersRegistry.listConsumerNodes()))
                .withConsumersPerSubscription(configFactory.getIntProperty(CONSUMER_WORKLOAD_CONSUMERS_PER_SUBSCRIPTION))
                .withMaxSubscriptionsPerConsumer(configFactory.getIntProperty(CONSUMER_WORKLOAD_MAX_SUBSCRIPTIONS_PER_CONSUMER))
                .build();
    }

    private void updateMetrics(SubscriptionAssignmentView balancedState, int missingResources, WorkDistributionChanges changes) {
        this.balancingMetrics.allAssignments = balancedState.getAllAssignments().size();
        this.balancingMetrics.missingResources = missingResources;
        this.balancingMetrics.createdAssignments = changes.getCreatedAssignmentsCount();
        this.balancingMetrics.deletedAssignments = changes.getDeletedAssignmentsCount();
    }

    private static class BalancingJobMetrics {

        volatile int allAssignments;

        volatile int missingResources;

        volatile int deletedAssignments;

        volatile int createdAssignments;

        void reset() {
            this.allAssignments = 0;
            this.missingResources = 0;
            this.deletedAssignments = 0;
            this.createdAssignments = 0;
        }
    }
}
