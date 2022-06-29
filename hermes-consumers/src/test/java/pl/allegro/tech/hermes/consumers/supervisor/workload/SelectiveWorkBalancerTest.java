package pl.allegro.tech.hermes.consumers.supervisor.workload;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Test;
import org.junit.runner.RunWith;
import pl.allegro.tech.hermes.api.Constraints;
import pl.allegro.tech.hermes.api.SubscriptionName;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;
import static org.apache.curator.shaded.com.google.common.collect.Iterables.get;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(DataProviderRunner.class)
public class SelectiveWorkBalancerTest {

    private final SelectiveWorkBalancer workBalancer = new SelectiveWorkBalancer();

    @Test
    public void shouldPerformSubscriptionsCleanup() {
        // given
        Set<SubscriptionName> activeSubscriptions = someSubscriptions(1);
        Set<String> activeConsumers = Set.of("c1");
        WorkloadGoals initialGoals = WorkloadGoals.builder()
                .withConsumersPerSubscription(2)
                .withMaxSubscriptionsPerConsumer(2)
                .withActiveConsumers(activeConsumers)
                .withActiveSubscriptions(activeSubscriptions)
                .build();
        SubscriptionAssignmentView initialState = initialState(initialGoals);
        WorkloadGoals targetGoals = WorkloadGoals.builder()
                .withWorkloadGoals(initialGoals)
                .withActiveSubscriptions(emptySet())
                .build();

        // when
        SubscriptionAssignmentView targetState = workBalancer.balance(initialState, targetGoals);

        // then
        assertThat(targetState.getSubscriptions()).isEmpty();
        assertThat(targetState.deletions(initialState).getSubscriptions()).isEmpty();
    }

    @Test
    public void shouldPerformSupervisorsCleanup() {
        // given
        Set<String> activeConsumers = Set.of("c1", "c2");
        SubscriptionName activeSubscription = anySubscription();
        WorkloadGoals initialGoals = WorkloadGoals.builder()
                .withConsumersPerSubscription(2)
                .withMaxSubscriptionsPerConsumer(2)
                .withActiveConsumers(activeConsumers)
                .withActiveSubscriptions(Set.of(activeSubscription))
                .build();
        SubscriptionAssignmentView initialState = initialState(initialGoals);
        WorkloadGoals targetGoals = WorkloadGoals.builder()
                .withWorkloadGoals(initialGoals)
                .withActiveConsumers(Set.of("c1"))
                .build();

        // when
        SubscriptionAssignmentView targetState = workBalancer.balance(initialState, targetGoals);

        // then
        assertThat(targetState.getConsumerNodes()).hasSize(1);
        assertThatSubscriptionIsAssignedTo(targetState, activeSubscription, "c1");
    }

    @Test
    public void shouldBalanceWorkForSingleSubscription() {
        // given
        Set<String> activeConsumers = Set.of("c1");
        SubscriptionName activeSubscription = anySubscription();
        WorkloadGoals initialGoals = WorkloadGoals.builder()
                .withConsumersPerSubscription(2)
                .withMaxSubscriptionsPerConsumer(2)
                .withActiveConsumers(activeConsumers)
                .withActiveSubscriptions(Set.of(activeSubscription))
                .build();

        // when
        SubscriptionAssignmentView targetState = initialState(initialGoals);

        // then
        assertThatSubscriptionIsAssignedTo(targetState, activeSubscription, "c1");
    }

    @Test
    public void shouldBalanceWorkForMultipleConsumersAndSingleSubscription() {
        // given
        Set<String> activeConsumers = Set.of("c1", "c2");
        SubscriptionName activeSubscription = anySubscription();
        WorkloadGoals initialGoals = WorkloadGoals.builder()
                .withConsumersPerSubscription(2)
                .withMaxSubscriptionsPerConsumer(2)
                .withActiveConsumers(activeConsumers)
                .withActiveSubscriptions(Set.of(activeSubscription))
                .build();

        // when
        SubscriptionAssignmentView targetState = initialState(initialGoals);

        // then
        assertThatSubscriptionIsAssignedTo(targetState, activeSubscription, activeConsumers);
    }

    @Test
    public void shouldBalanceWorkForMultipleConsumersAndMultipleSubscriptions() {
        // given
        Set<String> activeConsumers = Set.of("c1", "c2");
        SubscriptionName firstActiveSubscription = anySubscription();
        SubscriptionName secondActiveSubscription = anySubscription();
        WorkloadGoals initialGoals = WorkloadGoals.builder()
                .withConsumersPerSubscription(2)
                .withMaxSubscriptionsPerConsumer(2)
                .withActiveConsumers(activeConsumers)
                .withActiveSubscriptions(Set.of(firstActiveSubscription, secondActiveSubscription))
                .build();

        // when
        SubscriptionAssignmentView targetState = initialState(initialGoals);

        // then
        assertThatSubscriptionIsAssignedTo(targetState, firstActiveSubscription, activeConsumers);
        assertThatSubscriptionIsAssignedTo(targetState, secondActiveSubscription, activeConsumers);
    }

    @Test
    public void shouldNotOverloadConsumers() {
        // given
        Set<String> activeConsumers = Set.of("c1");
        Set<SubscriptionName> activeSubscriptions = someSubscriptions(3);
        WorkloadGoals initialGoals = WorkloadGoals.builder()
                .withConsumersPerSubscription(2)
                .withMaxSubscriptionsPerConsumer(2)
                .withActiveConsumers(activeConsumers)
                .withActiveSubscriptions(activeSubscriptions)
                .build();

        // when
        SubscriptionAssignmentView targetState = initialState(initialGoals);

        // then
        assertThat(targetState.getAssignmentsForConsumerNode("c1")).hasSize(2);
    }

    @Test
    public void shouldRebalanceAfterConsumerDisappearing() {
        // given
        Set<String> activeConsumers = Set.of("c1", "c2");
        SubscriptionName firstActiveSubscription = anySubscription();
        SubscriptionName secondActiveSubscription = anySubscription();
        WorkloadGoals initialGoals = WorkloadGoals.builder()
                .withConsumersPerSubscription(2)
                .withMaxSubscriptionsPerConsumer(2)
                .withActiveConsumers(activeConsumers)
                .withActiveSubscriptions(Set.of(firstActiveSubscription, secondActiveSubscription))
                .build();
        SubscriptionAssignmentView initialState = initialState(initialGoals);
        WorkloadGoals targetGoals = WorkloadGoals.builder()
                .withWorkloadGoals(initialGoals)
                .withActiveConsumers(Set.of("c1", "c3"))
                .build();

        // when
        SubscriptionAssignmentView targetState = workBalancer.balance(initialState, targetGoals);

        // then
        assertThat(targetState.getSubscriptionsForConsumerNode("c3"))
                .containsOnly(firstActiveSubscription, secondActiveSubscription);
    }

    @Test
    public void shouldAssignWorkToNewConsumersByWorkStealing() {
        // given
        Set<String> activeConsumers = Set.of("c1", "c2");
        SubscriptionName firstActiveSubscription = anySubscription();
        SubscriptionName secondActiveSubscription = anySubscription();
        WorkloadGoals initialGoals = WorkloadGoals.builder()
                .withConsumersPerSubscription(2)
                .withMaxSubscriptionsPerConsumer(2)
                .withActiveConsumers(activeConsumers)
                .withActiveSubscriptions(Set.of(firstActiveSubscription, secondActiveSubscription))
                .build();
        SubscriptionAssignmentView initialState = initialState(initialGoals);
        WorkloadGoals targetGoals = WorkloadGoals.builder()
                .withWorkloadGoals(initialGoals)
                .withActiveConsumers(Set.of("c1", "c3", "new-supervisor"))
                .build();

        // when
        SubscriptionAssignmentView targetState = workBalancer.balance(initialState, targetGoals);

        // then
        assertThat(targetState.getAssignmentsForConsumerNode("new-supervisor").size()).isGreaterThan(0);
        assertThat(targetState.getAssignmentsForSubscription(firstActiveSubscription)).hasSize(2);
        assertThat(targetState.getAssignmentsForSubscription(secondActiveSubscription)).hasSize(2);
    }

    @Test
    public void shouldEquallyAssignWorkToConsumers() {
        // given
        Set<String> activeConsumers = Set.of("c1", "c2");
        Set<SubscriptionName> activeSubscriptions = someSubscriptions(50);
        WorkloadGoals initialGoals = WorkloadGoals.builder()
                .withConsumersPerSubscription(2)
                .withMaxSubscriptionsPerConsumer(200)
                .withActiveConsumers(activeConsumers)
                .withActiveSubscriptions(activeSubscriptions)
                .build();
        SubscriptionAssignmentView initialState = initialState(initialGoals);
        WorkloadGoals targetGoals = WorkloadGoals.builder()
                .withWorkloadGoals(initialGoals)
                .withActiveConsumers(Set.of("c1", "c2", "c3"))
                .build();

        // when
        SubscriptionAssignmentView targetState = workBalancer.balance(initialState, targetGoals);

        // then
        assertThat(targetState.getAssignmentsForConsumerNode("c3")).hasSize(50 * 2 / 3);
    }

    @Test
    public void shouldReassignWorkToFreeConsumers() {
        // given
        Set<String> activeConsumers = Set.of("c1");
        Set<SubscriptionName> activeSubscriptions = someSubscriptions(10);
        WorkloadGoals initialGoals = WorkloadGoals.builder()
                .withConsumersPerSubscription(1)
                .withMaxSubscriptionsPerConsumer(100)
                .withActiveConsumers(activeConsumers)
                .withActiveSubscriptions(activeSubscriptions)
                .build();
        SubscriptionAssignmentView currentState = initialState(initialGoals);
        WorkloadGoals targetGoals = WorkloadGoals.builder()
                .withWorkloadGoals(initialGoals)
                .withActiveConsumers(Set.of("c1", "c2", "c3", "c4", "c5"))
                .build();

        // when
        SubscriptionAssignmentView targetState = workBalancer.balance(currentState, targetGoals);

        // then
        assertThat(targetState.getAssignmentsForConsumerNode("c5")).hasSize(2);
    }

    @Test
    public void shouldRemoveRedundantWorkAssignmentsToKeepWorkloadMinimal() {
        // given
        Set<String> activeConsumers = Set.of("c1", "c2", "c3");
        Set<SubscriptionName> activeSubscriptions = someSubscriptions(10);
        WorkloadGoals initialGoals = WorkloadGoals.builder()
                .withConsumersPerSubscription(3)
                .withMaxSubscriptionsPerConsumer(100)
                .withActiveConsumers(activeConsumers)
                .withActiveSubscriptions(activeSubscriptions)
                .build();
        SubscriptionAssignmentView currentState = initialState(initialGoals);
        WorkloadGoals targetGoals = WorkloadGoals.builder()
                .withWorkloadGoals(initialGoals)
                .withConsumersPerSubscription(1)
                .build();

        // when
        SubscriptionAssignmentView targetState = workBalancer.balance(currentState, targetGoals);

        // then
        assertThat(targetState.getAssignmentsCountForSubscription(get(activeSubscriptions, 0))).isEqualTo(1);
    }

    @DataProvider
    public static Object[][] subscriptionConstraints() {
        return new Object[][] {
                { 1 }, { 3 }
        };
    }

    @Test
    @UseDataProvider("subscriptionConstraints")
    public void shouldAssignConsumersForSubscriptionsAccordingToConstraints(int requiredConsumersNumber) {
        // given
        SubscriptionAssignmentView initialState = new SubscriptionAssignmentView(emptyMap());
        Set<String> activeConsumers = Set.of("c1", "c2", "c3");
        Set<SubscriptionName> activeSubscriptions = someSubscriptions(4);
        WorkloadGoals targetGoals = WorkloadGoals.builder()
                .withSubscriptionConstraints(Map.of(
                                get(activeSubscriptions, 0), new Constraints(requiredConsumersNumber)
                ))
                .withConsumersPerSubscription(2)
                .withMaxSubscriptionsPerConsumer(4)
                .withActiveConsumers(activeConsumers)
                .withActiveSubscriptions(activeSubscriptions)
                .build();

        // when
        SubscriptionAssignmentView targetState = workBalancer.balance(initialState, targetGoals);

        // then
        assertThat(targetState.getAssignmentsForSubscription(get(activeSubscriptions, 0)).size()).isEqualTo(requiredConsumersNumber);
        assertThat(targetState.getAssignmentsForSubscription(get(activeSubscriptions, 1)).size()).isEqualTo(2);
        assertThat(targetState.getAssignmentsForSubscription(get(activeSubscriptions, 2)).size()).isEqualTo(2);
        assertThat(targetState.getAssignmentsForSubscription(get(activeSubscriptions, 3)).size()).isEqualTo(2);
    }

    private SubscriptionAssignmentView initialState(WorkloadGoals workloadGoals) {
        return workBalancer.balance(new SubscriptionAssignmentView(emptyMap()), workloadGoals);
    }

    private Set<SubscriptionName> someSubscriptions(int count) {
        return IntStream.range(0, count).mapToObj(i -> anySubscription()).collect(toSet());
    }

    private SubscriptionName anySubscription() {
        return SubscriptionName.fromString("tech.topic$s" + UUID.randomUUID().getMostSignificantBits());
    }

    private void assertThatSubscriptionIsAssignedTo(SubscriptionAssignmentView work, SubscriptionName sub, String... nodeIds) {
        assertThatSubscriptionIsAssignedTo(work, sub, Stream.of(nodeIds).collect(toSet()));
    }

    private void assertThatSubscriptionIsAssignedTo(SubscriptionAssignmentView work, SubscriptionName sub, Set<String> nodeIds) {
        assertThat(work.getAssignmentsForSubscription(sub))
                .extracting(SubscriptionAssignment::getConsumerNodeId)
                .containsOnly(nodeIds.toArray(String[]::new));
    }
}
