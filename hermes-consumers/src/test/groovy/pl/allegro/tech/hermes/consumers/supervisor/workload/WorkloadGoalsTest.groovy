package pl.allegro.tech.hermes.consumers.supervisor.workload

import pl.allegro.tech.hermes.api.Constraints
import pl.allegro.tech.hermes.api.SubscriptionName
import pl.allegro.tech.hermes.api.TopicName
import spock.lang.Specification
import spock.lang.Unroll

import static java.util.Collections.emptyMap

class WorkloadGoalsTest extends Specification {

    @Unroll
    def "should return constraints of given subscription or default value if constraints don't exist (#subscriptionName)"() {
        given:
        def workloadGoals = WorkloadGoals.builder()
                .withSubscriptionConstraints([
                        (SubscriptionName.fromString('group.topic$sub1')): new Constraints(3),
                        (SubscriptionName.fromString('group.topic$sub2')): new Constraints(1)
                ])
                .withActiveConsumers(["consumer1", "consumer2", "consumer3", "consumer4"])
                .withConsumersPerSubscription(2)
                .withMaxSubscriptionsPerConsumer(2)
                .build()

        expect:
        workloadGoals.getConsumerCount(subscriptionName) == expectedResult

        where:
        subscriptionName                                     | expectedResult
        SubscriptionName.fromString('group.topic$sub1')      | 3
        SubscriptionName.fromString('group.topic$undefined') | 2
    }

    @Unroll
    def "should return subscription constraints or topic constraints if given subscription has no constraints (#subscriptionName)"() {
        def workloadGoals = WorkloadGoals.builder()
                .withSubscriptionConstraints([
                        (SubscriptionName.fromString('group.topic$sub1')): new Constraints(3)
                ])
                .withTopicConstraints([
                        (TopicName.fromQualifiedName('group.topic')): new Constraints(2)
                ])
                .withActiveConsumers(["consumer1", "consumer2", "consumer3", "consumer4"])
                .withConsumersPerSubscription(2)
                .withMaxSubscriptionsPerConsumer(2)
                .build()

        expect:
        workloadGoals.getConsumerCount(subscriptionName) == expectedResult

        where:
        subscriptionName                                | expectedResult
        SubscriptionName.fromString('group.topic$sub1') | 3
        SubscriptionName.fromString('group.topic$sub2') | 2
    }

    @Unroll
    def "should return max available number of consumers if specified constraint is higher than available consumers number"() {
        given:
        def availableConsumers = ["consumer1", "consumer2", "consumer3", "consumer4"]
        def workloadGoals = WorkloadGoals.builder()
                .withSubscriptionConstraints([
                        (SubscriptionName.fromString('group.topic1$sub')): new Constraints(availableConsumers.size() + 1)
                ])
                .withTopicConstraints([
                        (TopicName.fromQualifiedName('group.topic2')): new Constraints(availableConsumers.size() + 1)
                ])
                .withActiveConsumers(availableConsumers)
                .withConsumersPerSubscription(2)
                .withMaxSubscriptionsPerConsumer(2)
                .build()

        expect:
        workloadGoals.getConsumerCount(subscriptionName) == availableConsumers.size()

        where:
        subscriptionName << [
                SubscriptionName.fromString('group.topic1$sub'),
                SubscriptionName.fromString('group.topic2$sub')
        ]
    }

    @Unroll
    def "should return default number of consumers if specified constraints for topic have value less or equal to 0 (#incorrectConsumersNumber)"() {
        given:
        def subscriptionName = SubscriptionName.fromString('group.topic$sub')
        def workloadGoals = WorkloadGoals.builder()
                .withTopicConstraints([
                        (TopicName.fromQualifiedName('group.topic')): new Constraints(incorrectConsumersNumber)
                ])
                .withActiveConsumers(["consumer1", "consumer2", "consumer3", "consumer4"])
                .withConsumersPerSubscription(2)
                .withMaxSubscriptionsPerConsumer(2)
                .build()

        expect:
        workloadGoals.getConsumerCount(subscriptionName) == 2

        where:
        incorrectConsumersNumber << [0, -1]
    }

    @Unroll
    def "should return default number of consumers if specified constraints for subscription have value less or equal to 0 (#incorrectConsumersNumber)"() {
        given:
        def subscriptionName = SubscriptionName.fromString('group.incorrect_topic$sub')
        def workloadGoals = WorkloadGoals.builder()
                .withSubscriptionConstraints([
                        (subscriptionName): new Constraints(incorrectConsumersNumber)
                ])
                .withActiveConsumers(["consumer1", "consumer2", "consumer3", "consumer4"])
                .withConsumersPerSubscription(2)
                .withMaxSubscriptionsPerConsumer(2)
                .build()

        expect:
        workloadGoals.getConsumerCount(subscriptionName) == 2

        where:
        incorrectConsumersNumber << [0, -1]
    }

    @Unroll
    def "should return default number of consumers if specified constraints are null"() {
        given:
        def subscriptionName = SubscriptionName.fromString('group.incorrect_topic$sub')
        def workloadGoals = WorkloadGoals.builder()
                .withSubscriptionConstraints(constraintsSubscription as Map<SubscriptionName, Constraints>)
                .withTopicConstraints(constraintsTopic as Map<TopicName, Constraints>)
                .withActiveConsumers(["consumer1", "consumer2", "consumer3", "consumer4"])
                .withConsumersPerSubscription(2)
                .withMaxSubscriptionsPerConsumer(2)
                .build()

        expect:
        workloadGoals.getConsumerCount(subscriptionName) == 2

        where:
        constraintsSubscription | constraintsTopic
        null                    | emptyMap()
        emptyMap()              | null
    }
}
