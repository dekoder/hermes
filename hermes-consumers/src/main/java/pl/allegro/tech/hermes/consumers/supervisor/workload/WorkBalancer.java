package pl.allegro.tech.hermes.consumers.supervisor.workload;

public interface WorkBalancer {

    SubscriptionAssignmentView balance(SubscriptionAssignmentView currentState, WorkloadGoals workloadGoals);
}
