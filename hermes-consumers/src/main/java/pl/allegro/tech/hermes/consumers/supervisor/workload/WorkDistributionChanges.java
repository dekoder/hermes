package pl.allegro.tech.hermes.consumers.supervisor.workload;

import static java.lang.String.format;

class WorkDistributionChanges {

    static final WorkDistributionChanges NO_CHANGES = new WorkDistributionChanges(0, 0);

    private final int assignmentsDeleted;
    private final int assignmentsCreated;

    WorkDistributionChanges(int assignmentsDeleted, int assignmentsCreated) {
        this.assignmentsDeleted = assignmentsDeleted;
        this.assignmentsCreated = assignmentsCreated;
    }

    int getDeletedAssignmentsCount() {
        return assignmentsDeleted;
    }

    int getCreatedAssignmentsCount() {
        return assignmentsCreated;
    }

    @Override
    public String toString() {
        return format("assignments_created=%d, assignments_deleted=%d", assignmentsCreated, assignmentsDeleted);
    }
}
