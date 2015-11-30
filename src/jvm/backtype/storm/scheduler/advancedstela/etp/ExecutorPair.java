package backtype.storm.scheduler.advancedstela.etp;

import backtype.storm.generated.ExecutorSummary;

public class ExecutorPair {
    private ExecutorSummary targetExecutorSummary;
    private ExecutorSummary victimExecutorSummary;

    public ExecutorPair(ExecutorSummary target, ExecutorSummary summary) {
        targetExecutorSummary = target;
        victimExecutorSummary = summary;
    }

    public ExecutorSummary getTargetExecutorSummary() {
        return targetExecutorSummary;
    }

    public ExecutorSummary getVictimExecutorSummary() {
        return victimExecutorSummary;
    }

    public boolean bothPopulated() {
        return targetExecutorSummary != null && victimExecutorSummary != null;
    }
}
