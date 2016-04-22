package backtype.storm.scheduler.advancedstela.etp;

import backtype.storm.generated.ExecutorSummary;
import backtype.storm.scheduler.ExecutorDetails;

import java.util.ArrayList;
import java.util.List;

public class Component {
    private String id;
    private Integer parallelism;
    private List<String> parents;
    private List<String> children;
    private List<ExecutorDetails> executorDetails;
    private List<ExecutorSummary> executorSummaries;
    private double process_msg_latency;
    private double execute_msg_latency;
    private double complete_msg_avg_latency;
    private double capacity;
    private long lastRebalancedAt;

    public Component(String identifier, int parallelismHint){ //, double process_msg_latency, double execute_msg_latency, double complete_msg_avg_latency, double rate) {
        id = identifier;
        parallelism = parallelismHint;
        parents = new ArrayList<String>();
        children = new ArrayList<String>();
        executorDetails = new ArrayList<ExecutorDetails>();
        executorSummaries = new ArrayList<ExecutorSummary>();
        this.process_msg_latency = 0.0;
        this.execute_msg_latency = 0.0;
        this.complete_msg_avg_latency = 0.0;
        this.capacity = 0.0; // Expectation -> bolts.capacity	String (double value returned in String format)	This value indicates number of messages executed * average execute latency / time window
        long lastRebalancedAt = 0L;
    }

    public String getId() {
        return id;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public void setParallelism(Integer newParallelism) {
        parallelism = newParallelism;
    }

    public double getProcessLatency() {
        return process_msg_latency;
    }

    public void setProcessLatency(double latency) {
       process_msg_latency = latency;
    }

    public double getExecuteLatency() {
        return execute_msg_latency;
    }

    public void setExecuteLatency(double latency) {
        execute_msg_latency = latency;

    }

    public double getCompleteLatency() {
        return complete_msg_avg_latency;
    }

    public void setCompleteLatency(double latency) {
        complete_msg_avg_latency = latency;
    }

    public double getCapacity() {
        return capacity;
    }

    public void setCapacity(double capacity) {
       this.capacity = capacity;
    }

    public long getLastRebalancedAt() {
        return lastRebalancedAt;
    }

    public void setLastRebalancedAt(long time) {
        lastRebalancedAt = time;
    }

    public List<String> getParents() {
        return parents;
    }

    public List<String> getChildren() {
        return children;
    }

    public List<ExecutorDetails> getExecutorDetails() {
        return executorDetails;
    }

    public List<ExecutorSummary> getExecutorSummaries() {
        return executorSummaries;
    }

    public void addParent(String parentId) {
        parents.add(parentId);
    }

    public void addChild(String childId) {
        children.add(childId);
    }

    public void addExecutor(ExecutorDetails executor) {
        executorDetails.add(executor);
    }

    public void addExecutorSummary(ExecutorSummary summary) {
        executorSummaries.add(summary);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Component component = (Component) o;

        return !(id != null ? !id.equals(component.id) : component.id != null);

    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }
}
