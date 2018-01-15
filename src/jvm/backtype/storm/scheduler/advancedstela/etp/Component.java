package backtype.storm.scheduler.advancedstela.etp;

import backtype.storm.generated.ExecutorSummary;
import backtype.storm.scheduler.ExecutorDetails;

import java.util.ArrayList;
import java.util.List;

public class Component implements Comparable<Component>{

    private String id;
    private Integer parallelism;
    private List<ExecutorDetails> executorDetails;
    private List<ExecutorSummary> executorSummaries;
    private Double capacity;

    public int compareTo(Component other) {
        return this.capacity.compareTo(other.getCapacity());
    }

    public Component(String identifier, int parallelismHint){ //, double process_msg_latency,  double execute_msg_latency, double complete_msg_avg_latency, double rate) {
        id = identifier;
        parallelism = parallelismHint;
        executorDetails = new ArrayList<ExecutorDetails>();
        executorSummaries = new ArrayList<ExecutorSummary>();
        this.capacity = 0.0; // Expectation -> bolts.capacity	String (double value returned in String format)	This value indicates number of messages executed * average execute latency / time window

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


    public Double getCapacity() {
        return capacity;
    }

    public void setCapacity(double capacity) {
       this.capacity = capacity;
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
