package backtype.storm.scheduler.advancedstela.etp;

import backtype.storm.scheduler.advancedstela.slo.Topology;

public class ResultComponent implements Comparable<ResultComponent>{
    public Component component;
    public Double capacity;

    public  ResultComponent(Component comp, Double value) {
        this.component = comp;
        this.capacity = value;
    }

    @Override
    public int compareTo(ResultComponent that) {
        return this.capacity.compareTo(that.capacity);
    }

    public Integer getExecutorsForRebalancing() {
        Double cap = TopologySchedule.CAPACITY_CONGESTION_THRESHOLD ;
        Double curr_cap = capacity;
        if (cap > curr_cap) return 0;
        Double prop = ((curr_cap - cap)/cap);
        System.out.println("Prop " + prop);
        Double execs = Math.ceil(prop * (Topology.MAX_EXECUTORS * 1.0));
        System.out.println("Execs " + execs);
        Integer executors = execs.intValue();
        System.out.println("Executors " + executors);
        if (executors == 0) return 1;
        return executors;
    }
}
