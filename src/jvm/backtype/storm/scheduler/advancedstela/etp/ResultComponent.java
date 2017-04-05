package backtype.storm.scheduler.advancedstela.etp;

import backtype.storm.scheduler.advancedstela.slo.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResultComponent implements Comparable<ResultComponent>{
    public Component component;
    public Double capacity;
    private static final Logger LOG = LoggerFactory.getLogger(ResultComponent.class);

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
     //   LOG.info("Proportion " + prop);
        Double execs = Math.ceil(prop * (Topology.MULTIPLIER * 1.0));
    //    LOG.info("Execs " + execs);
        Integer executors = Math.min(Topology.MAX_EXECUTORS, execs.intValue());
        if (executors == 0) return 1;
      //  LOG.info("Executors " + executors);
        return executors;
    }
}
