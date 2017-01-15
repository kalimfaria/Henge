package backtype.storm.scheduler.advancedstela;

import backtype.storm.generated.StormTopology;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.advancedstela.slo.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class History implements Comparable {
    private HashMap<String, Topology> topologiesPerformance;
    private HashMap<String, TopologyDetails> topologySchedules;
    private Double systemUtility;
    private static final Logger LOG = LoggerFactory.getLogger(History.class);

    public boolean doWeNeedToRevert(History current) {
        LOG.info("In do we need to revert.");
        Double currentUtility = current.getSystemUtility();
        LOG.info("Current performance {} Old performance {}, currentUtility * 1.1 {}", currentUtility, this.systemUtility, currentUtility * 1.1);
        if (this.systemUtility > currentUtility * 1.1) { // place a threshold here
            return true;
        }
        return false;
    }

    public HashMap<String, Topology> getTopologiesPerformance() {
        return topologiesPerformance;
    }

    public HashMap<String, TopologyDetails> getTopologiesSchedule() {
        return topologySchedules;
    }

    public History(Map<String, TopologyDetails> schedule,
                   HashMap<String, Topology> performance) {
        topologiesPerformance = new HashMap<>();
        topologySchedules = new HashMap<>();
        systemUtility = 0.0;

        for (Map.Entry<String, Topology> topology : performance.entrySet()) {
            systemUtility += topology.getValue().getCurrentUtility();
        }
        // copy schedules first
        for (Map.Entry<String, TopologyDetails> scheduleEntry : schedule.entrySet()) {
            String topologyName = scheduleEntry.getKey();
            HashMap topologyConf = new HashMap<>();
            topologyConf.putAll(scheduleEntry.getValue().getConf());
            StormTopology topology = scheduleEntry.getValue().getTopology();
            int numWorkers = scheduleEntry.getValue().getNumWorkers();
            Map<ExecutorDetails, String> executorToComponents = new HashMap<>();

            // copied over executor to component
            for (Map.Entry<ExecutorDetails, String> executorToComponent : scheduleEntry.getValue().getExecutorToComponent().entrySet()) {
                executorToComponents.put(
                        new ExecutorDetails(
                                executorToComponent.getKey().getStartTask(),
                                executorToComponent.getKey().getEndTask()),
                        executorToComponent.getValue());
            }
            TopologyDetails details = new TopologyDetails(
                    topologyName,
                    topologyConf,
                    topology,
                    numWorkers,
                    executorToComponents);
            topologySchedules.put(topologyName, details);
        }

        // copy performance
        for (Map.Entry<String, Topology> performanceEntry : performance.entrySet()) {
            String name = performanceEntry.getKey();
            Topology temp = performanceEntry.getValue();
            Topology topology = new Topology(temp.getId(),
                    temp.getUserSpecifiedSLO(),
                    temp.getUserSpecifiedLatencySLO(),
                    temp.getTopologyUtility(),
                    temp.getWorkers());
            topology.setMeasuredSLOs(temp.getMeasuredSLO());
            topology.setAverageLatency(temp.getAverageLatency());
            topologiesPerformance.put(name, topology);
        }
    }

    public Double getSystemUtility() {
        return systemUtility;
    }

    @Override
    public int compareTo(Object o) { // this is going to give us ascending order
        if (o instanceof History) {
            History h2 = (History) o;
            return this.systemUtility.compareTo(h2.getSystemUtility());
        }
        return 0;
    }

    @Override
    public boolean equals(Object o) { // check whether the two histories are the same
        if (o instanceof History) {
            History h1 = (History) o;
            HashMap<String, TopologyDetails> h1_sched = h1.getTopologiesSchedule();
            LOG.info("h1_sched {}, sched {}", h1_sched.size(), topologySchedules.size());
            if (h1_sched.size() != topologySchedules.size()) return false;
            for (Map.Entry<String, TopologyDetails> entry : h1_sched.entrySet()) {
                LOG.info("history topology name {}", entry.getKey());
                if (!topologySchedules.containsKey(entry.getKey())) return false;
                LOG.info("topologySchedules.containsKey {}", topologySchedules.containsKey(entry.getKey()));
                TopologyDetails entryDetails = entry.getValue();

                LOG.info("Total number of executors in h1 {}", entryDetails.getExecutorToComponent().size());
                LOG.info("Total number of executors in this one {}", topologySchedules.get(entry.getKey()).getExecutorToComponent().size());

                if (entryDetails.getExecutorToComponent().size() != topologySchedules.get(entry.getKey()).getExecutorToComponent().size()) return false;

                Map<ExecutorDetails, String> h1_executors = entryDetails.getExecutorToComponent();
                Map<String, Integer> h1_executors_flipped = new Helpers().flipExecsMap(h1_executors);
                Map<String, Integer> executors_flipped = new Helpers().flipExecsMap(topologySchedules.get(entry.getKey()).getExecutorToComponent());
                for (Map.Entry<String, Integer>  h1_executors_flipped_entry: h1_executors_flipped.entrySet()) {
                    if (executors_flipped.containsKey(h1_executors_flipped_entry.getKey()))  {
                        LOG.info("h1 component name {}, number of executors {}", h1_executors_flipped_entry.getKey(), h1_executors_flipped_entry.getValue());
                        LOG.info("this component name {}, number of executors {}", h1_executors_flipped_entry.getKey(), executors_flipped.get(h1_executors_flipped_entry.getKey()).intValue());
                        if (executors_flipped.get(h1_executors_flipped_entry.getKey()).intValue() !=  h1_executors_flipped_entry.getValue()) {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
            }
            return true;
        }
        return false;
    }
}