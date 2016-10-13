package backtype.storm.scheduler.advancedstela;


import backtype.storm.generated.StormTopology;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.advancedstela.slo.Topology;

import java.util.HashMap;
import java.util.Map;

public class History {
    private HashMap<String, Topology> topologiesPerformance;
    private HashMap<String, TopologyDetails> topologySchedules;

    public History () {

    }

    public HashMap<String, Topology> getTopologiesPerformance () {
        return topologiesPerformance;
    }

    public HashMap<String, TopologyDetails> getTopologiesSchedule () {
        return topologySchedules;
    }

    public History (Map<String, TopologyDetails> schedule, HashMap<String, Topology> performance) {
        topologiesPerformance = new HashMap<>();
        topologySchedules = new HashMap<>();

        // copy schedules first
        for (Map.Entry<String, TopologyDetails> scheduleEntry : schedule.entrySet()) {
            String topologyName = scheduleEntry.getKey();
            HashMap topologyConf = new HashMap<>();
            topologyConf.putAll(scheduleEntry.getValue().getConf());
            StormTopology topology = scheduleEntry.getValue().getTopology();
            int numWorkers = scheduleEntry.getValue().getNumWorkers();
            Map<ExecutorDetails, String> executorToComponents = new HashMap<>();

            // copied over executor to component
            for (Map.Entry<ExecutorDetails, String> executorToComponent :  scheduleEntry.getValue().getExecutorToComponent().entrySet()) {
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
                    temp.getSensitivity(),
                    temp.getWorkers());
            topology.setMeasuredSLOs(temp.getMeasuredSLO());
            topology.setAverageLatency(temp.getAverageLatency());
            topologiesPerformance.put(name, topology);
        }
    }
}
