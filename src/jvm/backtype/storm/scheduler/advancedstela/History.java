package backtype.storm.scheduler.advancedstela;

import backtype.storm.generated.StormTopology;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.advancedstela.slo.Sensitivity;
import backtype.storm.scheduler.advancedstela.slo.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class History implements Comparable {
    private HashMap<String, Topology> topologiesPerformance;
    private HashMap<String, TopologyDetails> topologySchedules;
    private Double systemUtility;
    private ArrayList<String> latencySensitiveTopologiesDontMeetSLOs;
    private ArrayList<String> throughputSensitiveTopologiesDontMeetSLOs;
    private ArrayList<String> latencyAndThroughputSensitiveTopologiesDontMeetSLOs;
    private static final Logger LOG = LoggerFactory.getLogger(History.class);

    public boolean doWeNeedToRevert (History current) {
        Double currentUtility = 0.0, oldUtility = 0.0;
        for (Map.Entry<String,Topology> t : topologiesPerformance.entrySet()) {
            oldUtility += t.getValue().getCurrentUtility() ;
        }

        for (Map.Entry<String,Topology> t : current.topologiesPerformance.entrySet()) {
            currentUtility += t.getValue().getCurrentUtility() ;
        }
        if (oldUtility > currentUtility) {// new history has fewer LS topologies that meet SLO
            return true;
        }
        return false;
    }

    public HashMap<String, Topology> getTopologiesPerformance () {
        return topologiesPerformance;
    }

    public HashMap<String, TopologyDetails> getTopologiesSchedule () {
        return topologySchedules;
    }

    public ArrayList<String> getLSTDontMeetSLOs () { return latencySensitiveTopologiesDontMeetSLOs; }

    public ArrayList<String> getLTSTDontMeetSLOs () { return latencyAndThroughputSensitiveTopologiesDontMeetSLOs; }

    public ArrayList<String> getTSTDontMeetSLOs () {
        return throughputSensitiveTopologiesDontMeetSLOs;
    }

    public History (Map<String, TopologyDetails> schedule,
                    HashMap<String, Topology> performance,
                    ArrayList<Topology> receiver_topologies) {
        topologiesPerformance = new HashMap<>();
        topologySchedules = new HashMap<>();
        latencySensitiveTopologiesDontMeetSLOs = new ArrayList<>();
        throughputSensitiveTopologiesDontMeetSLOs = new ArrayList<>();
        latencyAndThroughputSensitiveTopologiesDontMeetSLOs = new ArrayList<>();
        systemUtility = 0.0;

        for (Map.Entry<String, Topology> topology: performance.entrySet()) {
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
                    temp.getTopologyUtility(),
                    temp.getWorkers());
            topology.setMeasuredSLOs(temp.getMeasuredSLO());
            topology.setAverageLatency(temp.getAverageLatency());
            topologiesPerformance.put(name, topology);
        } //

        for (Topology r: receiver_topologies) {
            if (r.getSensitivity() == Sensitivity.LATENCY) {
                latencySensitiveTopologiesDontMeetSLOs.add(r.getId());
            } else if (r.getSensitivity() == Sensitivity.JUICE) {
                throughputSensitiveTopologiesDontMeetSLOs.add(r.getId());
            }
            else if (r.getSensitivity() == Sensitivity.BOTH) {
                latencyAndThroughputSensitiveTopologiesDontMeetSLOs.add(r.getId());
            }
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

}
