package backtype.storm.scheduler.advancedstela;


import backtype.storm.generated.StormTopology;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.advancedstela.slo.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

import java.util.HashMap;
import java.util.Map;

public class History implements Comparable {
    private HashMap<String, Topology> topologiesPerformance;
    private HashMap<String, TopologyDetails> topologySchedules;
    private ArrayList<String> latencySensitiveTopologiesMeetSLOs;
    private ArrayList<String> latencySensitiveTopologiesDontMeetSLOs;
    private ArrayList<String> throughputSensitiveTopologiesMeetSLOs;
    private ArrayList<String> throughputSensitiveTopologiesDontMeetSLOs;
    private static final Logger LOG = LoggerFactory.getLogger(History.class);

    public History () {
    }


    public boolean doWeNeedToRevert (History current) {
        if (current.getLSTMeetSLOs().size() < latencySensitiveTopologiesMeetSLOs.size()) {// new history has fewer LS topologies that meet SLO
            return true;
        }
        if (current.getTSTMeetSLOs().size() + 1 < throughputSensitiveTopologiesMeetSLOs.size()) {// new history has fewer LS topologies that meet SLO
            return true;
        } // so we say that 2 TS = 1
        return false;
    }

    public HashMap<String, Topology> getTopologiesPerformance () {
        return topologiesPerformance;
    }

    public HashMap<String, TopologyDetails> getTopologiesSchedule () {
        return topologySchedules;
    }

    public ArrayList<String> getLSTDontMeetSLOs () {
        return latencySensitiveTopologiesDontMeetSLOs;
    }

    public ArrayList<String> getLSTMeetSLOs () {
        return latencySensitiveTopologiesMeetSLOs;
    }

    public ArrayList<String> getTSTDontMeetSLOs () {
        return throughputSensitiveTopologiesDontMeetSLOs;
    }

    public ArrayList<String> getTSTMeetSLOs () {
        return throughputSensitiveTopologiesMeetSLOs;
    }

    public History (Map<String, TopologyDetails> schedule,
                    HashMap<String, Topology> performance,
                    ArrayList<Topology> receiver_topologies,
                    ArrayList<Topology> giver_topologies) {
        topologiesPerformance = new HashMap<>();
        topologySchedules = new HashMap<>();
        latencySensitiveTopologiesMeetSLOs = new ArrayList<>();
        latencySensitiveTopologiesDontMeetSLOs = new ArrayList<>();
        throughputSensitiveTopologiesMeetSLOs = new ArrayList<>();
        throughputSensitiveTopologiesDontMeetSLOs = new ArrayList<>();

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
        } //

        for (Topology r: receiver_topologies) {
            if (r.getSensitivity() == "latency") {
                latencySensitiveTopologiesDontMeetSLOs.add(r.getId());
            } else{
                throughputSensitiveTopologiesDontMeetSLOs.add(r.getId());
            }
        }

        for (Topology g: giver_topologies) {
            if (g.getSensitivity() == "latency") {
                latencySensitiveTopologiesMeetSLOs.add(g.getId());
            } else{
                throughputSensitiveTopologiesMeetSLOs.add(g.getId());
            }
        }

    }

    @Override
    public int compareTo(Object o) {
        if (o instanceof History) {
            History h2 = (History)o;
            int h1LSSatisfyingSLOs = this.getLSTMeetSLOs().size();
            int h1TSSatisfyingSLOs = this.getTSTMeetSLOs().size();

            LOG.info("h1LSSatisfyingSLOs {}", h1LSSatisfyingSLOs);
            LOG.info("h1TSSatisfyingSLOs {}", h1TSSatisfyingSLOs);

            int h2LSSatisfyingSLOs = h2.getLSTMeetSLOs().size();
            int h2TSSatisfyingSLOs = h2.getTSTMeetSLOs().size();

            LOG.info("h2LSSatisfyingSLOs {}", h2LSSatisfyingSLOs);
            LOG.info("h2TSSatisfyingSLOs {}", h2TSSatisfyingSLOs);

            int LSdiff = h1LSSatisfyingSLOs - h2LSSatisfyingSLOs;
            int TSdiff = h1TSSatisfyingSLOs - h2TSSatisfyingSLOs;

            LOG.info("LSdiff {}", LSdiff);
            LOG.info("TSdiff {}", TSdiff);

            if (LSdiff > 0 && TSdiff > 0) { // More or equal LS topologies && More or equal TS topologies that satisfy the SLO
                LOG.info("solution {} ",1);
                return 1;
            }
            else if (LSdiff == 0 && TSdiff == 0) {
                LOG.info("solution {} ", 0);
                return 0;
            }
            else if (LSdiff < 0) {
                LOG.info("solution {} ", 1);
                return -1;
            }
            else if (TSdiff < 0) {
                LOG.info("solution {} ", -1);
                return -1;
            }

            LOG.info("solution {} ", 0);
            return 0; // default case
        } else {
            return -1 ; // return -1 if nothing works
        }
    }

}
