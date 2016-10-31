package backtype.storm.scheduler.advancedstela.slo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

public class Topology implements Comparable<Topology> {

    private static final Logger LOG = LoggerFactory.getLogger(Topology.class);
    private static final Integer SLO_WINDOW = 30;
    static public String sortingStrategy;
    public HashMap<HashMap<String, String>, Double> latencies;
    private Sensitivity sensitivity;
    private String id;
    private Double userSpecifiedSLO;
    private Double utility;
    private Double userSpecifiedLatencySLO;
    private Queue<Double> measuredSLOs;
    private Queue<Double> measuredLatency;
    private HashMap<String, Component> spouts;
    private HashMap<String, Component> bolts;
    private Double averageLatency;
    private Double tailLatency;
    private Long numWorkers;
    private File same_top;

    public Topology(String topologyId, Double slo, Double latency_slo, Double utility, Long numWorkers) {
        id = topologyId;

        measuredSLOs = new LinkedList<Double>();
        spouts = new HashMap<String, Component>();
        bolts = new HashMap<String, Component>();
        measuredLatency = new LinkedList<Double>();

        latencies = new HashMap<HashMap<String, String>, Double>();
        same_top = new File("/tmp/same_top.log");
        tailLatency = Double.MAX_VALUE;
        averageLatency = 0.0;
        sortingStrategy = "ascending";
        this.numWorkers = numWorkers;

        userSpecifiedSLO = slo;
        userSpecifiedLatencySLO = latency_slo;
        this.utility = utility;
        if (slo > 0 && latency_slo > 0)
            sensitivity = Sensitivity.BOTH;
        else if (slo > 0)
            sensitivity = Sensitivity.JUICE;
        else if (latency_slo > 0)
            sensitivity = Sensitivity.LATENCY;
        else {
            sensitivity = Sensitivity.NONE;
            LOG.info("No sensitivities are specified");
        }
    }

    public Long getWorkers() {
        return numWorkers;
    }

    public void setWorkers(Long numWorkers) {
        this.numWorkers = numWorkers;
    }

    public Sensitivity getSensitivity() {
        return sensitivity;
    }

    public Double getTailLatency() {
        return tailLatency;
    }

    public void setTailLatency(Double latency) {
        tailLatency = latency;
    }

    public Double getAverageLatency() {
        return averageLatency;
    }

    public void setAverageLatency(Double latency) {
        averageLatency = latency;

        if (measuredLatency.size() == SLO_WINDOW) {
            measuredLatency.remove();
        }
        measuredLatency.add(latency);

    }

    public String getId() {
        return id;
    }

    public Double getUserSpecifiedSLO() {
        return userSpecifiedSLO;
    }

    public Double getUserSpecifiedLatencySLO() {
        return userSpecifiedLatencySLO;
    }

    public Double getTopologyUtility() {
        return utility;
    }

    public void setMeasuredSLOs(Double value) {
        if (measuredSLOs.size() == SLO_WINDOW) {
            measuredSLOs.remove();
        }
        measuredSLOs.add(value);
    }

    public Double getMeasuredSLO() {
        double result = 0.0;
        for (Double value : measuredSLOs) {
            result += value;
        }
        return measuredSLOs.size() == 0 ? 0.0 : (result / measuredSLOs.size());
    }

    public boolean allReadingsViolateSLO() {

        for (Double value : measuredSLOs) {
            if (value > userSpecifiedSLO)
                return false;
        }

        return true;
    }

    public boolean allLatencyReadingsViolateSLO() {

        for (Double value : measuredLatency) {
            if (value < userSpecifiedLatencySLO)
                return false;
        }
        return true;
    }

    public void addSpout(String id, Component component) {
        spouts.put(id, component);
    }

    public HashMap<String, Component> getSpouts() {
        return spouts;
    }

    public void addBolt(String id, Component component) {
        bolts.put(id, component);
    }

    public HashMap<String, Component> getBolts() {
        return bolts;
    }

    public HashMap<String, Component> getAllComponents() {
        HashMap<String, Component> components = new HashMap<String, Component>(

        );
        components.putAll(spouts);
        components.putAll(bolts);
        return components;
    }


    public int compareTo(Topology other) {
        switch (sortingStrategy) {
            case "ascending":
                return ascending(other);
            case "descending":
                return descending(other);
            default:
                return 0;
        }
    }

    public int ascending(Topology other) {
        Double my_utility = this.getCurrentUtility(), other_utility = other.getCurrentUtility();
        return my_utility.compareTo(other_utility);
    }

    public int descending(Topology other) {
        Double my_utility = this.getCurrentUtility(), other_utility = other.getCurrentUtility();
        return other_utility.compareTo(my_utility);
    }

    public boolean sloViolated() {
        if (sensitivity != null) {
            if (sensitivity == Sensitivity.JUICE) {
                return getMeasuredSLO() < userSpecifiedSLO;
            } else if (sensitivity == Sensitivity.LATENCY) {
                return (getAverageLatency() > userSpecifiedLatencySLO);
            } else if (sensitivity == Sensitivity.BOTH)
                return (getMeasuredSLO() < userSpecifiedSLO || getAverageLatency() > userSpecifiedLatencySLO);
        }
        return getMeasuredSLO() < userSpecifiedSLO;
    }

    public Double getLatencyUtility() {
        Double utilityOfLatencySLO = userSpecifiedLatencySLO / (getAverageLatency());
        if (utilityOfLatencySLO > 1) utilityOfLatencySLO = 1.0;

        return utilityOfLatencySLO * utility;
    }

    public Double getThroughputUtility() {
        Double utilityOfThroughputSLO = (getMeasuredSLO()) / userSpecifiedSLO;
        if (utilityOfThroughputSLO > 1) utilityOfThroughputSLO = 1.0;

        return utilityOfThroughputSLO * utility;
    }

    public Sensitivity whichSLOToSatisfyFirst () {
        if (sensitivity == Sensitivity.BOTH) {
            Double measuredJuiceSLO = getMeasuredSLO();
            Double averageLatency = getAverageLatency();
            if (measuredJuiceSLO < userSpecifiedSLO && averageLatency > userSpecifiedLatencySLO) {
                Double juiceDiff = userSpecifiedSLO - measuredJuiceSLO; // this is already going to be normalized
                Double latencyDiff = (averageLatency - userSpecifiedLatencySLO)/userSpecifiedLatencySLO; // normalized
                if (latencyDiff > juiceDiff) return Sensitivity.JUICE; // easier to fulfill
                else return Sensitivity.LATENCY;
            } else if (averageLatency > userSpecifiedLatencySLO) {
                return Sensitivity.LATENCY;
            } else if (measuredJuiceSLO < userSpecifiedSLO) {
                return  Sensitivity.JUICE;
            }
            return Sensitivity.LATENCY;
        }
        else return null;
    }

    public Double getCurrentUtility() {
        if (sensitivity == Sensitivity.BOTH) {
            Double latencyUtility = getLatencyUtility();
            Double throughputUtility = getThroughputUtility();
            return (0.5 * latencyUtility + 0.5 * throughputUtility);

        } else if (sensitivity == Sensitivity.JUICE) {
            return getThroughputUtility();

        } else if (sensitivity == Sensitivity.LATENCY) {
            return getLatencyUtility();
        }
        return 0.0;
    }

    public void writeToFile(File file, String data) {
        try {
            FileWriter fileWriter = new FileWriter(file, true);
            BufferedWriter bufferWriter = new BufferedWriter(fileWriter);
            bufferWriter.append(data);
            bufferWriter.close();
            fileWriter.close();
        } catch (IOException ex) {
            LOG.info(ex.toString());
        }
    }
}