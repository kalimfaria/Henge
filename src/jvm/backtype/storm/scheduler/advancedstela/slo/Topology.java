package backtype.storm.scheduler.advancedstela.slo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

public class Topology implements Comparable<Topology> {

    private static final Logger LOG = LoggerFactory.getLogger(Topology.class);
    private static final Integer SLO_WINDOW = 10;
    public static final Integer MULTIPLIER = 10;
    public static final Integer MAX_EXECUTORS = 100;
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
    private Double ninetyNineLatency, seventyFiveLatency, fiftyLatency;
    private Long numWorkers;

    public Topology(String topologyId, Double slo, Double latency_slo, Double utility, Long numWorkers) {
        id = topologyId;

        measuredSLOs = new LinkedList<Double>();
        spouts = new HashMap<String, Component>();
        bolts = new HashMap<String, Component>();
        measuredLatency = new LinkedList<Double>();

        latencies = new HashMap<HashMap<String, String>, Double>();
        fiftyLatency = seventyFiveLatency = ninetyNineLatency = averageLatency = 0.0;
        sortingStrategy = "ascending-current-utility";
        this.numWorkers = numWorkers;

        userSpecifiedSLO = slo;
        userSpecifiedLatencySLO = latency_slo;
        this.utility = utility;
        if (slo > 0.0001 && latency_slo > 0)
            sensitivity = Sensitivity.BOTH;
        else if (slo > 0.0001)
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

    public Double get99PercentileLatency() {
        return ninetyNineLatency;
    }

    public void set99PercentileLatency(Double latency) {
        ninetyNineLatency = latency;
    }


    public Double get75PercentileLatency() {
        return seventyFiveLatency;
    }

    public void set75PercentileLatency(Double latency) {
        seventyFiveLatency = latency;
    }

    public Double get50PercentileLatency() {
        return fiftyLatency;
    }

    public void set50PercentileLatency(Double latency) {
        fiftyLatency = latency;
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
            case "ascending-current-utility":
                return ascendingCurrentUtility(other);
            case "descending-current-utility":
                return descendingCurrentUtility(other);
            case "ascending-specified-utility":
                return ascendingSpecifiedUtility(other);
            case "descending-specified-utility":
                return descendingSpecifiedUtility(other);
            case "descending-specified-ascending-current-utility":
                return descendingSpecAscendingCurrentUtility(other);
            default:
                return 0;
        }
    }

    public int descendingSpecAscendingCurrentUtility(Topology other) {
        Double my_utility = this.getCurrentUtility(), other_utility = other.getCurrentUtility();
        Double my_specified_utility = this.getTopologyUtility(),
                other_specified_utility = other.getTopologyUtility();
        // if not equal, sort by specified utility only
        System.out.println(my_utility + " " + other_utility + " " + my_specified_utility + " " + other_specified_utility + " " + my_utility.compareTo(other_utility) + " " +  my_specified_utility.compareTo(other_specified_utility));
        if (my_specified_utility.compareTo(other_specified_utility) != 0) return other_specified_utility.compareTo(my_specified_utility); // desc order by utility
        return my_utility.compareTo(other_utility); // ascending order by current utility

    }

    public int ascendingCurrentUtility(Topology other) {
        Double my_utility = this.getCurrentUtility(), other_utility = other.getCurrentUtility();
        return my_utility.compareTo(other_utility);
    }

    public int descendingCurrentUtility(Topology other) {
        Double my_utility = this.getCurrentUtility(), other_utility = other.getCurrentUtility();
        return other_utility.compareTo(my_utility);
    }

    public int ascendingSpecifiedUtility(Topology other) {
        Double my_utility = this.getTopologyUtility(),
                other_utility = other.getTopologyUtility();
        return my_utility.compareTo(other_utility);
    }

    public int descendingSpecifiedUtility(Topology other) {
        Double my_utility = this.getTopologyUtility(),
                other_utility = other.getTopologyUtility();
        return other_utility.compareTo(my_utility);
    }

    public boolean sloViolated() { // rounding to nearest 0.95
        if (sensitivity != null) {
            if (sensitivity == Sensitivity.JUICE) {
                return getMeasuredSLO() < userSpecifiedSLO  ;
            } else if (sensitivity == Sensitivity.LATENCY) {
                return (getAverageLatency() > userSpecifiedLatencySLO );
            } else if (sensitivity == Sensitivity.BOTH)
                return (getMeasuredSLO() < userSpecifiedSLO || getAverageLatency() > userSpecifiedLatencySLO);
        }
        return getMeasuredSLO() < userSpecifiedSLO;
    }

    public Double getLatencyUtility() {
        Double utilityOfLatencySLO = userSpecifiedLatencySLO / (getAverageLatency());
        if (utilityOfLatencySLO > 1) utilityOfLatencySLO = 1.0;
        LOG.info("user spec latency SLO {} average latency {} topology {} utility {} calcutility {}", userSpecifiedLatencySLO, getAverageLatency(), id, utility, utilityOfLatencySLO * utility);
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
        LOG.info("Sensitivity: {} {} ", id, sensitivity);
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

    public Integer getExecutorsForRebalancing() {
        Double util = utility;
        Double curr_util = getCurrentUtility();
        if (curr_util > util) return 0;
        Double prop = ((util - curr_util)/util);
        System.out.println("Prop " + prop);
        Double execs = Math.ceil(prop * (MULTIPLIER * 1.0));
        System.out.println("Execs " + execs);
        Integer executors = Math.min(MAX_EXECUTORS, execs.intValue());
        System.out.println("Executors " + executors);
        if (executors == 0) return 1;
        return executors;
    }

}