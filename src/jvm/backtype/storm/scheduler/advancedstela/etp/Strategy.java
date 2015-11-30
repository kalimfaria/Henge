package backtype.storm.scheduler.advancedstela.etp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Strategy {
    private static final Logger LOG = LoggerFactory.getLogger(GlobalState.class);

    private String id;
    private TopologySchedule topologySchedule;
    private TopologyStatistics topologyStatistics;
    private HashMap<String, Double> componentEmitRates;
    private HashMap<String, Double> componentExecuteRates;
    private TreeMap<String, Double> expectedEmitRates;
    private TreeMap<String, Double> expectedExecutedRates;
    private HashMap<String, Integer> parallelism;
    private ArrayList<Component> sourceList;
    private HashMap<Component, Double> congestionMap;
    private HashMap<Component, Double> topologyETPMap;
    private TreeMap<Component, Double> topologyETPRankDesc;

    private TreeMap<Component, Double> topologyETPRankAsc;

    public Strategy(TopologySchedule tS, TopologyStatistics tStats) {
        id = tS.getId();
        topologySchedule = tS;
        topologyStatistics = tStats;
        componentEmitRates = new HashMap<String, Double>();
        componentExecuteRates = new HashMap<String, Double>();
        parallelism = new HashMap<String, Integer>();
        congestionMap = new HashMap<Component, Double>();
        expectedEmitRates = new TreeMap<String, Double>();
        expectedExecutedRates = new TreeMap<String, Double>();
        sourceList = new ArrayList<Component>();
        topologyETPMap = new HashMap<Component, Double>();
        ComponentComparatorDesc bvc =  new ComponentComparatorDesc(this.topologyETPMap);
        topologyETPRankDesc = new TreeMap<Component, Double>(bvc);

        ComponentComparatorAsc avc =  new ComponentComparatorAsc(this.topologyETPMap);
        topologyETPRankAsc = new TreeMap<Component, Double>(avc);
    }

    public TreeMap<Component, Double> topologyETPRankDescending() {
        collectRates();
        congestionDetection();

        Double totalThroughput = 0.0;
        for (Component component: topologySchedule.getComponents().values()) {
            if (component.getChildren().size() == 0) {
                totalThroughput += expectedEmitRates.get(component.getId());
            }
        }

        if (totalThroughput == 0.0) {
            LOG.info("Nothing to do as throughput is 0.");
            new TreeMap<>();
        }

        HashMap<String, Double> sinksMap = new HashMap<String, Double>();
        for (Component component: topologySchedule.getComponents().values()) {
            if (component.getChildren().size() == 0) {
                Double throughputOfSink = expectedEmitRates.get(component.getId());
                sinksMap.put(component.getId(), throughputOfSink / totalThroughput);
            }
        }

        //calculate ETP for each component
        LOG.info("Component Emit Rate: {}", componentEmitRates);
        LOG.info("Component Execute Rates: {}", componentExecuteRates);
        LOG.info("Congestion Map: {}", congestionMap);

        for (Component component : topologySchedule.getComponents().values()) {
            Double score = etpCalculation(component, sinksMap);
            topologyETPMap.put(component, score);
        }
        topologyETPRankDesc.putAll(topologyETPMap);
        return topologyETPRankDesc;
    }

    public TreeMap<Component, Double> topologyETPRankAscending() {
        collectRates();
        congestionDetection();

        Double totalThroughput = 0.0;
        for (Component component: topologySchedule.getComponents().values()) {
            if (component.getChildren().size() == 0) {
                totalThroughput += expectedEmitRates.get(component.getId());
            }
        }

        if (totalThroughput == 0.0) {
            LOG.info("Nothing to do as throughput is 0.");
            new TreeMap<>();
        }

        HashMap<String, Double> sinksMap = new HashMap<String, Double>();
        for (Component component: topologySchedule.getComponents().values()) {
            if (component.getChildren().size() == 0) {
                Double throughputOfSink = expectedEmitRates.get(component.getId());
                sinksMap.put(component.getId(), throughputOfSink / totalThroughput);
            }
        }

        //calculate ETP for each component
        for (Component component : topologySchedule.getComponents().values()) {
            Double score = etpCalculation(component, sinksMap);
            topologyETPMap.put(component, score);
        }
        topologyETPRankAsc.putAll(topologyETPMap);
        return topologyETPRankAsc;
    }

    private void congestionDetection() {
        HashMap<String, Component> components = topologySchedule.getComponents();
        for (Map.Entry<String, Double> componentRate : expectedExecutedRates.entrySet()) {
            Double out = componentRate.getValue();
            Double in = 0.0;

            Component self = components.get(componentRate.getKey());

            if (self.getParents().size() != 0) {
                for (String parent : self.getParents()) {
                    in += expectedEmitRates.get(parent);
                }
            }

            if (in > 1.2 * out) {
                Double io = in - out;
                congestionMap.put(self, io);
            }
        }
    }

    private Double etpCalculation(Component component, HashMap<String, Double> sinksMap) {
    	LOG.info("******ETP Calculation******");
        Double ret = 0.0;
        if (component.getChildren().size() == 0) {
        	LOG.info("Reaching component: {} is a sink", component.getId());
            return sinksMap.get(component.getId());
        }

        HashMap<String, Component> components = topologySchedule.getComponents();
        for (String c : component.getChildren()) {
        	LOG.info("Exploring component: {}", component.getId());
            Component child = components.get(c);
            if (congestionMap.get(child)==null) {
            	LOG.info("Reaching component: {} is not congested, keep exploring", component.getId());
                ret = ret + etpCalculation(child, sinksMap);
            }
        }

        return ret;
    }

    private void collectRates() {
        LOG.info("********************** collectRates ********************");
        for (Map.Entry<String, List<Integer>> emitThroughput : topologyStatistics.getEmitThroughputHistory().entrySet()) {
            componentEmitRates.put(emitThroughput.getKey(), computeMovingAverage(emitThroughput.getValue()));
        }

        LOG.info("****** Component Emit Rates ******");
        for (String key: componentEmitRates.keySet()) {
            LOG.info("Component: {}, Value: {}", key, componentEmitRates.get(key));
        }

        expectedEmitRates.putAll(componentEmitRates);

        for (Map.Entry<String, List<Integer>> executeThroughput : topologyStatistics.getExecuteThroughputHistory().entrySet()) {
            componentExecuteRates.put(executeThroughput.getKey(), computeMovingAverage(executeThroughput.getValue()));
        }
        expectedExecutedRates.putAll(componentExecuteRates);

        LOG.info("****** Component Execute Rates ******");
        for (String key: componentExecuteRates.keySet()) {
            LOG.info("Component: {}, Value: {}", key, componentEmitRates.get(key));
        }

        for (Map.Entry<String, Component> component : topologySchedule.getComponents().entrySet()) {
            parallelism.put(component.getKey(), component.getValue().getParallelism());
        }

        for (Component component : topologySchedule.getComponents().values()) {
            if (component.getParents().size() == 0) {
                sourceList.add(component);
            }
        }
    }

    private Double computeMovingAverage(List<Integer> rates) {
        Double sum = 0.0;
        for (Integer val : rates) {
            sum += val;
        }
        return sum / (rates.size() * 1.0);
    }
    
	private class ComponentComparatorDesc implements Comparator<Component> {

		HashMap<Component, Double> base;
	    public ComponentComparatorDesc(HashMap<Component, Double> base) {
	        this.base = base;
	    }

	    // Note: this comparator imposes orderings that are inconsistent with equals.
	    public int compare(Component a, Component b) {
	        if (base.get(a) >= base.get(b)) {
	            return -1;
	        } else {
	            return 1;
	        } // returning 0 would merge keys
	    }
	}

	private class ComponentComparatorAsc implements Comparator<Component> {

		HashMap<Component, Double> base;
	    public ComponentComparatorAsc(HashMap<Component, Double> base) {
	        this.base = base;
	    }

	    // Note: this comparator imposes orderings that are inconsistent with equals.
	    public int compare(Component a, Component b) {
	        if (base.get(b) >= base.get(a)) {
	            return -1;
	        } else {
	            return 1;
	        } // returning 0 would merge keys
	    }
	}
}
