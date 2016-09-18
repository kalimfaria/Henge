package backtype.storm.scheduler.advancedstela.etp;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ETPStrategy {
    private String id;

    private static final Logger LOG = LoggerFactory.getLogger(GlobalState.class);
    private TopologySchedule topologySchedule;
    private TopologyStatistics topologyStatistics;
    private HashMap<String, Double> componentEmitRates;
    private HashMap<String, Double> componentExecuteRates;
    private TreeMap<String, Double> expectedEmitRates;
    private TreeMap<String, Double> expectedExecutedRates;
    private HashMap<String, Integer> parallelism;
    private ArrayList<Component> sourceList;
    private ArrayList<Component> sinkList;
    private HashMap<Component, Double> congestionMap;
    private HashMap<Component, Double> topologyETPMap;



    public ETPStrategy(TopologySchedule tS, TopologyStatistics tStats) {
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
        sinkList = new ArrayList<Component>();
        topologyETPMap = new HashMap<Component, Double>();

    }

    public ArrayList<ResultComponent> topologyETPRankDescending() { //used by targets
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

        for (Component component : topologySchedule.getComponents().values()) {
            Double score = etpCalculation(component, sinksMap);
            topologyETPMap.put(component, score);
        }

        ArrayList<ResultComponent> resultComponents = new ArrayList<ResultComponent>();
        for (Component component: topologyETPMap.keySet()) {
        	if(this.congestionMap.containsKey(component)){
        		//only benefiting congested component
        		Long curTime = System.currentTimeMillis();
        		if(curTime-component.getLastRebalancedAt()>300000){
        			resultComponents.add(new ResultComponent(component, topologyETPMap.get(component)));
        			//component.setLastRebalancedAt(curTime);
        		}
        	}
        }

        Collections.sort(resultComponents, Collections.reverseOrder());
        return resultComponents;
    }

    public ArrayList<ResultComponent> topologyETPRankAscending() { //used by victims
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

        ArrayList<ResultComponent> resultComponents = new ArrayList<ResultComponent>();
        for (Component component: topologyETPMap.keySet()) {
        	if(!this.congestionMap.containsKey(component)){ 
		    	Long curTime = System.currentTimeMillis();
				if(curTime-component.getLastRebalancedAt()>300000){
					resultComponents.add(new ResultComponent(component, topologyETPMap.get(component)));
					component.setLastRebalancedAt(curTime);
				}
        	}
        }
        

        Collections.sort(resultComponents);
        return resultComponents;
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

            if (in >  1.2 * out) {
                Double io = in - out;
                congestionMap.put(self, io);
            }
        }
    }

    private Double etpCalculation(Component component, HashMap<String, Double> sinksMap) {
        Double ret = 0.0;
        if (component.getChildren().size() == 0) {
            return sinksMap.get(component.getId());
        }

        HashMap<String, Component> components = topologySchedule.getComponents();
        for (String c : component.getChildren()) {
            Component child = components.get(c);
            if (!congestionMap.containsKey(child)) {
                ret = ret + etpCalculation(child, sinksMap);
            }
        }

        return ret;
    }

    private void collectRates() {
        for (Map.Entry<String, List<Integer>> emitThroughput : topologyStatistics.getEmitThroughputHistory().entrySet()) {
            componentEmitRates.put(emitThroughput.getKey(), computeMovingAverage(emitThroughput.getValue()));
        }

        expectedEmitRates.putAll(componentEmitRates);

        for (Map.Entry<String, List<Integer>> executeThroughput : topologyStatistics.getExecuteThroughputHistory().entrySet()) {
            componentExecuteRates.put(executeThroughput.getKey(), computeMovingAverage(executeThroughput.getValue()));
        }
        expectedExecutedRates.putAll(componentExecuteRates);

        /**------not using window right now, maybe later----**/
        	
        for (Map.Entry<String, Component> component : topologySchedule.getComponents().entrySet()) {
            parallelism.put(component.getKey(), component.getValue().getParallelism());
        }

        for (Component component : topologySchedule.getComponents().values()) {
            if (component.getParents().size() == 0) {
                sourceList.add(component);
            }
        }
        
        for (Component component : topologySchedule.getComponents().values()) {
            if (component.getChildren().size() == 0) {
                sinkList.add(component);
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

	
}
