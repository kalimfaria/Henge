package backtype.storm.scheduler.advancedstela.etp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.advancedstela.slo.Topology;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

public class LatencyStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(GlobalState.class);

    private String id;
    private TopologySchedule topologySchedule;
    private TopologyStatistics topologyStatistics;
    private Topology topo;
    private HashMap<String, Double> componentEmitRates;
    private HashMap<String, Double> componentExecuteRates;
    private TreeMap<String, Double> expectedEmitRates;
    private TreeMap<String, Double> expectedExecutedRates;
    private HashMap<String, Integer> parallelism;
    private ArrayList<Component> sourceList;
    private ArrayList<Component> sinkList;
    private HashMap<Component, Double> congestionMap;
    private HashMap<Component, Double> topologyETPMap;
    //private HashMap<Component, HashMap<ArrayList<Component>, Double>> uncongestedPaths;
    private HashMap<Component, HashMap<ArrayList<Component>, Double>> pathCollection; //indexed by each component - points to all paths that lead to this sink that pass by this component
    private HashMap<Component, Double> etpLatencyMap;
    private File latency_log;


    public LatencyStrategy(TopologySchedule tS, TopologyStatistics tStats, Topology t) {
        id = tS.getId();
        topologySchedule = tS;
        topologyStatistics = tStats;
        topo = t;
        componentEmitRates = new HashMap<String, Double>();
        componentExecuteRates = new HashMap<String, Double>();
        parallelism = new HashMap<String, Integer>();
        congestionMap = new HashMap<Component, Double>();
        expectedEmitRates = new TreeMap<String, Double>();
        expectedExecutedRates = new TreeMap<String, Double>();
        sourceList = new ArrayList<Component>();
        sinkList = new ArrayList<Component>();
        topologyETPMap = new HashMap<Component, Double>();
        //uncongestedPaths = new HashMap<Component, HashMap<ArrayList<Component>, Double>>();
        pathCollection = new HashMap<Component, HashMap<ArrayList<Component>, Double>>();
        etpLatencyMap = new HashMap<Component, Double>();
        latency_log = new File("/tmp/ETPlatency.log");
    }

    public ArrayList<ResultComponent> topologyETPRankDescending() { //used by targets
    	String topologyId = topologySchedule.getId();
    	writeToFile(latency_log, "------Calculating Component Descending Map for:"+ topologyId + "-------" + "\n");
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
        //print topologyETP
        writeToFile(latency_log, "------Topology ETP Map for "+ topologySchedule.getId() + "-------" + "\n");
        for(Entry<Component, Double> e: topologyETPMap.entrySet()){
        	writeToFile(latency_log, e.getKey().getId()+"->"+ e.getValue() +"\n");
        }
        
        writeToFile(latency_log, "+++++++++ Path Collection +++++++" + "\n");
        //calculate all path from source to sink for each component
        HashMap<ArrayList<Component>, Double> compLatencyMap = new HashMap<ArrayList<Component>, Double>();
        for (Component component : topologySchedule.getComponents().values()) {
        	writeToFile(latency_log, "------Path Collection for Component: "+ component.getId()+ "-------" + "\n");
        	//populate pathCollectionMap 
        	ArrayList<ArrayList<Component>> downStreams = downstreamTracking(component);
        	ArrayList<ArrayList<Component>> upStreams = upstreamTracking(component);	
        	//populate compLatencyMap
        	for(ArrayList<Component> downStream : downStreams){
        		for(ArrayList<Component> upStream : upStreams){
        			writeToFile(latency_log, "Path(downstream):\n");
        			for(Component c: downStream){
        	        	writeToFile(latency_log, c.getId()+"->");
        	        }
        			writeToFile(latency_log, "\n");
                    writeToFile(latency_log, "Path(upstream):\n");
                    for(Component c: upStream){
        	        	writeToFile(latency_log, c.getId()+"->");
        	        }
                    writeToFile(latency_log, upStream+"\n");
            		Double totalLatency =0.0;
            		Component head = upStream.get(upStream.size()-1);
            		Component tail = downStream.get(0);
            		HashMap<String, String> head_tail = new HashMap<String, String>();
            		head_tail.put(head.getId(), tail.getId());
            		totalLatency = this.topo.latencies.get(head_tail);
            		writeToFile(latency_log, "\n Total Path Latency: "+totalLatency+"\n");
            		ArrayList<Component> newpath = new ArrayList<Component>();
            		newpath.addAll(downStream);
            		newpath.addAll(upStream);
            		writeToFile(latency_log, "New Path:\n");
        			for(Component c:newpath){
        	        	writeToFile(latency_log, c.getId()+"->");
        	        }
        			writeToFile(latency_log, "\n");
            		compLatencyMap.put(newpath, totalLatency);
        		}
        		this.pathCollection.put(component, compLatencyMap);
        		
        	}
        }
        
        
        //populate etpLatencyMap
        for (Component component : topologySchedule.getComponents().values()) {
        	HashMap<Component, Integer>  sinkCount = new HashMap<Component, Integer>();
        	HashMap<Component, Double> sinkTotalLatency = new HashMap<Component, Double>();
        	compLatencyMap = this.pathCollection.get(component);
        	for(ArrayList<Component> path : compLatencyMap.keySet()){
        		Component sink = path.get(0);
        		if(!sinkCount.containsKey(sink)){
        			sinkCount.put(sink, 1);
        			sinkTotalLatency.put(sink, compLatencyMap.get(path));
        		}
        		else{
        			sinkCount.put(sink, sinkCount.get(sink)+1);
        			sinkTotalLatency.put(sink, sinkTotalLatency.get(sink)+compLatencyMap.get(path));
        		}
        	}
        	
        	Double etpLatencyScore =0.0;
        	for(Component sink: sinkCount.keySet()){
        		//take an average 
        		etpLatencyScore += sinkTotalLatency.get(sink)/sinkCount.get(sink)*topologyETPMap.get(sink);
        	}
        	this.etpLatencyMap.put(component, etpLatencyScore);
        	writeToFile(latency_log, "=== Component: "+ component.getId()+ ", ETPLatency Score: " + etpLatencyScore+"===\n");
        }
        
        


        ArrayList<ResultComponent> resultComponents = new ArrayList<ResultComponent>();
        for (Component component: etpLatencyMap.keySet()) {
        	if(this.congestionMap.containsKey(component)){
        		//only benefiting congested component
        		Long curTime = System.currentTimeMillis();
        		if(curTime-component.getLastRebalancedAt()>300000){
        			resultComponents.add(new ResultComponent(component, etpLatencyMap.get(component)));
        			component.setLastRebalancedAt(curTime);
        		}
        		
        	}
            
        }

        Collections.sort(resultComponents, Collections.reverseOrder());
        //print descending list
        writeToFile(latency_log, "=== Descending List ===\n");
        for(ResultComponent r:resultComponents){
        	writeToFile(latency_log, r.component.getId()+":"+r.etpValue+"->");
        }
        writeToFile(latency_log, "\n");
        return resultComponents;
    }

    public ArrayList<ResultComponent> topologyETPRankAscending() { //used by victims
    	String topologyId = topologySchedule.getId();
    	writeToFile(latency_log, "------Calculating Component Descending Map for:"+ topologyId + "-------" + "\n");
    	collectRates();
        congestionDetection();

        Double totalThroughput = 0.0;
        for (Component component: topologySchedule.getComponents().values()) {
            if (component.getChildren().size() == 0) {
                totalThroughput += expectedEmitRates.get(component.getId());
            }
        }

        if (totalThroughput == 0.0) {
        	//LOG.info("Nothing to do as throughput is 0.");
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
        
        //print topologyETP
        writeToFile(latency_log, "------Topology ETP Map for "+ topologySchedule.getId() + "-------" + "\n");
        for(Entry<Component, Double> e: topologyETPMap.entrySet()){
        	writeToFile(latency_log, e.getKey().getId()+"->"+ e.getValue() +"\n");
        }    
        
        writeToFile(latency_log, "+++++++++ Path Collection +++++++" + "\n");
        //calculate uncongestedPath for each component
        HashMap<ArrayList<Component>, Double> compLatencyMap = new HashMap<ArrayList<Component>, Double>();
        for (Component component : topologySchedule.getComponents().values()) {
        	writeToFile(latency_log, "------Path Collection for Component: "+ component.getId()+ "-------" + "\n");
        	//populate pathCollectionMap 
        	ArrayList<ArrayList<Component>> downStreams = downstreamTracking(component);
        	ArrayList<ArrayList<Component>> upStreams = upstreamTracking(component);	
        	//populate compLatencyMap
        	for(ArrayList<Component> downStream : downStreams){
        		for(ArrayList<Component> upStream : upStreams){
        			writeToFile(latency_log, "Path(downstream):\n");
        			for(Component c: downStream){
        	        	writeToFile(latency_log, c.getId()+"->");
        	        }
        			writeToFile(latency_log, "\n");
                    writeToFile(latency_log, "Path(upstream):\n");
                    for(Component c: upStream){
        	        	writeToFile(latency_log, c.getId()+"->");
        	        }
                    writeToFile(latency_log, upStream+"\n");
            		Double totalLatency =0.0;
            		Component head = upStream.get(upStream.size()-1);
            		Component tail = downStream.get(0);
            		HashMap<String, String> head_tail = new HashMap<String, String>();
            		head_tail.put(head.getId(), tail.getId());
            		totalLatency = this.topo.latencies.get(head_tail);
            		writeToFile(latency_log, "\n Total Path Latency: "+totalLatency+"\n");
            		ArrayList<Component> newpath = new ArrayList<Component>();
            		newpath.addAll(downStream);
            		newpath.addAll(upStream);
            		writeToFile(latency_log, "New Path:\n");
        			for(Component c:newpath){
        	        	writeToFile(latency_log, c.getId()+"->");
        	        }
        			writeToFile(latency_log, "\n");
            		compLatencyMap.put(newpath, totalLatency);
        		}
        		this.pathCollection.put(component, compLatencyMap);
        		
        	}
        }
        		
        
        //populate etpLatencyMap
        for (Component component : topologySchedule.getComponents().values()) {
        	HashMap<Component, Integer>  sinkCount = new HashMap<Component, Integer>();
        	HashMap<Component, Double> sinkTotalLatency = new HashMap<Component, Double>();
        	compLatencyMap = this.pathCollection.get(component);
        	for(ArrayList<Component> path : compLatencyMap.keySet()){
        		Component sink = path.get(0);
        		if(!sinkCount.containsKey(sink)){
        			sinkCount.put(sink, 1);
        			sinkTotalLatency.put(sink, compLatencyMap.get(path));
        		}
        		else{
        			sinkCount.put(sink, sinkCount.get(sink)+1);
        			sinkTotalLatency.put(sink, sinkTotalLatency.get(sink)+compLatencyMap.get(path));
        		}
        	}
        	
        	Double etpLatencyScore =0.0;
        	for(Component sink: sinkCount.keySet()){
        		//take an average 
        		etpLatencyScore += sinkTotalLatency.get(sink)/sinkCount.get(sink)*topologyETPMap.get(sink);
        	}
        	writeToFile(latency_log, "=== Component: "+ component.getId()+ ", ETPLatency Score: " + etpLatencyScore+"===\n");
        	this.etpLatencyMap.put(component, etpLatencyScore);
        }
           


        ArrayList<ResultComponent> resultComponents = new ArrayList<ResultComponent>();
        for (Component component: etpLatencyMap.keySet()) {
        	if(!this.congestionMap.containsKey(component)){//if not congested
		    	Long curTime = System.currentTimeMillis();
				if(curTime-component.getLastRebalancedAt()>300000){
					resultComponents.add(new ResultComponent(component, etpLatencyMap.get(component)));
					component.setLastRebalancedAt(curTime);
				}
        	}
        }

        Collections.sort(resultComponents);
        writeToFile(latency_log, "=== Ascending List ===\n");
        for(ResultComponent r:resultComponents){
        	writeToFile(latency_log, r.component.getId()+":"+r.etpValue+"->");
        }
        writeToFile(latency_log, "\n");
        return resultComponents;
    }

    private ArrayList<ArrayList<Component>> upstreamTracking(Component component) {
		// TODO Auto-generated method stub
    	ArrayList<ArrayList<Component>> ret = new ArrayList<ArrayList<Component>>();
        if (component.getParents().size() == 0) {
        	//add an entry to the uncongested path
        	ret.add(new ArrayList<Component>());
        	ret.get(ret.size()-1).add(component);
            return ret;
        }

        HashMap<String, Component> components = topologySchedule.getComponents();
        for (String p : component.getParents()) {
            Component parent = components.get(p);
            ret = upstreamTracking(parent); 
            ret.get(ret.size()-1).add(component);
        }

        return ret;
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
        
        //print congestion map
        writeToFile(latency_log, "------Congestion Map for "+ topologySchedule.getId() + "-------" + "\n");
        for(Entry<Component, Double> e: congestionMap.entrySet()){
        	writeToFile(latency_log, "->"+ e.getKey().getId());
        }
        writeToFile(latency_log, "\n");
    }

    private Double etpCalculation(Component component, HashMap<String, Double> sinksMap) {
        Double ret = 0.0;
        if (component.getChildren().size() == 0) {
        	//add an entry to the uncongested path
            return sinksMap.get(component.getId());
        }

        HashMap<String, Component> components = topologySchedule.getComponents();
        for (String c : component.getChildren()) {
            Component child = components.get(c);
            if (congestionMap.get(child)==null) {
                ret = ret + etpCalculation(child, sinksMap);
            }
        }

        return ret;
    }
    
    private ArrayList<ArrayList<Component>> downstreamTracking(Component component) {
        //int ret = -1;
    	ArrayList<ArrayList<Component>> ret = new ArrayList<ArrayList<Component>>();
        if (component.getChildren().size() == 0) {
        	//add an entry to the uncongested path
        	ret.add(new ArrayList<Component>());
        	ret.get(ret.size()-1).add(component);
            return ret;
        }

        HashMap<String, Component> components = topologySchedule.getComponents();
        for (String c : component.getChildren()) {
            Component child = components.get(c);
            ret = downstreamTracking(child); 
            ret.get(ret.size()-1).add(component);
        }

        return ret;
    }

    private void collectRates() {
        for (Map.Entry<String, List<Integer>> emitThroughput : topologyStatistics.getEmitThroughputHistory().entrySet()) {
            componentEmitRates.put(emitThroughput.getKey(), computeMovingAverage(emitThroughput.getValue()));
        }

        expectedEmitRates.putAll(componentEmitRates);
        
        //print
        writeToFile(latency_log, "------Emit Rate Map for "+ topologySchedule.getId() + "-------" + "\n");
        for(Entry<String, Double> e: expectedEmitRates.entrySet()){
        	writeToFile(latency_log, e.getKey()+"->"+ e.getValue() +"\n");
        }
        
        for (Map.Entry<String, List<Integer>> executeThroughput : topologyStatistics.getExecuteThroughputHistory().entrySet()) {
            componentExecuteRates.put(executeThroughput.getKey(), computeMovingAverage(executeThroughput.getValue()));
        }
        expectedExecutedRates.putAll(componentExecuteRates);
        
        //print
        writeToFile(latency_log, "------Execution Rate Map for "+ topologySchedule.getId() + "-------" + "\n");      
        for(Entry<String, Double> e: expectedExecutedRates.entrySet()){
        	writeToFile(latency_log, e.getKey()+"->"+ e.getValue() +"\n");
        }

        /**------not using window right now, maybe later----**/
        	
        for (Map.Entry<String, Component> component : topologySchedule.getComponents().entrySet()) {
            parallelism.put(component.getKey(), component.getValue().getParallelism());
        }
        //print
        writeToFile(latency_log, "------Parallelism Map for "+ topologySchedule.getId() + "-------" + "\n");      
        for(Entry<String, Integer> e: parallelism.entrySet()){
        	writeToFile(latency_log, e.getKey()+"->"+ e.getValue() +"\n");
        }      
        
        for (Component component : topologySchedule.getComponents().values()) {
            if (component.getParents().size() == 0) {
                sourceList.add(component);
            }
        }
        //print
        writeToFile(latency_log, "------Source List for "+ topologySchedule.getId() + "-------" + "\n");      
        for(Component c: sourceList){
        	writeToFile(latency_log, c+"->"+ c.getId());
        }
        writeToFile(latency_log, "\n");
        
        for (Component component : topologySchedule.getComponents().values()) {
            if (component.getChildren().size() == 0) {
                sinkList.add(component);
            }
        }
        //print
        writeToFile(latency_log, "------Sink List for "+ topologySchedule.getId() + "-------" + "\n");      
        for(Component c: sinkList){
        	writeToFile(latency_log, c+"->"+ c.getId());
        }
        writeToFile(latency_log, "\n");
        
    }

    private Double computeMovingAverage(List<Integer> rates) {
        Double sum = 0.0;
        for (Integer val : rates) {
            sum += val;
        }
        return sum / (rates.size() * 1.0);
    }
    
    public void writeToFile(File file, String data) {
        try {
            FileWriter fileWritter = new FileWriter(file, true);
            BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
            bufferWritter.append(data);
            bufferWritter.close();
            fileWritter.close();
        } catch (IOException ex) {
          System.out.println(ex.toString());
        }
    }

	
}
