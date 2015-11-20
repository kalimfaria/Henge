package backtype.storm.scheduler.advancedstela.etp;//package backtype.storm.scheduler.advancedstela.etp;
//
//import java.awt.Component;
//import java.util.Comparator;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.TreeMap;
//
//import backtype.storm.scheduler.Cluster;
//import backtype.storm.scheduler.Topologies;
//import backtype.storm.scheduler.TopologyDetails;
//
//public class ETPStrategy {
//
//	GlobalState globalState;
//	GetStats getStats;
//	TopologyDetails topo;
//	Cluster cluster;
//	Topologies topologies;
//	HashMap<Component, Double> executeRateMap;
//	HashMap<Component, Double> emitRateMap;
//	HashMap<Component, Double> outputRateMap;
//
//	TreeMap<Component, Double> topoETPRankDesc;
//	TreeMap<Component, Double> topoETPRankAsc;
//	HashMap<Component, Double> topoETPMap;
//	HashMap<Component, Double> CongestedMap;
//	ComponentComparator bvc;
//
//
//	public ETPStrategy(GlobalState globalState, GetStats getStats,
//	TopologyDetails topo, Cluster cluster, Topologies topologies, HashMap<Component, Double> executeRateMap, HashMap<Component, Double> inputRateMap, HashMap<Component, Double> outputRateMap ){
//		this.globalState = globalState;
//		this.getStats = getStats;
//		this.topo =  topo;
//		this.cluster = cluster;
//		this.topologies = topologies;
//		this.executeRateMap = executeRateMap;
//		this.emitRateMap = emitRateMap;
//		this.outputRateMap = outputRateMap;
//		this.CongestedMap = new HashMap<Component, Double>();
//		this.topoETPMap = new HashMap<Component, Double>();
//		ComponentComparatorDesc bvc =  new ComponentComparatorDesc(this.topoETPMap);
//		this.topoETPRankDesc = new TreeMap<Component, Double>(bvc);
//		ComponentComparatorAsc avc =  new ComponentComparatorAsc(this.topoETPMap);
//		this.topoETPRankDesc = new TreeMap<Component, Double>(avc);
//	}
//
//	public TreeMap<Component, Double> TopoETPRankDesc(){
//		//get a map of congested components
//		CongestedMap = CongestionDetection();
//
//		//calculate ETP for each component
//		for(Component e: this.topo.components){
//			Double score = ETPGeneration(e);
//			topoETPMap.put(e, score);
//		}
//		this.topoETPRankDesc.putAll(topoETPMap);
//		return this.topoETPRankDesc;
//	}
//
//	public TreeMap<Component, Double> TopoETPRankAsc(){
//		//get a map of congested components
//		CongestedMap = CongestionDetection();
//
//		//calculate ETP for each component
//		for(Component e: this.topo.components){
//			Double score = ETPGeneration(e);
//			topoETPMap.put(e, score);
//		}
//		this.topoETPRankAsc.putAll(topoETPMap);
//		return this.topoETPRankAsc;
//	}
//
//	private HashMap<Component, Double> CongestionDetection() {
//		for( Map.Entry<Component, Double> i : executeRateMap.entrySet()) {
//			Double out=i.getValue();
//			Double in=0.0;
//			Component self=i.getKey();
//			if(self.parents.size()!=0){
//				for(String parent: self.parents){
//					in+=outputRateMap.get(parent);
//				}
//			}
//			if(in>1.2*out){
//				Double io=in-out;
//				this.CongestedMap.put(i.getKey(), io);
//			}
//		}
//	}
//
//	private Double ETPGeneration(Component e) {
//		// TODO Auto-generated method stub
//		Double ret = 0.0;
//		if(e.getChildren().size==0){
//			return this.emitRateMap.get(e);
//		}
//		for(Component c:e.getChildren()){
//			if(this.CongestedMap.get(c)!=null){
//				//give up this branch
//				return 0.0;
//			}
//			else{
//				ret=ret+ETPGeneration(c);
//			}
//		}
//
//		return ret;
//	}
//
//	private class ComponentComparatorDesc implements Comparator<Component> {
//
//		HashMap<Component, Double> base;
//	    public ComponentComparatorDesc(HashMap<Component, Double> base) {
//	        this.base = base;
//	    }
//
//	    // Note: this comparator imposes orderings that are inconsistent with equals.
//	    public int compare(Component a, Component b) {
//	        if (base.get(a) >= base.get(b)) {
//	            return -1;
//	        } else {
//	            return 1;
//	        } // returning 0 would merge keys
//	    }
//	}
//
//	private class ComponentComparatorAsc implements Comparator<Component> {
//
//		HashMap<Component, Double> base;
//	    public ComponentComparatorAsc(HashMap<Component, Double> base) {
//	        this.base = base;
//	    }
//
//	    // Note: this comparator imposes orderings that are inconsistent with equals.
//	    public int compare(Component a, Component b) {
//	        if (base.get(b) >= base.get(a)) {
//	            return -1;
//	        } else {
//	            return 1;
//	        } // returning 0 would merge keys
//	    }
//	}
//
//
//}
//
