package backtype.storm.scheduler.advancedstela.etp;//package backtype.storm.scheduler.advancedstela.etp;
//
//import java.awt.Component;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Map.Entry;
//import java.util.TreeMap;
//
//import backtype.storm.scheduler.Cluster;
//import backtype.storm.scheduler.ExecutorDetails;
//import backtype.storm.scheduler.Topologies;
//import backtype.storm.scheduler.TopologyDetails;
//import backtype.storm.scheduler.WorkerSlot;
//
//public class ExecutorSelect {
//
//
//	public static ArrayList<ExecutorDetails> selectPair(TopologyDetails topo_target, TopologyDetails topo_victim, GlobalState globalState, GetStats getStats, TopologyDetails topo, Cluster cluster, Topologies topologies, HashMap<Component, Double> executeRateMap, HashMap<Component, Double> inputRateMap, HashMap<Component, Double> outputRateMap ){
//		ETPStrategy targetStrategy = new ETPStrategy(globalState, getStats, topo_target, cluster, topologies, executeRateMap, inputRateMap, outputRateMap);
//		ETPStrategy victimStrategy = new ETPStrategy(globalState, getStats, topo_victim, cluster, topologies, executeRateMap, inputRateMap, outputRateMap);
//		TreeMap<Component, Double> rankTarget = targetStrategy.TopoETPRankDesc();
//		TreeMap<Component, Double> rankVictim = victimStrategy.TopoETPRankAsc();
//		for(Map.Entry<Component, Double> v : rankVictim.entrySet()){
//			List<ExecutorDetails> arr_v_execs = v.getKey().execs;
//			for(Entry<Component, Double> t:rankTarget.entrySet()){
//				List<ExecutorDetails> arr_t_execs = t.getKey().execs;
//				for(ExecutorDetails ve :arr_v_execs ){
//					for(ExecutorDetails te : arr_t_execs){
//						if(ExecToNode(ve,topo).equals(ExecToNode(te,topo))==true){
//							ArrayList<ExecutorDetails> ret = new ArrayList<ExecutorDetails>();
//							ret.add(ve);
//							ret.add(te);
//							return ret;
//						}
//
//					}
//				}
//			}
//		}
//		return null;
//
//	}
//
//	private static String ExecToNode(ExecutorDetails ve, TopologyDetails topo) {
//		// TODO Auto-generated method stub
//		WorkerSlot w = cluster.getAssignmentById(topo.getId()).getExecutorToSlot().get(ve);
//		return w.getNodeId();
//	}
//}
//