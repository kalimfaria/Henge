package backtype.storm.scheduler.advancedstela.etp;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.generated.ExecutorSummary;
import backtype.storm.scheduler.advancedstela.slo.Topology;

public class Selector {

    public ExecutorPair selectPair(GlobalState globalState, GlobalStatistics globalStatistics, Topology targetTopo, Topology victimTopo ) {

        TopologySchedule targetSchedule = globalState.getTopologySchedules().get(targetTopo.getId());
        TopologySchedule victimSchedule = globalState.getTopologySchedules().get(victimTopo.getId());
        TopologyStatistics targetStatistics = globalStatistics.getTopologyStatistics().get(targetTopo.getId());
        TopologyStatistics victimStatistics = globalStatistics.getTopologyStatistics().get(victimTopo.getId());

        //ETPStrategy targetStrategy = new ETPStrategy(targetSchedule, targetStatistics);
        //ETPStrategy victimStrategy = new ETPStrategy(victimSchedule, victimStatistics);
        ArrayList<ResultComponent> rankTarget = new ArrayList<ResultComponent>();
        if(targetTopo.getSensitivity().equals("throughput")){
        	ETPStrategy targetStrategy = new ETPStrategy(targetSchedule, targetStatistics);
        	rankTarget = targetStrategy.topologyETPRankDescending();
        }
        else{
        	LatencyStrategy targetStrategy = new LatencyStrategy(targetSchedule, targetStatistics, targetTopo);
        	rankTarget = targetStrategy.topologyETPRankDescending();
        }
        
        ArrayList<ResultComponent> rankVictim = new ArrayList<ResultComponent>();
        if(targetTopo.getSensitivity().equals("throughput")){
        	ETPStrategy victimStrategy = new ETPStrategy(victimSchedule, victimStatistics);
        	rankVictim = victimStrategy.topologyETPRankDescending();
        }
        else{
        	LatencyStrategy victimStrategy = new LatencyStrategy(victimSchedule, victimStatistics, victimTopo);
        	rankVictim = victimStrategy.topologyETPRankDescending();
        }
        

        for (ResultComponent victimComponent : rankVictim) {
            List<ExecutorSummary> victimExecutorDetails = victimComponent.component.getExecutorSummaries();

            for (ResultComponent targetComponent : rankTarget) {
                List<ExecutorSummary> targetExecutorDetails = targetComponent.component.getExecutorSummaries();

                for (ExecutorSummary victimSummary : victimExecutorDetails) {
                    for (ExecutorSummary targetSummary : targetExecutorDetails) {

                        if (victimSummary.get_host().equals(targetSummary.get_host())) {
                            return new ExecutorPair(targetSummary, victimSummary);
                        }

                    }
                }
            }
        }
        return null;
    }
}
