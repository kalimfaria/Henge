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

        ArrayList<ResultComponent> rankTarget = new ArrayList<ResultComponent>();
        if(targetTopo.getSensitivity().equals("throughput")){
        	ETPStrategy targetStrategy = new ETPStrategy(targetSchedule, targetStatistics);
        	rankTarget = targetStrategy.topologyETPRankDescending();
        }
        else{
        	LatencyStrategyWithCapacity targetStrategy = new LatencyStrategyWithCapacity(targetSchedule, targetStatistics, targetTopo);
        	rankTarget = targetStrategy.topologyCapacityDescending();

        }
        
        ArrayList<ResultComponent> rankVictim = new ArrayList<ResultComponent>();
        if(targetTopo.getSensitivity().equals("throughput")){
        	ETPStrategy victimStrategy = new ETPStrategy(victimSchedule, victimStatistics);
        	rankVictim = victimStrategy.topologyETPRankAscending(); // CHANGED BY FARIA
        }
        else{
        	LatencyStrategyWithCapacity victimStrategy = new LatencyStrategyWithCapacity(victimSchedule, victimStatistics, victimTopo);
        	rankVictim = victimStrategy.topologyCapacityAscending(); // CHANGED BY FARIA
        }

        //ETPStrategy targetStrategy = new ETPStrategy(targetSchedule, targetStatistics);
        //ETPStrategy victimStrategy = new ETPStrategy(victimSchedule, victimStatistics);
        for (ResultComponent victimComponent : rankVictim) {
            List<ExecutorSummary> victimExecutorDetails = victimComponent.component.getExecutorSummaries();

            for (ResultComponent targetComponent : rankTarget) {
                List<ExecutorSummary> targetExecutorDetails = targetComponent.component.getExecutorSummaries();

                for (ExecutorSummary victimSummary : victimExecutorDetails) {
                    for (ExecutorSummary targetSummary : targetExecutorDetails) {

                        if (victimSummary.get_host().equals(targetSummary.get_host())) {
                            System.out.println("What is being returned by the selector");
                            System.out.println("targetComponent ID: " + targetComponent.component.getId());
                            System.out.println("targetComponent Capacity: " + targetComponent.component.getCapacity());
                            System.out.println("victimComponent ID: " + victimComponent.component.getId());
                            System.out.println("victimComponent Capacity: " + victimComponent.component.getCapacity());
                            return new ExecutorPair(targetSummary, victimSummary);
                        }

                    }
                }
            }
        }
        return null;
    }
}
