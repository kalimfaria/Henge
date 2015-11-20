package backtype.storm.scheduler.advancedstela.etp;

import backtype.storm.generated.ExecutorSummary;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Selector {

    public ExecutorPair selectPair(GlobalState globalState, GlobalStatistics globalStatistics,
                                                 String targetID, String victimID) {

        TopologySchedule targetSchedule = globalState.getTopologySchedules().get(targetID);
        TopologySchedule victimSchedule = globalState.getTopologySchedules().get(victimID);
        TopologyStatistics targetStatistics = globalStatistics.getTopologyStatistics().get(targetID);
        TopologyStatistics victimStatistics = globalStatistics.getTopologyStatistics().get(victimID);

        Strategy targetStrategy = new Strategy(targetSchedule, targetStatistics);
        Strategy victimStrategy = new Strategy(victimSchedule, victimStatistics);
        TreeMap<Component, Double> rankTarget = targetStrategy.topologyETPRankDescending();
        TreeMap<Component, Double> rankVictim = victimStrategy.topologyETPRankAscending();

        for (Map.Entry<Component, Double> victimComponent : rankVictim.entrySet()) {
            List<ExecutorSummary> victimExecutorDetails = victimComponent.getKey().getExecutorSummaries();

            for (Map.Entry<Component, Double> targetComponent : rankTarget.entrySet()) {
                List<ExecutorSummary> targetExecutorDetails = targetComponent.getKey().getExecutorSummaries();

                for (ExecutorSummary victimSummary : victimExecutorDetails) {
                    for (ExecutorSummary targetSummary : targetExecutorDetails) {

                        if (victimSummary.get_host().equals(targetSummary.get_host())) {
                            ExecutorPair executorPair = new ExecutorPair(targetSummary, victimSummary);
                            return executorPair;
                        }

                    }
                }
            }
        }
        return null;
    }
}
