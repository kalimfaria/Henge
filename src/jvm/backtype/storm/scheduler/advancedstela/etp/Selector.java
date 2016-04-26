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

        ETPStrategy targetStrategy = new ETPStrategy(targetSchedule, targetStatistics);
        ETPStrategy victimStrategy = new ETPStrategy(victimSchedule, victimStatistics);

        ArrayList<ResultComponent> rankTarget = targetStrategy.topologyETPRankDescending();
        ArrayList<ResultComponent> rankVictim = victimStrategy.topologyETPRankAscending();

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
