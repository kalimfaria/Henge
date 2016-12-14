package backtype.storm.scheduler.advancedstela.etp;

import backtype.storm.scheduler.advancedstela.slo.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class OperatorSelector {
    private static final Logger LOG = LoggerFactory.getLogger(GlobalState.class);

    public Component selectOperator(GlobalState globalState, GlobalStatistics globalStatistics, Topology targetTopo) {
        LOG.info("In Operator Selector");
        TopologySchedule targetSchedule = globalState.getTopologySchedules().get(targetTopo.getId());
        TopologyStatistics targetStatistics = globalStatistics.getTopologyStatistics().get(targetTopo.getId());

        ArrayList<ResultComponent> rankTarget = new ArrayList<ResultComponent>();
        LatencyStrategyWithCapacity targetStrategy = new LatencyStrategyWithCapacity(targetSchedule, targetStatistics, targetTopo);
        rankTarget = targetStrategy.topologyCapacityDescending();
        Component targetOperator = null;
        try {
            targetOperator = rankTarget.get(0).component;
        } catch (Exception e) {
        }
        return targetOperator;
    }

    public ArrayList<ResultComponent> selectAllOperators(GlobalState globalState, GlobalStatistics globalStatistics, Topology targetTopo) {
        LOG.info("In Operator Selector");
        TopologySchedule targetSchedule = globalState.getTopologySchedules().get(targetTopo.getId());
        TopologyStatistics targetStatistics = globalStatistics.getTopologyStatistics().get(targetTopo.getId());
        ArrayList<ResultComponent> rankTarget = new ArrayList<ResultComponent>();
        LatencyStrategyWithCapacity targetStrategy = new LatencyStrategyWithCapacity(targetSchedule, targetStatistics, targetTopo);
        rankTarget = targetStrategy.topologyCapacityDescending();
        return rankTarget;
    }
}
