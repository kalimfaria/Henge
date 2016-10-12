package backtype.storm.scheduler.advancedstela.etp;

import backtype.storm.generated.ExecutorSummary;
import backtype.storm.scheduler.advancedstela.slo.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class OperatorSelector {
    private static final Logger LOG = LoggerFactory.getLogger(GlobalState.class);

    public Component selectOperator(GlobalState globalState, GlobalStatistics globalStatistics, Topology targetTopo) {
        LOG.info("In Operator Selector");
        TopologySchedule targetSchedule = globalState.getTopologySchedules().get(targetTopo.getId());
        TopologyStatistics targetStatistics = globalStatistics.getTopologyStatistics().get(targetTopo.getId());

        ArrayList<ResultComponent> rankTarget = new ArrayList<ResultComponent>();
        if(targetTopo.getSensitivity().equals("throughput")){
            LOG.info("Sensitivity equals throughput");
        	ETPStrategy targetStrategy = new ETPStrategy(targetSchedule, targetStatistics);
        	rankTarget = targetStrategy.topologyETPRankDescending();
        }
        else {
            LOG.info("Sensitivity equals latency");
        	LatencyStrategyWithCapacity targetStrategy = new LatencyStrategyWithCapacity(targetSchedule, targetStatistics, targetTopo);
        	rankTarget = targetStrategy.topologyCapacityDescending();
        }
        Component targetOperator = rankTarget.get(0).component;
        LOG.info("Picked target operator {}", targetOperator.getId());
        LOG.info("Picked target operator capacity {}", targetOperator.getCapacity());
        LOG.info("Picked target operator etp Value {}", rankTarget.get(0).etpValue);
        return targetOperator;
    }
}
