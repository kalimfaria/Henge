package backtype.storm.scheduler.advancedstela.etp;

import backtype.storm.scheduler.advancedstela.slo.Sensitivity;
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
        if(targetTopo.getSensitivity() == Sensitivity.JUICE){
            LOG.info("Sensitivity equals throughput");
        	ETPStrategy targetStrategy = new ETPStrategy(targetSchedule, targetStatistics);
        	rankTarget = targetStrategy.topologyETPRankDescending();
        }
        else if (targetTopo.getSensitivity() == Sensitivity.LATENCY)  {
            LOG.info("Sensitivity equals latency");
        	LatencyStrategyWithCapacity targetStrategy = new LatencyStrategyWithCapacity(targetSchedule, targetStatistics, targetTopo);
        	rankTarget = targetStrategy.topologyCapacityDescending();
        } else if (targetTopo.getSensitivity() == Sensitivity.BOTH) {
            LOG.info("Sensitivity equals both");
            Sensitivity s = targetTopo.whichSLOToSatisfyFirst();
            LOG.info("Sensitivity chosen : {} ", s);

            if(s == Sensitivity.JUICE){
                LOG.info("Sensitivity equals throughput");
                ETPStrategy targetStrategy = new ETPStrategy(targetSchedule, targetStatistics);
                rankTarget = targetStrategy.topologyETPRankDescending();
            }
            else if (s == Sensitivity.LATENCY)  {
                LOG.info("Sensitivity equals latency");
                LatencyStrategyWithCapacity targetStrategy = new LatencyStrategyWithCapacity(targetSchedule, targetStatistics, targetTopo);
                rankTarget = targetStrategy.topologyCapacityDescending();
            }

        }
        Component targetOperator = null;
        try {
            targetOperator= rankTarget.get(0).component;
            LOG.info("Picked target operator {}", targetOperator.getId());
            LOG.info("Picked target operator capacity {}", targetOperator.getCapacity());
            LOG.info("Picked target operator etp Value {}", rankTarget.get(0).etpValue);
        } catch (Exception e) {
            LOG.info("EXCEPTION: {} time {}", e.toString(), System.currentTimeMillis());
        }
        return targetOperator;
    }
}
