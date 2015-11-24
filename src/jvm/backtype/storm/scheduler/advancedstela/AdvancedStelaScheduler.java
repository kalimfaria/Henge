package backtype.storm.scheduler.advancedstela;

import backtype.storm.Config;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.advancedstela.etp.*;
import backtype.storm.scheduler.advancedstela.slo.Observer;
import backtype.storm.scheduler.advancedstela.slo.Runner;
import backtype.storm.scheduler.advancedstela.slo.TopologyPairs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AdvancedStelaScheduler implements IScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(AdvancedStelaScheduler.class);
    private static final Integer OBSERVER_RUN_INTERVAL = 30;

    @SuppressWarnings("rawtypes")
    private Map config;
    private Observer sloObserver;
    private GlobalState globalState;
    private GlobalStatistics globalStatistics;
    private Selector selector;

    public void prepare(@SuppressWarnings("rawtypes") Map conf) {
        config = conf;
        sloObserver = new Observer(conf);

        Integer observerRunDelay = (Integer) config.get(Config.STELA_SLO_OBSERVER_INTERVAL);
        if (observerRunDelay == null) {
            observerRunDelay = OBSERVER_RUN_INTERVAL;
        }
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(new Runner(sloObserver), 0, observerRunDelay, TimeUnit.SECONDS);

        globalState = new GlobalState(config);
        globalStatistics = new GlobalStatistics(config);
        selector = new Selector();
    }

    public void schedule(Topologies topologies, Cluster cluster) {
        if (cluster.needsSchedulingTopologies(topologies).size() > 0) {
            new backtype.storm.scheduler.EvenScheduler().schedule(topologies, cluster);
            globalState.collect(cluster, topologies);
            globalStatistics.collect();

        } else if (cluster.needsSchedulingTopologies(topologies).size() == 0 && topologies.getTopologies().size() > 0){
            globalState.collect(cluster, topologies);
            globalStatistics.collect();

            TopologyPairs topologiesToBeRescaled = sloObserver.getTopologiesToBeRescaled();
            ArrayList<String> receivers = topologiesToBeRescaled.getReceivers();
            ArrayList<String> givers = topologiesToBeRescaled.getGivers();

            if (receivers.size() > 0 && givers.size() > 0) {
                TopologyDetails target = topologies.getById(receivers.get(0));
                TopologySchedule targetSchedule = globalState.getTopologySchedules().get(receivers.get(0));
                TopologyDetails victim = topologies.getById(givers.get(givers.size() - 1));
                TopologySchedule victimSchedule = globalState.getTopologySchedules().get(givers.get(givers.size() - 1));

                ExecutorPair executorSummaries =
                        selector.selectPair(globalState, globalStatistics, receivers.get(0), givers.get(givers.size() - 1));

                if (executorSummaries != null) {
                    rebalanceTwoTopologies(target, targetSchedule, victim, victimSchedule, executorSummaries);
                }
            } else if (givers.size() == 0) {
                LOG.error("No topology is satisfying its SLO. New nodes need to be added to the cluster");
            }
        }
    }

    private void rebalanceTwoTopologies(TopologyDetails targetDetails, TopologySchedule target,
                                        TopologyDetails victimDetails, TopologySchedule victim, ExecutorPair executorSummaries) {
        String targetComponent = executorSummaries.getTargetExecutorSummary().get_component_id();
        String targetCommand = "/var/storm/bin/storm rebalance " + targetDetails.getName() + " -e " +
                targetComponent + "=" + (target.getComponents().get(targetComponent).getParallelism() + 1);

        String victimComponent = executorSummaries.getVictimExecutorSummary().get_component_id();
        String victimCommand = "/var/storm/bin/storm rebalance " + victimDetails.getName() + " -e " +
                victimComponent + "=" + (victim.getComponents().get(victimComponent).getParallelism() - 1);

        try {
            Runtime.getRuntime().exec(targetCommand);
            Runtime.getRuntime().exec(victimCommand);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
