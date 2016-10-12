package backtype.storm.scheduler.advancedstela;

import backtype.storm.scheduler.*;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.advancedstela.etp.*;
import backtype.storm.scheduler.advancedstela.slo.Observer;
import backtype.storm.scheduler.advancedstela.slo.Topology;
import backtype.storm.scheduler.advancedstela.slo.TopologyPairs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.util.*;

public class AdvancedStelaScheduler implements IScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(AdvancedStelaScheduler.class);
    private static final Integer OBSERVER_RUN_INTERVAL = 30;

    @SuppressWarnings("rawtypes")
    private Map config;
    private Observer sloObserver;
    private GlobalState globalState;
    private GlobalStatistics globalStatistics;
    private OperatorSelector selector;
    private HashMap<String, ExecutorPair> targets, victims;
    private File juice_log;

    public void prepare(@SuppressWarnings("rawtypes") Map conf) {
        juice_log = new File("/tmp/output.log");
        config = conf;
        sloObserver = new Observer(conf);
        globalState = new GlobalState(conf);
        globalStatistics = new GlobalStatistics(conf);
        selector = new OperatorSelector();
        victims = new HashMap<String, ExecutorPair>();
        targets = new HashMap<String, ExecutorPair>();
    }

    public void schedule(Topologies topologies, Cluster cluster) {
        logUnassignedExecutors(cluster.needsSchedulingTopologies(topologies), cluster);
        int numTopologiesThatNeedScheduling = cluster.needsSchedulingTopologies(topologies).size();
        for (String target : targets.keySet()) {
            LOG.info("target {} ", target);
        }
        for (String victim : victims.keySet()) {
            LOG.info("victim {} ", victim);
        }

        runAdvancedStelaComponents(cluster, topologies);
        if (numTopologiesThatNeedScheduling > 0) {
            LOG.info("STORM IS GOING TO PERFORM THE REBALANCING");
            new backtype.storm.scheduler.EvenScheduler().schedule(topologies, cluster);
        } else if (numTopologiesThatNeedScheduling == 0) {
            LOG.info("((victims.isEmpty() && targets.isEmpty()) && numTopologiesThatNeedScheduling == 0 && numTopologies > 0)");
            LOG.info("Printing topology schedules");
            Collection<TopologyDetails> topologyDetails = topologies.getTopologies();
            TopologyPairs topologiesToBeRescaled = sloObserver.getTopologiesToBeRescaled();

            ArrayList<String> receivers = topologiesToBeRescaled.getReceivers();
            ArrayList<String> givers = topologiesToBeRescaled.getGivers();

            ArrayList<Topology> receiver_topologies = new ArrayList<Topology>();
            ArrayList<Topology> giver_topologies = new ArrayList<Topology>();

            for (int i = 0; i < receivers.size(); i++)
                receiver_topologies.add(sloObserver.getTopologyById(receivers.get(i)));

            for (int i = 0; i < givers.size(); i++)
                giver_topologies.add(sloObserver.getTopologyById(givers.get(i)));

            LOG.info("Length of givers {}", giver_topologies.size());
            LOG.info("Length of receivers {}", receiver_topologies.size());

            if (receiver_topologies.size() > 0 && giver_topologies.size() > 0) {

                ArrayList<String> topologyPair = new TopologyPicker().classBasedStrategy(receiver_topologies, giver_topologies);//unifiedStrategy(receiver_topologies, giver_topologies);//.worstTargetBestVictim(receivers, givers);
                String receiver = topologyPair.get(0);

                LOG.info("Picked the first two topologies for rebalance");

                TopologyDetails target = topologies.getById(receiver);
                TopologySchedule targetSchedule = globalState.getTopologySchedules().get(receiver);
                Component targetComponent = selector.selectOperator(globalState, globalStatistics, sloObserver.getTopologyById(receiver));
                LOG.info("target before rebalanceTwoTopologies {} ", target.getId());

                if (targetComponent != null) {
                    LOG.info("topology {} target component", receiver, targetComponent.getId());
                    rebalanceTopology(target, targetSchedule, targetComponent, cluster);
                }
            } else if (giver_topologies.size() == 0) {
                StringBuffer sb = new StringBuffer();
                LOG.info("There are no givers! *Sob* \n");
                LOG.info("Receivers:  \n");

                for (int i = 0; i < receiver_topologies.size(); i++)
                    LOG.info(receiver_topologies.get(i).getId() + "\n");

            } else if (receiver_topologies.size() == 0) {
                StringBuffer sb = new StringBuffer();
                LOG.info("There are no receivers! *Sob* \n");
                LOG.info("Givers:  \n");

                for (int i = 0; i < giver_topologies.size(); i++)
                    LOG.info(giver_topologies.get(i).getId() + "\n");

            }
        }
    }

    private void rebalanceTopology(TopologyDetails targetDetails,
                                        TopologySchedule target,
                                        Component component,
                                        Cluster cluster) {
        LOG.info("In rebalance topology");
        if (config != null) {
            try {
                int one = 1;
                String targetComponent = component.getId();
                Integer targetOldParallelism = target.getComponents().get(targetComponent).getParallelism();
                Integer targetNewParallelism = targetOldParallelism + one;
                String targetCommand = "/var/nimbus/storm/bin/storm " +
                        "rebalance " + targetDetails.getName() + " -w 0 -e " +
                        targetComponent + "=" + targetNewParallelism;
                target.getComponents().get(targetComponent).setParallelism(targetNewParallelism);
                try {
                    writeToFile(juice_log, targetCommand + "\n");
                    writeToFile(juice_log, System.currentTimeMillis() + "\n");
                    LOG.info(targetCommand + "\n");
                    LOG.info(System.currentTimeMillis() + "\n");
                    LOG.info("running the rebalance using storm's rebalance command \n");
                    LOG.info("Target old executors count {}", targetDetails.getExecutors().size());
                    Runtime.getRuntime().exec(targetCommand);
                    sloObserver.updateLastRebalancedTime(target.getId(), System.currentTimeMillis() / 1000);
                    sloObserver.clearTopologySLOs(target.getId());
                } catch (Exception e) {
                    LOG.info(e.toString());
                    LOG.info("In first exception");
                    //e.printStackTrace();
                }
            } catch (Exception e) {
                LOG.info(e.toString());
                LOG.info("In second exception");
                //e.printStackTrace();
                return;
            }
        }
    }

    private void runAdvancedStelaComponents(Cluster cluster, Topologies topologies) {
        sloObserver.run();
        globalState.collect(cluster, topologies);
        globalState.setCapacities(sloObserver.getAllTopologies());
        globalStatistics.collect();
    }

    private void logUnassignedExecutors(List<TopologyDetails> topologiesScheduled, Cluster cluster) {
        for (TopologyDetails topologyDetails : topologiesScheduled) {
            Collection<ExecutorDetails> unassignedExecutors = cluster.getUnassignedExecutors(topologyDetails);

            if (unassignedExecutors.size() > 0) {
                for (ExecutorDetails executorDetails : unassignedExecutors) {
                    LOG.info("executorDetails.toString(): " + executorDetails.toString() + "\n");
                }
            }
        }
    }

    private void rebalanceTwoTopologies(TopologyDetails targetDetails,
                                        TopologySchedule target,
                                        TopologyDetails victimDetails,
                                        TopologySchedule victim,
                                        ExecutorPair executorSummaries,
                                        Cluster cluster) {
        LOG.info("In rebalance two topologies");
        if (config != null) {
            try {
                int one = 1;
                String targetComponent = executorSummaries.getTargetExecutorSummary().get_component_id();
                Integer targetOldParallelism = target.getComponents().get(targetComponent).getParallelism();
                Integer targetNewParallelism = targetOldParallelism + one;
                String targetCommand = "/var/nimbus/storm/bin/storm " +
                        "rebalance " + targetDetails.getName() + " -e " +
                        targetComponent + "=" + targetNewParallelism;
                target.getComponents().get(targetComponent).setParallelism(targetNewParallelism);

                String victimComponent = executorSummaries.getVictimExecutorSummary().get_component_id();
                Integer victimOldParallelism = victim.getComponents().get(victimComponent).getParallelism();
                Integer victimNewParallelism = victimOldParallelism - one;
                String victimCommand = "/var/nimbus/storm/bin/storm " +
                        "rebalance " + victimDetails.getName() + " -e " +
                        victimComponent + "=" + victimNewParallelism;

                victim.getComponents().get(victimComponent).setParallelism(victimNewParallelism);
                // FORMAT /var/nimbus/storm/bin/storm henge-rebalance  production-topology1 -e bolt_output_sink=13 xyz production-topology2 -e spout_head=12 xyz  production-topology3 -e bolt_output_sink=13 xyz production-topology4 -e spout_head=12
                try {
                    writeToFile(juice_log, targetCommand + "\n");
                    writeToFile(juice_log, System.currentTimeMillis() + "\n");
                    writeToFile(juice_log, victimCommand + "\n");

                    LOG.info(targetCommand + "\n");
                    LOG.info(System.currentTimeMillis() + "\n");
                    LOG.info(victimCommand + "\n");

                    Runtime.getRuntime().exec(targetCommand);
                    Runtime.getRuntime().exec(victimCommand);

                    targets.put(targetDetails.getId(), executorSummaries);
                    victims.put(victimDetails.getId(), executorSummaries);

                    sloObserver.updateLastRebalancedTime(target.getId(), System.currentTimeMillis() / 1000);
                    sloObserver.updateLastRebalancedTime(victim.getId(), System.currentTimeMillis() / 1000);

                    sloObserver.clearTopologySLOs(target.getId());
                    sloObserver.clearTopologySLOs(victim.getId());
                } catch (Exception e) {
                    LOG.info(e.toString());
                    LOG.info("In first exception");
                }
            } catch (Exception e) {
                LOG.info(e.toString());
                LOG.info("In second exception");
                return;
            }
        }
    }


    public void writeToFile(File file, String data) {
        try {
            FileWriter fileWriter = new FileWriter(file, true);
            BufferedWriter bufferWriter = new BufferedWriter(fileWriter);
            bufferWriter.append(data);
            bufferWriter.close();
            fileWriter.close();
        } catch (IOException ex) {
            LOG.info("error! writing to file {}", ex);
        }
    }
}