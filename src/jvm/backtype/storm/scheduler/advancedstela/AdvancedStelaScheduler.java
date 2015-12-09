package backtype.storm.scheduler.advancedstela;

import backtype.storm.generated.ExecutorSummary;
import backtype.storm.scheduler.*;
import backtype.storm.scheduler.advancedstela.etp.*;
import backtype.storm.scheduler.advancedstela.slo.Observer;
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
    private Selector selector;
    private HashMap<String, String> targetToVictimMapping;
    private HashMap<String, ExecutorPair> targetToNodeMapping;
    private File rebalance_log;

    public void prepare(@SuppressWarnings("rawtypes") Map conf) {
        rebalance_log = new File("/var/nimbus/storm/rebalance.log");
        config = conf;
        sloObserver = new Observer(conf);
        globalState = new GlobalState(conf);
        globalStatistics = new GlobalStatistics(conf);
        selector = new Selector();
        targetToVictimMapping = new HashMap<String, String>();
        targetToNodeMapping = new HashMap<String, ExecutorPair>();

//  TODO: Code for running the observer as a separate thread.
//        Integer observerRunDelay = (Integer) config.get(Config.STELA_SLO_OBSERVER_INTERVAL);
//        if (observerRunDelay == null) {
//            observerRunDelay = OBSERVER_RUN_INTERVAL;
//        }
//
//        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
//        executorService.scheduleAtFixedRate(new Runner(sloObserver), 180, observerRunDelay, TimeUnit.SECONDS);
    }

    public void schedule(Topologies topologies, Cluster cluster) {
        if (cluster.needsSchedulingTopologies(topologies).size() > 0) {

            new backtype.storm.scheduler.EvenScheduler().schedule(topologies, cluster);
            List<TopologyDetails> topologiesScheduled = cluster.needsSchedulingTopologies(topologies);
            logUnassignedExecutors(topologiesScheduled, cluster);
            if (targetToVictimMapping.size() > 0) {
                applyRebalancedScheduling(cluster, topologies);
            }

            globalState.collect(cluster, topologies);
            globalStatistics.collect();


        } else if (cluster.needsSchedulingTopologies(topologies).size() == 0 && topologies.getTopologies().size() > 0) {

            globalState.collect(cluster, topologies);
            globalStatistics.collect();

            TopologyPairs topologiesToBeRescaled = sloObserver.getTopologiesToBeRescaled();
            ArrayList<String> receivers = topologiesToBeRescaled.getReceivers();
            ArrayList<String> givers = topologiesToBeRescaled.getGivers();

            removeAlreadySelectedPairs(receivers, givers);

            if (receivers.size() > 0 && givers.size() > 0) {
                TopologyDetails target = topologies.getById(receivers.get(0));
                TopologySchedule targetSchedule = globalState.getTopologySchedules().get(receivers.get(0));
                TopologyDetails victim = topologies.getById(givers.get(givers.size() - 1));
                TopologySchedule victimSchedule = globalState.getTopologySchedules().get(givers.get(givers.size() - 1));

                ExecutorPair executorSummaries =
                        selector.selectPair(globalState, globalStatistics, receivers.get(0), givers.get(givers.size() - 1));

                if (executorSummaries.bothPopulated()) {
                    rebalanceTwoTopologies(target, targetSchedule, victim, victimSchedule, executorSummaries);
                } else {
                    LOG.error("No topology is satisfying its SLO. New nodes need to be added to the cluster");
                }
            } else if (givers.size() == 0) {
                LOG.error("No topology is satisfying its SLO. New nodes need to be added to the cluster");
            }
        }

        runAdvancedStelaComponents(cluster, topologies);
    }

    private void runAdvancedStelaComponents(Cluster cluster, Topologies topologies) {
        sloObserver.run();
        globalState.collect(cluster, topologies);
        globalStatistics.collect();
    }

    private void logUnassignedExecutors(List<TopologyDetails> topologiesScheduled, Cluster cluster) {
        for (TopologyDetails topologyDetails : topologiesScheduled) {
            Collection<ExecutorDetails> unassignedExecutors = cluster.getUnassignedExecutors(topologyDetails);
            LOG.info("******** Topology Assignment {} *********", topologyDetails.getId());
            System.out.println("******** Topology Assignment " + topologyDetails.getId() + " *********");
            LOG.info("Unassigned executors size: {}", unassignedExecutors.size());
            System.out.println("Unassigned executors size: "+unassignedExecutors.size()+"");
            if (unassignedExecutors.size() > 0) {
                for (ExecutorDetails executorDetails : unassignedExecutors) {
                    LOG.info(executorDetails.toString());
                    System.out.println("executorDetails.toString(): " + executorDetails.toString());
                }
            }
        }
    }

    private void rebalanceTwoTopologies(TopologyDetails targetDetails, TopologySchedule target,
                                        TopologyDetails victimDetails, TopologySchedule victim, ExecutorPair executorSummaries) {
        String targetComponent = executorSummaries.getTargetExecutorSummary().get_component_id();
        String targetCommand = "/var/nimbus/storm/bin/storm " +
                "rebalance " + targetDetails.getName() + " -w 0 -e " +
                targetComponent + "=" + (target.getComponents().get(targetComponent).getParallelism() + 1);
        System.out.println(targetCommand);

        writeToFile(rebalance_log, targetDetails.getName() + "," + targetComponent + "," + (target.getComponents().get(targetComponent).getParallelism() + 1 + "," + System.currentTimeMillis() + "\n"));

        String victimComponent = executorSummaries.getVictimExecutorSummary().get_component_id();
        String victimCommand = "/var/nimbus/storm/bin/storm " +
                "rebalance " + victimDetails.getName() + " -w 0 -e " +
                victimComponent + "=" + (victim.getComponents().get(victimComponent).getParallelism() - 1);
        System.out.println(victimCommand);
        writeToFile(rebalance_log, victimDetails.getName() + "," + victimComponent + "," + (victim.getComponents().get(victimComponent).getParallelism() + 1 + "," + System.currentTimeMillis() + "\n"));

        try {

            LOG.info("Triggering rebalance for target: {}, victim: {}\n", targetDetails.getId(), victimDetails.getId());
            LOG.info(targetCommand + "\n");
            LOG.info(victimCommand + "\n");

            Runtime.getRuntime().exec(targetCommand);
            Runtime.getRuntime().exec(victimCommand);

            targetToVictimMapping.put(target.getId(), victim.getId());
            targetToNodeMapping.put(target.getId(), executorSummaries);
            System.out.println("sloObserver.clearTopologySLOs(target.getId());: " + target.getId());
            sloObserver.clearTopologySLOs(target.getId());
            System.out.println("sloObserver.clearTopologySLOs(victim.getId());: " + victim.getId());
            sloObserver.clearTopologySLOs(victim.getId());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void removeAlreadySelectedPairs(ArrayList<String> receivers, ArrayList<String> givers) {
        for (String target : targetToVictimMapping.keySet()) {
            int targetIndex = receivers.indexOf(target);
            if (targetIndex != -1) {
                receivers.remove(targetIndex);
            }

            String victim = targetToVictimMapping.get(target);
            int victimIndex = givers.indexOf(victim);
            if (victimIndex != -1) {
                givers.remove(victimIndex);
            }
        }
    }

    private void applyRebalancedScheduling(Cluster cluster, Topologies topologies) {
        for (Map.Entry<String, String> targetToVictim : targetToVictimMapping.entrySet()) {
            TopologyDetails target = topologies.getById(targetToVictim.getKey());
            TopologyDetails victim = topologies.getById(targetToVictim.getValue());
            ExecutorPair executorPair = targetToNodeMapping.get(targetToVictim.getKey());

            reassignNewScheduling(target, victim, cluster, executorPair);
        }
    }

    private void reassignNewScheduling(TopologyDetails target, TopologyDetails victim, Cluster cluster,
                                       ExecutorPair executorPair) {

        ExecutorSummary targetExecutorSummary = executorPair.getTargetExecutorSummary();
        ExecutorSummary victimExecutorSummary = executorPair.getVictimExecutorSummary();

        WorkerSlot targetSlot = new WorkerSlot(targetExecutorSummary.get_host(), targetExecutorSummary.get_port());
        WorkerSlot victimSlot = new WorkerSlot(victimExecutorSummary.get_host(), victimExecutorSummary.get_port());
        System.out.println("previousTargetExecutors" + target.getId());
        Set<ExecutorDetails> previousTargetExecutors = globalState.getTopologySchedules().get(target.getId()).getExecutorToComponent().keySet();

        LOG.info("\n************** Target Topology **************");

        LOG.info("\n****** Previous Executors ******");
        for (ExecutorDetails executorDetails : previousTargetExecutors) {
            LOG.info(executorDetails.toString());
        }

        SchedulerAssignment currentTargetAssignment = cluster.getAssignmentById(target.getId());
        if (currentTargetAssignment != null) {
            Set<ExecutorDetails> currentTargetExecutors = currentTargetAssignment.getExecutorToSlot().keySet();
            currentTargetExecutors.removeAll(previousTargetExecutors);

            LOG.info("\n****** Current Executors ******");
            for (ExecutorDetails executorDetails : currentTargetExecutors) {
                LOG.info(executorDetails.toString());
            }

            for (ExecutorDetails newExecutor : currentTargetExecutors) {
                LOG.info("\n********************** Found new executor *********************");
                LOG.info(newExecutor.toString());
            }
        }

        LOG.info("\n************** Victim Topology **************");

        Set<ExecutorDetails> previousVictimExecutors = globalState.getTopologySchedules().get(victim.getId()).getExecutorToComponent().keySet();
        SchedulerAssignment currentVictimAssignment = cluster.getAssignmentById(victim.getId());

        LOG.info("\n****** Previous Executors ******");
        for (ExecutorDetails executorDetails : previousVictimExecutors) {
            LOG.info(executorDetails.toString());
        }

        if (currentVictimAssignment != null) {
            Set<ExecutorDetails> currentVictimExecutors = currentVictimAssignment.getExecutorToSlot().keySet();
            previousVictimExecutors.removeAll(currentVictimExecutors);

            LOG.info("\n****** Current Executors ******");
            for (ExecutorDetails executorDetails : currentVictimExecutors) {
                LOG.info(executorDetails.toString());
            }

            for (ExecutorDetails newExecutor : previousVictimExecutors) {
                LOG.info("********************** Removed executor *********************");
                LOG.info(newExecutor.toString());
            }
        }

//        Collection<ExecutorDetails> unassignedExecutors = cluster.getUnassignedExecutors(target);
//        cluster.freeSlot(targetSlot);
//        targetExecutorDetails.addAll(unassignedExecutors);
//        cluster.assign(targetSlot, target.getId(), targetExecutorDetails);

//
//        targetToVictimMapping.remove(target.getId());
//        targetToNodeMapping.remove(target.getId());
    }

    private List<ExecutorDetails> difference(Collection<ExecutorDetails> execs1,
                                             Collection<ExecutorDetails> execs2) {
        List<ExecutorDetails> result = new ArrayList<ExecutorDetails>();
        for (ExecutorDetails exec : execs1) {
            if (!execs2.contains(exec)) {
                result.add(exec);
            }
        }
        return result;

    }

    public void writeToFile(File file, String data) {
        try {
            FileWriter fileWritter = new FileWriter(file, true);
            BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
            bufferWritter.append(data);
            bufferWritter.close();
            fileWritter.close();
            LOG.info("wrote to slo file {}", data);
        } catch (IOException ex) {
            LOG.info("error! writing to file {}", ex);
        }
    }
}
