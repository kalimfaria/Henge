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

//import backtype.storm.utils.NimbusClient;

public class AdvancedStelaScheduler_Temp implements IScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(AdvancedStelaScheduler_Temp.class);
    private static final Integer OBSERVER_RUN_INTERVAL = 30;

    @SuppressWarnings("rawtypes")
    private Map config;
    private Observer sloObserver;
    private GlobalState globalState;
    private GlobalStatistics globalStatistics;
    private Selector selector;
    private HashMap<String, String> targetToVictimMapping;
    private String targetID, victimID;
    private HashMap<String, ExecutorPair> targetToNodeMapping;
    private File rebalance_log;
    private File advanced_scheduling_log;
    private File slo_log;

    public void prepare(@SuppressWarnings("rawtypes") Map conf) {
        rebalance_log = new File("/var/nimbus/storm/rebalance.log");
        advanced_scheduling_log = new File("/var/nimbus/storm/advanced_scheduling_log.log");
        slo_log = new File("/var/nimbus/storm/slo.log");
        config = conf;
        sloObserver = new Observer(conf);
        globalState = new GlobalState(conf);
        globalStatistics = new GlobalStatistics(conf);
        selector = new Selector();
        targetToVictimMapping = new HashMap<String, String>();
        targetToNodeMapping = new HashMap<String, ExecutorPair>();
        targetID = new String();
        victimID = new String();

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
        logUnassignedExecutors(cluster.needsSchedulingTopologies(topologies), cluster);
        if (cluster.needsSchedulingTopologies(topologies).size() > 0) {

            writeToFile(advanced_scheduling_log, "Before calling EvenScheduler: \n");
            writeToFile(advanced_scheduling_log, "Size of cluster.needsSchedulingTopologies(topologies): " + cluster.needsSchedulingTopologies(topologies).size() + "\n");
            boolean victimNeedsToBeRebalanced = false, targetNeedsToBeRebalanced = false;
            List<TopologyDetails> topologiesScheduled = cluster.needsSchedulingTopologies(topologies);

            for (TopologyDetails topologyThatNeedsToBeScheduled : topologiesScheduled) {
                if (targetID.length() > 0 && targetID.equals(topologyThatNeedsToBeScheduled.getId()))
                    targetNeedsToBeRebalanced = true;
                if (victimID.length() > 0 && victimID.equals(topologyThatNeedsToBeScheduled.getId()))
                    victimNeedsToBeRebalanced = true;

            }

            writeToFile(advanced_scheduling_log, "targetNeedsToBeRebalanced : " + targetNeedsToBeRebalanced + "\n");
            writeToFile(advanced_scheduling_log, "victimNeedsToBeRebalanced: " + victimNeedsToBeRebalanced + "\n");
            writeToFile(advanced_scheduling_log, "targetID.length() : " + targetID.length() + "\n");
            writeToFile(advanced_scheduling_log, "victimID.length(): " + victimID.length() + "\n");
            writeToFile(advanced_scheduling_log, "targetID : " + targetID + "\n");
            writeToFile(advanced_scheduling_log, "victimID: " + victimID + "\n");


            writeToFile(advanced_scheduling_log, "After calling EvenScheduler: \n");
            writeToFile(advanced_scheduling_log, "Size of cluster.needsSchedulingTopologies(topologies): " + cluster.needsSchedulingTopologies(topologies).size() + "\n");
            topologiesScheduled = cluster.needsSchedulingTopologies(topologies);
            for (TopologyDetails topologyThatNeedsToBeScheduled : topologiesScheduled) {
                writeToFile(advanced_scheduling_log, "Id of topology: " + topologyThatNeedsToBeScheduled.getId() + "\n");
            }

            if (targetToVictimMapping.size() > 0) {
                applyRebalancedScheduling(cluster, topologies, targetNeedsToBeRebalanced, victimNeedsToBeRebalanced);
            }

            //   globalState.collect(cluster, topologies);
            //   globalStatistics.collect();

            new EvenScheduler().schedule(topologies, cluster);
            runAdvancedStelaComponents(cluster, topologies);
        } else if (cluster.needsSchedulingTopologies(topologies).size() == 0 && topologies.getTopologies().size() > 0) {

            //   globalState.collect(cluster, topologies);
            //   globalStatistics.collect();

            runAdvancedStelaComponents(cluster, topologies);

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


    }

    private void runAdvancedStelaComponents(Cluster cluster, Topologies topologies) {
        sloObserver.run();
        globalState.collect(cluster, topologies);
        globalStatistics.collect();
    }

    private void logUnassignedExecutors(List<TopologyDetails> topologiesScheduled, Cluster cluster) {
        writeToFile(advanced_scheduling_log, "In logUnassignedExecutors\n Outside the loop: \n");

        for (TopologyDetails topologyDetails : topologiesScheduled) {
            Collection<ExecutorDetails> unassignedExecutors = cluster.getUnassignedExecutors(topologyDetails);
            writeToFile(advanced_scheduling_log, "In logUnassignedExecutors \n ******** Topology Assignment " + topologyDetails.getId() + " ********* \n ");
            writeToFile(advanced_scheduling_log, "Unassigned executors size: " + unassignedExecutors.size() + "\n");
            writeToFile(advanced_scheduling_log, "Unassigned executors size: " + unassignedExecutors.size() + "\n");
            StringBuffer forOutputLog = new StringBuffer();
            if (unassignedExecutors.size() > 0) {
                for (ExecutorDetails executorDetails : unassignedExecutors) {
                    forOutputLog.append("executorDetails.toString(): " + executorDetails.toString() + "\n");
                }
            }
            writeToFile(advanced_scheduling_log, forOutputLog.toString() + " \n end of logUnassignedExecutors \n " + "\n");
        }
    }


    private void rebalanceTwoTopologies(TopologyDetails targetDetails, TopologySchedule target,
                                        TopologyDetails victimDetails, TopologySchedule victim, ExecutorPair executorSummaries) {

      //  NimbusClient nimbusClient;

        if (config != null) {
            try {
           //     nimbusClient = new NimbusClient(config, (String) config.get(Config.NIMBUS_HOST));


                String targetComponent = executorSummaries.getTargetExecutorSummary().get_component_id();
                Integer targetOldParallelism = target.getComponents().get(targetComponent).getParallelism();
                Integer targetNewParallelism = targetOldParallelism + 1;
                String targetCommand = "/var/nimbus/storm/bin/storm " +
                        "rebalance " + targetDetails.getName() + " -e " +
                        targetComponent + "=" + targetNewParallelism;
                target.getComponents().get(targetComponent).setParallelism(targetNewParallelism);


                String victimComponent = executorSummaries.getVictimExecutorSummary().get_component_id();
                Integer victimOldParallelism = victim.getComponents().get(victimComponent).getParallelism();
                Integer victimNewParallelism = victimOldParallelism - 1;
                String victimCommand = "/var/nimbus/storm/bin/storm " +
                        "rebalance " + victimDetails.getName() + " -e " +
                        victimComponent + "=" + victimNewParallelism;
                victim.getComponents().get(victimComponent).setParallelism(victimNewParallelism);

                try {

                    writeToFile(advanced_scheduling_log, "Triggering rebalance for target: " + targetDetails.getId() + ", victim: " + victimDetails.getId() + "\n");
                    writeToFile(advanced_scheduling_log, targetCommand + "\n");
                    writeToFile(advanced_scheduling_log, victimCommand + "\n");
                    writeToFile(advanced_scheduling_log, "New parallelism hint for target: " + target.getComponents().get(targetComponent).getParallelism() + "\n");
                    writeToFile(advanced_scheduling_log, "New parallelism hint for victim: " + victim.getComponents().get(victimComponent).getParallelism() + "\n");

                    Runtime.getRuntime().exec(targetCommand);
                    Runtime.getRuntime().exec(victimCommand);
                    writeToFile(slo_log, "Rebalance at time:  " + System.currentTimeMillis() + "\n");

                    targetToVictimMapping.put(target.getId(), victim.getId());
                    targetToNodeMapping.put(target.getId(), executorSummaries);
                    sloObserver.clearTopologySLOs(target.getId());
                    sloObserver.clearTopologySLOs(victim.getId());

                    targetID = target.getId();
                    victimID = victim.getId();

                    writeToFile(advanced_scheduling_log, "Name of target topology: " + targetID + "\n");
                    writeToFile(advanced_scheduling_log, "Name of victim topology: " + victimID + "\n");
                    writeToFile(advanced_scheduling_log, "End of rebalanceTwoTopologies\n");


                    writeToFile(advanced_scheduling_log, "\n targetComponent  :" + targetComponent + "\n");
                  //  StormTopology topology = nimbusClient.getClient().getTopology(target.getId());

                 /*   for (Map.Entry<String, SpoutSpec> spout : topology.get_spouts().entrySet()) {
                        writeToFile(advanced_scheduling_log, "\nSpout key: :" + spout.getKey()+ "\n");
                        if (spout.getKey().equals(targetComponent)) {
                            writeToFile(advanced_scheduling_log, "\nThis should be printed only once for target.\n");
                            spout.getValue().get_common().set_parallelism_hint(targetNewParallelism);
                            writeToFile(advanced_scheduling_log, "\n Target component's parallelism after rebalance" + spout.getValue().get_common().get_parallelism_hint() + "\n");
                        }
                    }

                    for (Map.Entry<String, Bolt> bolt : topology.get_bolts().entrySet()) {
                        writeToFile(advanced_scheduling_log, "\nBolt key: :" + bolt.getKey());
                        if (bolt.getKey().equals(targetComponent)) {
                            writeToFile(advanced_scheduling_log, "\nThis should be printed only once for target.\n");
                            bolt.getValue().get_common().set_parallelism_hint(targetNewParallelism);
                            writeToFile(advanced_scheduling_log, "\n Target component's parallelism after rebalance" + bolt.getValue().get_common().get_parallelism_hint() + "\n");
                        }
                    }

                    writeToFile(advanced_scheduling_log, "\n victimComponent  :" + victimComponent + "\n");

                    for (Map.Entry<String, SpoutSpec> spout : topology.get_spouts().entrySet()) {
                        writeToFile(advanced_scheduling_log, "\nSpout key: :" + spout.getKey() + "\n");

                        if ( spout.getKey().equals(victimComponent)) {
                            writeToFile(advanced_scheduling_log, "\nThis should be printed only once for victim.\n");
                            spout.getValue().get_common().set_parallelism_hint(victimNewParallelism);
                            writeToFile(advanced_scheduling_log, "\n Victim component's parallelism after rebalance" + spout.getValue().get_common().get_parallelism_hint() + "\n");
                        }
                    }

                    for (Map.Entry<String, Bolt> bolt : topology.get_bolts().entrySet()) {
                        writeToFile(advanced_scheduling_log, "\nBolt key: :" + bolt.getKey());
                        if ( bolt.getKey().equals(victimComponent)) {
                            writeToFile(advanced_scheduling_log, "\nThis should be printed only once for victim.\n");
                            bolt.getValue().get_common().set_parallelism_hint(victimNewParallelism);
                            writeToFile(advanced_scheduling_log, "\n Victim component's parallelism after rebalance" + bolt.getValue().get_common().get_parallelism_hint() + "\n");
                        }
                    }*/

                } catch (Exception e) {
                    e.printStackTrace();
                }


            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
        }
    }

    private void removeAlreadySelectedPairs(ArrayList<String> receivers, ArrayList<String> givers) {
        for (String target : targetToVictimMapping.keySet()) {
            int targetIndex = receivers.indexOf(target);
            if (targetIndex != -1) {
                writeToFile(advanced_scheduling_log, "Receiver topology that is removed after one rebalance:  " + receivers.get(targetIndex) + "\n");
                receivers.remove(targetIndex);
            }

            String victim = targetToVictimMapping.get(target);
            int victimIndex = givers.indexOf(victim);
            if (victimIndex != -1) {
                writeToFile(advanced_scheduling_log, "Giver topology that is removed after one rebalance:  " + givers.get(victimIndex) + "\n");
                givers.remove(victimIndex);
            }

            writeToFile(advanced_scheduling_log, "Size of givers:  " + givers.size() + "\n");
            writeToFile(advanced_scheduling_log, "Size of receivers:  " + receivers.size() + "\n");

        }
    }

    private void applyRebalancedScheduling(Cluster cluster, Topologies topologies, boolean targetNeedsToBeRebalanced, boolean victimNeedsToBeRebalanced) {
        for (Map.Entry<String, String> targetToVictim : targetToVictimMapping.entrySet()) {
            final TopologyDetails target = topologies.getById(targetToVictim.getKey());
            TopologyDetails victim = topologies.getById(targetToVictim.getValue());
            ExecutorPair executorPair = targetToNodeMapping.get(targetToVictim.getKey());

            //     if (targetNeedsToBeRebalanced) reassignTargetNewScheduling(target, victim, cluster, executorPair);
            //     if (victimNeedsToBeRebalanced) reassignVictimNewScheduling(target, victim, cluster, executorPair);

            writeToFile(advanced_scheduling_log, "\n New schedule for the target is created: " + (cluster.getAssignmentById(target.getId()) != null) + "\n");
            if (cluster.getAssignmentById(target.getId()) != null)
                reassignTargetNewScheduling(target, victim, cluster, executorPair);
            writeToFile(advanced_scheduling_log, "\n New schedule for the victim is created: " + (cluster.getAssignmentById(victim.getId()) != null) + "\n");
            if (cluster.getAssignmentById(victim.getId()) != null)
                reassignVictimNewScheduling(target, victim, cluster, executorPair);


            if (targetID.length() < 1 && victimID.length() < 1) {
                Timer timer = new Timer();
                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        targetToVictimMapping.remove(target.getId());
                        targetToNodeMapping.remove(target.getId());
                    }
                }, 3*60*1000); // two minutes

            }
        }
    }

    private void reassignTargetNewScheduling(TopologyDetails target, TopologyDetails victim, Cluster cluster,
                                             ExecutorPair executorPair) {

        writeToFile(advanced_scheduling_log, "Only the target topology needs to be rescheduled. That's more normal :D ");
        ExecutorSummary targetExecutorSummary = executorPair.getTargetExecutorSummary();

        WorkerSlot targetSlot = new WorkerSlot(targetExecutorSummary.get_host(), targetExecutorSummary.get_port());

        Map<WorkerSlot, ArrayList<ExecutorDetails>> targetSchedule = globalState.getTopologySchedules().get(target.getId()).getAssignment();
        Set<ExecutorDetails> previousTargetExecutors = globalState.getTopologySchedules().get(target.getId()).getExecutorToComponent().keySet();


        writeToFile(advanced_scheduling_log, "\n************** Target Topology **************" + "\n");

        ArrayList<Integer> previousTargetTasks = new ArrayList<Integer>();
        ArrayList<Integer> currentTargetTasks = new ArrayList<Integer>();

        writeToFile(advanced_scheduling_log, "\n****** Previous Executors ******" + "\n");
        for (ExecutorDetails executorDetails : previousTargetExecutors) {
            writeToFile(advanced_scheduling_log, executorDetails.toString() + "\n");
            previousTargetTasks.add(executorDetails.getStartTask());
            previousTargetTasks.add(executorDetails.getEndTask());
        }

        writeToFile(advanced_scheduling_log, "\n********************** Previous Tasks *********************\n");
        for (Integer oldTasks : previousTargetTasks) {
            writeToFile(advanced_scheduling_log, oldTasks + "\n");
        }
        SchedulerAssignment currentTargetAssignment = cluster.getAssignmentById(target.getId());
        writeToFile(advanced_scheduling_log, "\n currentTargetAssignment != null : " + (currentTargetAssignment != null ? 1 : 0) + "\n");
        writeToFile(advanced_scheduling_log, "\n currentVictimAssignment != null : " + (cluster.getAssignmentById(victim.getId()) != null ? 1 : 0) + "\n");
        if (currentTargetAssignment != null) {
            Set<ExecutorDetails> currentTargetExecutors = currentTargetAssignment.getExecutorToSlot().keySet();

            writeToFile(advanced_scheduling_log, "\n****** Current Executors ******\n");
            for (ExecutorDetails executorDetails : currentTargetExecutors) {
                writeToFile(advanced_scheduling_log, executorDetails.toString() + "\n");
                currentTargetTasks.add(executorDetails.getStartTask());
                currentTargetTasks.add(executorDetails.getEndTask());

            }

            writeToFile(advanced_scheduling_log, "\n********************** Current Tasks *********************\n");
            for (Integer newTasks : currentTargetTasks) {
                writeToFile(advanced_scheduling_log, newTasks + "\n");
            }

            currentTargetExecutors.removeAll(previousTargetExecutors);
            currentTargetTasks.removeAll(previousTargetTasks);

            writeToFile(advanced_scheduling_log, "\n********************** Found new executor *********************\n");
            for (ExecutorDetails newExecutor : currentTargetExecutors) {
                writeToFile(advanced_scheduling_log, newExecutor.toString() + "\n");
            }

            writeToFile(advanced_scheduling_log, "\n********************** Found new tasks *********************\n");
            for (Integer newTasks : currentTargetTasks) {
                writeToFile(advanced_scheduling_log, newTasks + "\n");
            }

            writeToFile(advanced_scheduling_log, "Old target schedule\n");

            for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> topologyEntry : targetSchedule.entrySet()) {
                writeToFile(advanced_scheduling_log, "Worker: " + topologyEntry.getKey() + " Executors: " + topologyEntry.getValue().toString() + "\n");

                if (topologyEntry.getKey().equals(targetSlot)) {
                    ArrayList<ExecutorDetails> executorsOfOldTarget = topologyEntry.getValue();
                    executorsOfOldTarget.addAll(currentTargetExecutors);
                    targetSchedule.put(targetSlot, executorsOfOldTarget);
                }
            }
        }
        writeToFile(advanced_scheduling_log, "New Assignment for Target Topology: \n");
        for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> topologyEntry : targetSchedule.entrySet()) {
            writeToFile(advanced_scheduling_log, "Worker: " + topologyEntry.getKey() + " Executors: " + topologyEntry.getValue().toString() + "\n");

            if (cluster.getUsedSlots().contains(topologyEntry.getKey())) {
                writeToFile(advanced_scheduling_log, "Worker Slot is already occupied \n");
                writeToFile(advanced_scheduling_log, "Checking if old schedule matches the new schedule \n");

                cluster.freeSlot(topologyEntry.getKey());
            }
            cluster.assign(topologyEntry.getKey(), target.getId(), topologyEntry.getValue());
        }

        targetID = new String();

    }


    private void reassignVictimNewScheduling(TopologyDetails target, TopologyDetails victim, Cluster cluster,
                                             ExecutorPair executorPair) {

        writeToFile(advanced_scheduling_log, "Only the victim topology needs to be rescheduled. Woot, we made it to stage II");

        Map<WorkerSlot, ArrayList<ExecutorDetails>> victimSchedule = globalState.getTopologySchedules().get(victim.getId()).getAssignment();


        ExecutorSummary victimExecutorSummary = executorPair.getVictimExecutorSummary();

        WorkerSlot victimSlot = new WorkerSlot(victimExecutorSummary.get_host(), victimExecutorSummary.get_port());


        writeToFile(advanced_scheduling_log, "\n************** Victim Topology **************\n");

        Set<ExecutorDetails> previousVictimExecutors = globalState.getTopologySchedules().get(victim.getId()).getExecutorToComponent().keySet();
        SchedulerAssignment currentVictimAssignment = cluster.getAssignmentById(victim.getId());
        writeToFile(advanced_scheduling_log, "\n currentVictimAssignment != null : " + (currentVictimAssignment != null ? 1 : 0) + "\n");
        writeToFile(advanced_scheduling_log, "\n currentTargetAssignment != null : " + (cluster.getAssignmentById(target.getId()) != null ? 1 : 0) + "\n");
        ArrayList<Integer> previousVictimTasks = new ArrayList<Integer>();
        ArrayList<Integer> otherTaskofVictimExecutor = new ArrayList<Integer>();
        writeToFile(advanced_scheduling_log, "\n****** Previous Executors ******\n");
        for (ExecutorDetails executorDetails : previousVictimExecutors) {
            writeToFile(advanced_scheduling_log, executorDetails.toString() + "\n");
            previousVictimTasks.add(executorDetails.getStartTask());
            previousVictimTasks.add(executorDetails.getEndTask());
        }

        if (currentVictimAssignment != null) {
            Set<ExecutorDetails> currentVictimExecutors = currentVictimAssignment.getExecutorToSlot().keySet();

            writeToFile(advanced_scheduling_log, "\n****** Current Executors ******");
            ArrayList<Integer> currentVictimTasks = new ArrayList<Integer>();
            for (ExecutorDetails executorDetails : currentVictimExecutors) {
                writeToFile(advanced_scheduling_log, executorDetails.toString() + "\n");
                currentVictimTasks.add(executorDetails.getStartTask());
                currentVictimTasks.add(executorDetails.getEndTask());
            }

            previousVictimExecutors.removeAll(currentVictimExecutors);
            previousVictimTasks.removeAll(currentVictimTasks);
            writeToFile(advanced_scheduling_log, "********************** Removed executor *********************\n");
            for (ExecutorDetails newExecutor : previousVictimExecutors) {

                writeToFile(advanced_scheduling_log, newExecutor.toString() + "\n");
            }

            writeToFile(advanced_scheduling_log, "********************** Removed tasks *********************\n");
            for (Integer removedTasks : previousVictimTasks) {

                writeToFile(advanced_scheduling_log, removedTasks + "\n");
            }

            writeToFile(advanced_scheduling_log, "Old victim schedule\n");
            for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> topologyEntry : victimSchedule.entrySet()) {
                writeToFile(advanced_scheduling_log, "Worker: " + topologyEntry.getKey() + " Executors: " + topologyEntry.getValue().toString() + "\n");

           /*     for (ExecutorDetails executorsToRemove : previousVictimExecutors) {
                    writeToFile(advanced_scheduling_log, "\nVictim to remove: " + executorsToRemove.toString());
                    if (topologyEntry.getValue().contains(executorsToRemove)) {
                        ArrayList<ExecutorDetails> executorsOfOldVictim = topologyEntry.getValue();
                        executorsOfOldVictim.remove(executorsToRemove);
                        victimSchedule.put(topologyEntry.getKey(), executorsOfOldVictim);
                    }
                }*/

                for (Integer removedTasks : previousVictimTasks) {

                    ArrayList <ExecutorDetails> oldExecutors = topologyEntry.getValue();
                    Iterator iterator =  oldExecutors.iterator();

                    while (iterator.hasNext())  {// java.util.ConcurrentModificationException
                        ExecutorDetails oldExecutor =  (ExecutorDetails)iterator.next();
                        if (removedTasks ==  oldExecutor.getStartTask()) {
                            otherTaskofVictimExecutor.add(oldExecutor.getEndTask());
                           // ArrayList<ExecutorDetails> executorsOfOldVictim = topologyEntry.getValue();
                           // oldExecutorsTemp .remove(oldExecutor);
                            iterator.remove();
                         //   victimSchedule.put(topologyEntry.getKey(), executorsOfOldVictim);
                        } else if (removedTasks == oldExecutor.getEndTask()) {
                            otherTaskofVictimExecutor.add(oldExecutor.getStartTask());
                           // ArrayList<ExecutorDetails> executorsOfOldVictim = topologyEntry.getValue();
                         //   oldExecutorsTemp .remove(oldExecutor);
                            iterator.remove();
                          //  victimSchedule.put(topologyEntry.getKey(), executorsOfOldVictim);
                        }

                    }
                    victimSchedule.put(topologyEntry.getKey(), oldExecutors);
                }
            }

            for (Map.Entry<ExecutorDetails, WorkerSlot> topologyEntry : currentVictimAssignment.getExecutorToSlot().entrySet()) {
                writeToFile(advanced_scheduling_log, "Worker: " + topologyEntry.getKey() + " Executors: " + topologyEntry.getValue().toString() + "\n");
                for (Integer taskToAddBack : otherTaskofVictimExecutor) {
                    if (taskToAddBack == topologyEntry.getKey().getStartTask() || taskToAddBack == topologyEntry.getKey().getEndTask()) {
                        victimSchedule.get(victimSlot).add(topologyEntry.getKey());
                    }
                }
            }


        }
        writeToFile(advanced_scheduling_log, "\nNew Assignment for Victim Topology: \n");

        for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> topologyEntry : victimSchedule.entrySet()) {
            writeToFile(advanced_scheduling_log, "Worker: " + topologyEntry.getKey() + " Executors: " + topologyEntry.getValue().toString() + "\n");

            if (cluster.getUsedSlots().contains(topologyEntry.getKey())) {
                writeToFile(advanced_scheduling_log, "Worker Slot is already occupied \n");
                writeToFile(advanced_scheduling_log, "Checking if old schedule matches the new schedule \n");

                cluster.freeSlot(topologyEntry.getKey());
            }
            cluster.assign(topologyEntry.getKey(), victim.getId(), topologyEntry.getValue());
        }
        victimID = new String();


    }

    public void writeToFile(File file, String data) {
        try {
            FileWriter fileWriter = new FileWriter(file, true);
            BufferedWriter bufferWriter = new BufferedWriter(fileWriter);
            bufferWriter.append(data);
            bufferWriter.close();
            fileWriter.close();
            LOG.info("wrote to file {}", data);
        } catch (IOException ex) {
            LOG.info("error! writing to file {}", ex);
        }
    }
}