package backtype.storm.scheduler.advancedstela;

import backtype.storm.generated.ExecutorSummary;
import backtype.storm.scheduler.*;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.advancedstela.etp.*;
import backtype.storm.scheduler.advancedstela.slo.*;
import backtype.storm.scheduler.advancedstela.slo.Observer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

// things to do -- if compaction does occur, you do need to intercept the schedule and provide separate scheduling for the compaction

public class AdvancedStelaScheduler implements IScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(AdvancedStelaScheduler.class);
    private static final Integer OBSERVER_RUN_INTERVAL = 30;

    @SuppressWarnings("rawtypes")

    private Map config;
    private Observer sloObserver;
    private GlobalState globalState;
    private GlobalStatistics globalStatistics;
    private Selector selector;
    private HashMap<String, ExecutorPair> targets, victims;
    private File juice_log;
    private File same_top;
    final int numExecutorsExchanged = 1;

    public void prepare(@SuppressWarnings("rawtypes") Map conf) {
        juice_log = new File("/tmp/output.log");
        same_top = new File("/tmp/same_top.log");
        config = conf;
        sloObserver = new Observer(conf);
        globalState = new GlobalState(conf);
        globalStatistics = new GlobalStatistics(conf);
        selector = new Selector();
        victims = new HashMap<String, ExecutorPair>();
        targets = new HashMap<String, ExecutorPair>();
    }

    public void schedule(Topologies topologies, Cluster cluster) {
        logUnassignedExecutors(cluster.needsSchedulingTopologies(topologies), cluster);
        int numTopologiesThatNeedScheduling = cluster.needsSchedulingTopologies(topologies).size();
        int numTopologies = topologies.getTopologies().size();

        if (numTopologiesThatNeedScheduling > 0) {

            StringBuffer sb = new StringBuffer();
            sb.append("cluster.needsSchedulingTopologies(topologies).size() > 0\n");
            sb.append("Before calling EvenScheduler: \n");
            sb.append("Size of cluster.needsSchedulingTopologies(topologies): " + cluster.needsSchedulingTopologies(topologies).size() + "\n");
            List<TopologyDetails> topologiesScheduled = cluster.needsSchedulingTopologies(topologies);
            sb.append("targets.length() : " + targets.size() + "\n");
            sb.append("victims.length(): " + victims.size() + "\n");

            for (TopologyDetails topologyThatNeedsToBeScheduled : topologiesScheduled) {
                Collection<ExecutorDetails> unscheduledExecutors =  cluster.getUnassignedExecutors(topologyThatNeedsToBeScheduled);
                writeToFile(same_top, " Topology that needs to be scheduled " + topologyThatNeedsToBeScheduled.getName() + "\n");
                writeToFile(same_top,  "Is there an assignment for this topology: " + (cluster.getAssignmentById(topologyThatNeedsToBeScheduled.getId()) != null)  + "\n");
                writeToFile(same_top, " Number of unassigned executors " + unscheduledExecutors.size() + "\n");
                for (ExecutorDetails unscheduledExecutor: unscheduledExecutors ) {
                    writeToFile(same_top, " Unassigned executor "+ unscheduledExecutor.toString() + "\n");
                }
            }

            for (TopologyDetails topologyThatNeedsToBeScheduled : topologiesScheduled) {
                sb.append("Id of topology: " + topologyThatNeedsToBeScheduled.getId() + "\n");
            }

            if (!targets.isEmpty()) {
                sb.append("!targets.isEmpty()\n");
                decideAssignmentForTargets(topologies, cluster);
                targets.clear();
            }
            if (!victims.isEmpty()) {
                sb.append("!victims.isEmpty()\n");
                decideAssignmentForVictims(topologies, cluster);
                victims.clear();
            }
            writeToFile(same_top, sb.toString());
            new backtype.storm.scheduler.EvenScheduler().schedule(topologies, cluster);
            runAdvancedStelaComponents(cluster, topologies);
        } else if (numTopologiesThatNeedScheduling == 0 && numTopologies > 0) {
            runAdvancedStelaComponents(cluster, topologies);

            TopologyPairs topologiesToBeRescaled = sloObserver.getTopologiesToBeRescaled();

            ArrayList<String> receivers = topologiesToBeRescaled.getReceivers();
            ArrayList<String> givers = topologiesToBeRescaled.getGivers();

            ArrayList<Topology> receiver_topologies = new ArrayList<Topology>();
            ArrayList<Topology> giver_topologies = new ArrayList<Topology>();

            for (int i = 0; i < receivers.size(); i++)
                receiver_topologies.add(sloObserver.getTopologyById(receivers.get(i)));

            for (int i = 0; i < givers.size(); i++)
                giver_topologies.add(sloObserver.getTopologyById(givers.get(i)));
            removeAlreadySelectedPairs(receivers, givers);
            ArrayList<String> removedTopologies = new ArrayList<>();

            for (Topology receiver : receiver_topologies) {

                if (receiver.getSensitivity() != null && receiver.getSensitivity().equals("latency")) {
                    {
                        // check congestionmap size and then make a decision.
                        TopologySchedule receiverSchedule = globalState.getTopologySchedules().get(receiver.getId());
                        TopologyStatistics receiverStatistics = globalStatistics.getTopologyStatistics().get(receiver.getId());
                        LatencyStrategyWithCapacity receiverStrategy = new LatencyStrategyWithCapacity(receiverSchedule, receiverStatistics, receiver);

                        if (!receiverStrategy.isThereCongestion()) {
                            compactLatencySensitiveTopology(topologies.getById(receiver.getId()), receiverSchedule);
                            removedTopologies.add(receiver.getId());
                        }
                    }
                }
            }

            // cleaning up receivers from the receiver_topologies
            if (removedTopologies.size() > 0) {
                for (String ID : removedTopologies) {
                    Iterator<Topology> index = receiver_topologies.iterator();
                    while (index.hasNext()) {
                        Topology t = index.next();
                        if (t.getId().equals(ID)) {
                            System.out.println("Size of receiver topologies before removal: " + receiver_topologies.size());
                            System.out.println("Removing a topology");
                            index.remove();
                            System.out.println("Size of receiver topologies after removal: " + receiver_topologies.size());
                        }
                    }
                }
            }

            if (receiver_topologies.size() > 0 && giver_topologies.size() > 0) {

                ArrayList<String> topologyPair = new TopologyPicker().classBasedStrategy(receiver_topologies, giver_topologies);//unifiedStrategy(receiver_topologies, giver_topologies);//.worstTargetBestVictim(receivers, givers);
                String receiver = topologyPair.get(0);
                String giver = topologyPair.get(1);
                TopologyDetails target = topologies.getById(receiver);
                TopologySchedule targetSchedule = globalState.getTopologySchedules().get(receiver);
                TopologyDetails victim = topologies.getById(giver);
                TopologySchedule victimSchedule = globalState.getTopologySchedules().get(giver);
                ExecutorPair executorSummaries =
                        selector.selectPair(globalState, globalStatistics, sloObserver.getTopologyById(receiver), sloObserver.getTopologyById(giver));

                if (executorSummaries != null && executorSummaries.bothPopulated()) {
                    rebalanceTwoTopologies(target, targetSchedule, victim, victimSchedule, executorSummaries);
                }
            } else if (giver_topologies.size() == 0) {
                StringBuffer sb = new StringBuffer();
                sb.append("There are no givers! *Sob* \n");
                sb.append("Receivers:  \n");

                for (int i = 0; i < receiver_topologies.size(); i++)
                    sb.append(receiver_topologies.get(i).getId() + "\n");
            } else if (receiver_topologies.size() == 0) {
                StringBuffer sb = new StringBuffer();
                sb.append("There are no receivers! *Sob* \n");
                sb.append("Givers:  \n");

                for (int i = 0; i < giver_topologies.size(); i++)
                    sb.append(giver_topologies.get(i).getId() + "\n");
            }
        }
    }

    private void decideAssignmentForTargets(Topologies topologies, Cluster cluster) {
        List<TopologyDetails> unscheduledTopologies = cluster.needsSchedulingTopologies(topologies);
        for (TopologyDetails topologyDetails : unscheduledTopologies) {
            if (targets.containsKey(topologyDetails.getId()) /* && cluster.getAssignmentById(topologyDetails.getId()) != null*/) // BY REMOVING THIS, WE CAN FIX THE BUG
            {
                findAssignmentForTarget(topologyDetails, cluster, topologyDetails.getId());
            }
        }
    }

    private void decideAssignmentForVictims(Topologies topologies, Cluster cluster) {
        List<TopologyDetails> unscheduledTopologies = cluster.needsSchedulingTopologies(topologies);

        for (TopologyDetails topologyDetails : unscheduledTopologies) {
            if (victims.containsKey(topologyDetails.getId()) /* && cluster.getAssignmentById(topologyDetails.getId()) != null()*/) // BY REMOVING THIS SECOND PART, WE CAN FIX THE BUG
            {
                findAssignmentForVictim(topologyDetails, cluster, topologyDetails.getId());
            }
        }
    }

    private void findAssignmentForTarget(TopologyDetails target, Cluster cluster, String topologyId) {
        ExecutorPair executorPair = targets.get(topologyId);
        reassignTargetNewScheduling(target, cluster, executorPair);
    }

    private void findAssignmentForVictim(TopologyDetails victim, Cluster cluster, String topologyId) {
        ExecutorPair executorPair = victims.get(topologyId);
        reassignVictimNewScheduling(victim, cluster, executorPair);

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
            StringBuffer forOutputLog = new StringBuffer();
            if (unassignedExecutors.size() > 0) {
                for (ExecutorDetails executorDetails : unassignedExecutors) {
                    forOutputLog.append("executorDetails.toString(): " + executorDetails.toString() + "\n");
                }
            }
        }
    }


    public void compactLatencySensitiveTopology(TopologyDetails targetDetails, TopologySchedule target) { // reduce by two workers at a time and stop at two
        Topology targetTopology = sloObserver.getTopologyById(target.getId());
        Long numWorkers = targetTopology.getWorkers();
        if (numWorkers <= 2) {
            System.out.println(target.getId() + "Topology is only distributed across two machines. Can't do no more bro :D.");
        } else {
            String targetCommand = new String();
            Long workers;
            writeToFile(juice_log, "/var/nimbus/storm/bin/storm old-workers: " + numWorkers + "\n");
            if (numWorkers >= 4) workers = numWorkers - 2; // decreasing in increments of two
            else workers = 2L; // goes from 3 to
            //   if (numWorkers < numMachines) {
            targetCommand = "/var/nimbus/storm/bin/storm " +
                    "rebalance " + targetDetails.getName() + " -n " +
                    workers;
            targetTopology.setWorkers(workers);
            writeToFile(juice_log, System.currentTimeMillis() + "\n");
            writeToFile(juice_log, targetCommand + "\n");
            try {
                Runtime.getRuntime().exec(targetCommand);
            } catch (IOException ex) {
                System.out.println("Could not reduce the number of workers :/");
            }
            /* TODO we don't place in targets here because we don't need to intercept the schedule.
            If you need to intercept the schedule for compaction -- place in something other than targets */
            sloObserver.updateLastRebalancedTime(target.getId(), System.currentTimeMillis() / 1000);
            sloObserver.clearTopologySLOs(target.getId());
        }
    }


    private void rebalanceTwoTopologies(TopologyDetails targetDetails, TopologySchedule target,
                                        TopologyDetails victimDetails, TopologySchedule victim, ExecutorPair executorSummaries) {
        if (config != null) {
            try {
                String targetComponent = executorSummaries.getTargetExecutorSummary().get_component_id();
                Integer targetOldParallelism = target.getComponents().get(targetComponent).getParallelism();
                Integer targetNewParallelism = targetOldParallelism + numExecutorsExchanged ;
                String targetCommand = "/var/nimbus/storm/bin/storm " +
                        "rebalance " + targetDetails.getName() + " -e " +
                        targetComponent + "=" + targetNewParallelism;
                target.getComponents().get(targetComponent).setParallelism(targetNewParallelism);

                String victimComponent = executorSummaries.getVictimExecutorSummary().get_component_id();
                Integer victimOldParallelism = victim.getComponents().get(victimComponent).getParallelism();
                Integer victimNewParallelism = victimOldParallelism - numExecutorsExchanged ;
                String victimCommand = "/var/nimbus/storm/bin/storm " +
                        "rebalance " + victimDetails.getName() + " -e " +
                        victimComponent + "=" + victimNewParallelism;

                victim.getComponents().get(victimComponent).setParallelism(victimNewParallelism);
                // FORMAT /var/nimbus/storm/bin/storm henge-rebalance  production-topology1 -e bolt_output_sink=13 xyz production-topology2 -e spout_head=12 xyz  production-topology3 -e bolt_output_sink=13 xyz production-topology4 -e spout_head=12
                try {
                    writeToFile(juice_log, targetCommand + "\n");
                    writeToFile(juice_log, System.currentTimeMillis() + "\n");
                    writeToFile(juice_log, victimCommand + "\n");

                    Runtime.getRuntime().exec(targetCommand);
                    Runtime.getRuntime().exec(victimCommand);

                    sloObserver.updateLastRebalancedTime(target.getId(), System.currentTimeMillis() / 1000);
                    sloObserver.updateLastRebalancedTime(victim.getId(), System.currentTimeMillis() / 1000);

                    targets.put(targetDetails.getId(), executorSummaries);
                    victims.put(victimDetails.getId(), executorSummaries);

                    sloObserver.clearTopologySLOs(target.getId());
                    sloObserver.clearTopologySLOs(victim.getId());
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
        for (String target : targets.keySet()) {
            int targetIndex = receivers.indexOf(target);
            if (targetIndex != -1) {
                receivers.remove(targetIndex);
            }
        }
        for (String victim : victims.keySet()) {
            int victimIndex = givers.indexOf(victim);
            if (victimIndex != -1) {
                givers.remove(victimIndex);
            }
        }
    }

    private void reassignTargetNewScheduling(TopologyDetails target, Cluster cluster,
                                             ExecutorPair executorPair) {

        Map<WorkerSlot, ArrayList<ExecutorDetails>> targetSchedule = globalState.getTopologySchedules().get(target.getId()).getAssignment();
        ExecutorSummary targetExecutorSummary = executorPair.getTargetExecutorSummary();

        WorkerSlot targetSlot = new WorkerSlot(targetExecutorSummary.get_host(), targetExecutorSummary.get_port());
        String component = targetExecutorSummary.get_component_id();
        int componentParallelism = globalState.getTopologySchedules().get(target.getId()).getComponents().get(component).getParallelism(); // TODO break up this line
        List<ExecutorDetails> targetExecutors = globalState.getTopologySchedules().get(target.getId()).getComponents().get(component).getExecutorDetails();

        // get all the tasks
        ArrayList <Integer> targetTasks = new ArrayList<>();
        for (ExecutorDetails targetExecutor: targetExecutors) {
            targetTasks.add(targetExecutor.getStartTask());
            targetTasks.add(targetExecutor.getEndTask());
        }
        // all of them should be unique -- should now be sorted
        Collections.sort(targetTasks);
        int totalTasks = targetTasks.size();
        double tasksPerExecutor = Math.floor(new Double(totalTasks) / new Double(componentParallelism));

        Map<WorkerSlot, ArrayList<ExecutorDetails>> newExecutorToSlot = new HashMap <>();


        ArrayList<ExecutorDetails> targetExecutorDetails = new ArrayList<>();

        for (int i = 0; i < componentParallelism; i ++) {
            int startingTask = i * (int)tasksPerExecutor ;
            int endingTask  = (i + 1) * (int)tasksPerExecutor;
            if (endingTask > totalTasks)
                endingTask = totalTasks - 1; // because starts from 0

            targetExecutorDetails.add(new ExecutorDetails(startingTask, endingTask));
        }
        newExecutorToSlot.put(targetSlot, targetExecutorDetails);

        // copy old components part that are not on this slot
        for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> slotSchedule : targetSchedule.entrySet() ) {
            if (!slotSchedule.getKey().equals(targetSlot)) {
                newExecutorToSlot.put(slotSchedule.getKey(), slotSchedule.getValue()); // load the other parts
            }
        }


        // so currently, we are changing the config for all worker slots of this topology
        // TODO skip the upper loop and do this only for the target slot
        for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> topologyEntry : newExecutorToSlot.entrySet()) {
            if (cluster.getUsedSlots().contains(topologyEntry.getKey())) {
                cluster.freeSlot(topologyEntry.getKey());
            }
            cluster.assign(topologyEntry.getKey(), target.getId(), topologyEntry.getValue());
        }
        targets.remove((target.getId()));
    }


    private void printSchedule(SchedulerAssignment currentTargetAssignment, String tag ) {
        Map<ExecutorDetails, WorkerSlot> map = currentTargetAssignment.getExecutorToSlot();
        writeToFile(same_top, "Logging for " + tag + "\n");
        for (Entry<ExecutorDetails, WorkerSlot> e : map.entrySet()) {
            writeToFile(same_top, "WorkerSlot : " + e.getValue() + " Executor {}" + e.getKey().toString() + "\n");
        }
    }

    private void reassignVictimNewScheduling(TopologyDetails victim, Cluster cluster,
                                             ExecutorPair executorPair) {
        Map<WorkerSlot, ArrayList<ExecutorDetails>> victimSchedule = globalState.getTopologySchedules().get(victim.getId()).getAssignment();
        ExecutorSummary victimExecutorSummary = executorPair.getVictimExecutorSummary();
        String component = victimExecutorSummary.get_component_id();
        WorkerSlot victimSlot = new WorkerSlot(victimExecutorSummary.get_host(), victimExecutorSummary.get_port());

        ArrayList <ExecutorDetails> victimSlotExecutors = victimSchedule.get(victimSlot); // these are the executors on the victim slot

        Map<ExecutorDetails, String> victimExectorsToComponents = victim.getExecutorToComponent();
        ArrayList <ExecutorDetails> victimComponentExecutors = new ArrayList<>();

        for (Map.Entry<ExecutorDetails, String> victimExecutorToComponent: victimExectorsToComponents.entrySet()) {
            writeToFile(same_top, "component: " + component + "\n");
            writeToFile(same_top, "component we are iterating on : " + victimExecutorToComponent.getValue() + "\n");
            if (victimExecutorToComponent.getValue().equals(component)) {
                victimComponentExecutors.add(victimExecutorToComponent.getKey()); // this gives us all of the executors that belong to that component
                writeToFile(same_top, "executor details: " + victimExecutorToComponent.getKey().toString() + "\n");
            }
        }
        // do a deep copy
        ArrayList <ExecutorDetails> victimNonComponentExecutorsOnSlot = new ArrayList<>();
        for (ExecutorDetails ed : victimSlotExecutors) {
            victimNonComponentExecutorsOnSlot.add(ed);
        }

        victimNonComponentExecutorsOnSlot.removeAll(victimComponentExecutors);
        // these are all the executors that are on the slot but do not belong to the component in question
        if (victimSlotExecutors != null) {
            ///  we have to find the common subset between them

            victimComponentExecutors.retainAll(victimSlotExecutors); // now we have the common elements only.
            int oldNumExecutors = victimComponentExecutors.size();
            int newNumExecutors = oldNumExecutors - numExecutorsExchanged;
            if (newNumExecutors > 0) {
                ArrayList <Integer> victimTasks = new ArrayList<>();
                for (ExecutorDetails victimExecutor: victimComponentExecutors) {
                    victimTasks.add(victimExecutor.getStartTask());
                    victimTasks.add(victimExecutor.getEndTask());
                }
                Collections.sort(victimTasks);
                int start = victimTasks.get(0); // assumption -- values are contiguous
                int end = victimTasks.get(victimTasks.size()-1);
                int range = (int) Math.floor((double)(end-start)/(newNumExecutors));
                if (range == 0) range = 1;

                ArrayList <ExecutorDetails> newVictimComponentExecutorsOnSlot = new ArrayList<>();
                for (int i = 0; i < newNumExecutors; i++){
                    int startingTask = start + i * range;
                    int endingTask = start  + (i + 1) * range; // assumption that range needs to be continuous
                    if (endingTask > end)
                        endingTask = end;
                    newVictimComponentExecutorsOnSlot.add(new ExecutorDetails(startingTask, endingTask));
                } // now we have set up the tasks for the executor

                for (ExecutorDetails ed : victimNonComponentExecutorsOnSlot) {
                    newVictimComponentExecutorsOnSlot.add(ed); // add the other executors that were on this slot but do not belong to this component
                }

                //for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> topologyEntry : newExecutorToSlot.entrySet()) {
                if (cluster.getUsedSlots().contains(victimSlot)) {
                    cluster.freeSlot(victimSlot);
                }
                cluster.assign(victimSlot, victim.getId(), newVictimComponentExecutorsOnSlot);
                // }
                victims.remove((victim.getId()));

            } else {
                writeToFile(same_top, "Num of new executors is now zero :S");
            }
            // get all the tasks


            // all of them should be unique -- should now be sorted

    /*    // copy old components part that are not on this slot
        for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> slotSchedule : victimSchedule.entrySet() ) {
            if (!slotSchedule.getKey().equals(victimSlot)) {
                newExecutorToSlot.put(slotSchedule.getKey(), slotSchedule.getValue()); // load the other parts
            }
        }
*/
            // so currently, we are changing the config for all worker slots of this topology
            // TODO skip the upper loop and do this only for the target slot

        } else {
            writeToFile(same_top, "the old victims schedule does not have this worker slot.");

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