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
    private HashMap<String, Integer> targetsExecutorsCount, victimsExecutorsCount;
    private File juice_log;
    private File same_top;
    private int count;
    // final int numExecutorsExchanged = 1;

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
        targetsExecutorsCount = new HashMap<String, Integer>();
        victimsExecutorsCount = new HashMap<String, Integer>();
    }

    public void schedule(Topologies topologies, Cluster cluster) {
        logUnassignedExecutors(cluster.needsSchedulingTopologies(topologies), cluster);
        int numTopologiesThatNeedScheduling = cluster.needsSchedulingTopologies(topologies).size();
        int numTopologies = topologies.getTopologies().size();

        for (String target : targets.keySet()) {
            LOG.info("target {} ", target);
        }
        for (String victim : victims.keySet()) {
            LOG.info("victim {} ", victim);
        }

        runAdvancedStelaComponents(cluster, topologies);
        if (/*victims.isEmpty() && targets.isEmpty() &&*/ numTopologiesThatNeedScheduling > 0) {
            LOG.info("STORM IS GOING TO PERFORM THE REBALANCING");
            LOG.info("Phase 2");
            new backtype.storm.scheduler.EvenScheduler().schedule(topologies, cluster);
        } else if ((victims.isEmpty() && targets.isEmpty()) && numTopologiesThatNeedScheduling == 0 && numTopologies > 0) {
            LOG.info("((victims.isEmpty() && targets.isEmpty()) && numTopologiesThatNeedScheduling == 0 && numTopologies > 0)");
            LOG.info("Phase 1");
            LOG.info("Printing topology schedules");
            Collection<TopologyDetails> topologyDetails = topologies.getTopologies();
            for (TopologyDetails detail : topologyDetails) {
                printSchedule(cluster.getAssignmentById(detail.getId()), detail.getId());
            }

            TopologyPairs topologiesToBeRescaled = sloObserver.getTopologiesToBeRescaled();

            ArrayList<String> receivers = topologiesToBeRescaled.getReceivers();
            ArrayList<String> givers = topologiesToBeRescaled.getGivers();

            ArrayList<Topology> receiver_topologies = new ArrayList<Topology>();
            ArrayList<Topology> giver_topologies = new ArrayList<Topology>();

            for (int i = 0; i < receivers.size(); i++)
                receiver_topologies.add(sloObserver.getTopologyById(receivers.get(i)));

            for (int i = 0; i < givers.size(); i++)
                giver_topologies.add(sloObserver.getTopologyById(givers.get(i)));

            //removeAlreadySelectedPairs(receivers, givers);

            LOG.info("Length of givers {}", giver_topologies.size());
            LOG.info("Length of receivers {}", receiver_topologies.size());

            // IGNORING LATENCY STUFF FOR NOW
     /*       ArrayList<String> removedTopologies = new ArrayList<>();

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
            } */

            if (receiver_topologies.size() > 0 && giver_topologies.size() > 0) {

                ArrayList<String> topologyPair = new TopologyPicker().classBasedStrategy(receiver_topologies, giver_topologies);//unifiedStrategy(receiver_topologies, giver_topologies);//.worstTargetBestVictim(receivers, givers);
                String receiver = topologyPair.get(0);
                String giver = topologyPair.get(1);

                LOG.info("Picked the first two topologies for rebalance");

                TopologyDetails target = topologies.getById(receiver);
                TopologySchedule targetSchedule = globalState.getTopologySchedules().get(receiver);
                TopologyDetails victim = topologies.getById(giver);
                TopologySchedule victimSchedule = globalState.getTopologySchedules().get(giver);
                ExecutorPair executorSummaries =
                        selector.selectPair(globalState, globalStatistics, sloObserver.getTopologyById(receiver), sloObserver.getTopologyById(giver));

                LOG.info("target before rebalanceTwoTopologies {} ", target.getId());
                LOG.info("victim before rebalanceTwoTopologies {}", victim.getId());

                if (executorSummaries != null && executorSummaries.bothPopulated()) {
                    LOG.info("target host {} target port {} before rebalance", executorSummaries.getTargetExecutorSummary().get_host(), executorSummaries.getTargetExecutorSummary().get_port());
                        rebalanceTwoTopologies(target, targetSchedule, victim, victimSchedule, executorSummaries, cluster);
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
        } else if ((!victims.isEmpty() || !targets.isEmpty()) && numTopologies > 0  && numTopologiesThatNeedScheduling == 0) {
            LOG.info("((!victims.isEmpty() || !targets.isEmpty()) && numTopologies > 0  && numTopologiesThatNeedScheduling == 0)");
            LOG.info("Phase 3");
            // do the rebalancing the way we want it

            for (Map.Entry<String, ExecutorPair> victim: victims.entrySet()) {
                LOG.info("Looping through victims : {}", victim);
                String victimName = victim.getKey();
                if (victimsExecutorsCount.containsKey(victim)) {
                    LOG.info("Looping through victims : {}", victimsExecutorsCount.get(victimName));
                    int newExecutors = cluster.getAssignments().get(victimName).getExecutors().size();
                    if (newExecutors > victimsExecutorsCount.get(victimName)) {
                        findAssignmentForVictim(topologies.getById(victimName), cluster, victimName);
                    }
                }
            }

            for (Map.Entry<String, ExecutorPair> target: targets.entrySet()) {
                LOG.info("Looping through targets : {}", target);
                String targetName = target.getKey();
                if (targetsExecutorsCount.containsKey(target)) {
                    LOG.info("Looping through targets : {}", targetsExecutorsCount.get(targetName));
                    int newExecutors = cluster.getAssignments().get(targetName).getExecutors().size();
                    if (newExecutors > victimsExecutorsCount.get(targetName)) {
                        findAssignmentForTarget(topologies.getById(targetName), cluster, targetName);
                    }
                }
            }
        }
    }

    private void findAssignmentForTarget(TopologyDetails target, Cluster cluster, String topologyId) {
        try {
            ExecutorPair executorPair = targets.get(topologyId);
            LOG.info("findAssignment for Target " + executorPair.getTargetExecutorSummary().get_host() + " " + executorPair.getTargetExecutorSummary().get_port() + "\n");
            reassignNewScheduling(target, cluster, 1, "target", executorPair.getTargetExecutorSummary());
            targets.remove((target.getId()));
            targetsExecutorsCount.remove((target.getId()));
        } catch (Exception e) {
            LOG.info("Exception in findAssignment for target {}", e.toString());
        }

    }

    private void findAssignmentForVictim(TopologyDetails victim, Cluster cluster, String topologyId) {
        try{
            ExecutorPair executorPair = victims.get(topologyId);
            LOG.info("findAssignment for Victim " + executorPair.getVictimExecutorSummary().get_host() + " " + executorPair.getVictimExecutorSummary().get_port() + "\n");
            reassignNewScheduling(victim, cluster, -1, "victim", executorPair.getVictimExecutorSummary());
            victims.remove((victim.getId()));
            victimsExecutorsCount.remove((victim.getId()));
        } catch (Exception e) {
            LOG.info("Exception in find assignment for victim {}", e.toString());
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
            LOG.info("Logging unassigned executors", topologyDetails.getName());
            LOG.info("Topology name{}", topologyDetails.getName());
            Collection<ExecutorDetails> unassignedExecutors = cluster.getUnassignedExecutors(topologyDetails);

            if (unassignedExecutors.size() > 0) {
                for (ExecutorDetails executorDetails : unassignedExecutors) {
                    LOG.info("executorDetails.toString(): " + executorDetails.toString() + "\n");
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
                        "rebalance " + targetDetails.getName() + " -w 0 -e " +
                        targetComponent + "=" + targetNewParallelism;
                target.getComponents().get(targetComponent).setParallelism(targetNewParallelism);

                String victimComponent = executorSummaries.getVictimExecutorSummary().get_component_id();
                Integer victimOldParallelism = victim.getComponents().get(victimComponent).getParallelism();
                Integer victimNewParallelism = victimOldParallelism - one;
                String victimCommand = "/var/nimbus/storm/bin/storm " +
                        "rebalance " + victimDetails.getName() + " -w 0 -e " +
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

                    LOG.info("running the rebalance using storm's rebalance command \n");
                    targets.put(targetDetails.getId(), executorSummaries);
                    victims.put(victimDetails.getId(), executorSummaries);

                    LOG.info("Target old executors count {}", targetDetails.getExecutors().size());
                    targetsExecutorsCount.put(targetDetails.getId(), targetDetails.getExecutors().size());
                    LOG.info("Victim old executors count {}", victimDetails.getExecutors().size());
                    victimsExecutorsCount.put(victimDetails.getId(), victimDetails.getExecutors().size());

                    Runtime.getRuntime().exec(targetCommand);
                    Runtime.getRuntime().exec(victimCommand);

                    sloObserver.updateLastRebalancedTime(target.getId(), System.currentTimeMillis() / 1000);
                    sloObserver.updateLastRebalancedTime(victim.getId(), System.currentTimeMillis() / 1000);

                    sloObserver.clearTopologySLOs(target.getId());
                    sloObserver.clearTopologySLOs(victim.getId());
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

    /* private void removeAlreadySelectedPairs(ArrayList<String> receivers, ArrayList<String> givers) {
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
 */
    private void

    reassignNewScheduling(TopologyDetails target, Cluster cluster,
                                       int executorsExchanged, String status, ExecutorSummary targetExecutorSummary) {
        LOG.info("In reassignNewScheduling status {}", status);
        LOG.info("{} topology {}", status, target.getId());
        Map<ExecutorDetails, WorkerSlot> assignmentFromCluster = new HashMap<ExecutorDetails, WorkerSlot> ();
        Map<ExecutorDetails, WorkerSlot> tmp = cluster.getAssignmentById(target.getId()).getExecutorToSlot();
        for (Map.Entry <ExecutorDetails, WorkerSlot> e : tmp.entrySet()){
            assignmentFromCluster.put(
                    new ExecutorDetails(e.getKey().getStartTask(), e.getKey().getEndTask()),
                    new WorkerSlot(e.getValue().getNodeId(), e.getValue().getPort()));
        }

        if (assignmentFromCluster == null) {
            LOG.info("Assignment from cluster for {} topology is null", status);
        } else {
            Map<WorkerSlot, ArrayList<ExecutorDetails>> targetSchedule = globalState.getTopologySchedules().get(target.getId()).getAssignment();

            for (WorkerSlot ws : targetSchedule.keySet()) {
                LOG.info("{} schedule worker slot {} ", status, ws.getNodeId(), ws.getPort());
            }
            String component = targetExecutorSummary.get_component_id();
            String supervisorId = getSupervisorIdFromNodeName(globalState.getSupervisorToNode(), targetExecutorSummary.get_host());
            if (supervisorId == null) {
                LOG.info("Supervisor ID is null for creating the worker slot for the {}", status);
            }
            LOG.info("From the {} topology", status);
            int port = targetExecutorSummary.get_port();
            for (WorkerSlot workerSlots : targetSchedule.keySet()) {
                LOG.info(status + " slot port: " + workerSlots.getPort() + " slot host: " + workerSlots.getNodeId() + "\n");
                if (status.equals("victim") && workerSlots.getNodeId().equals(supervisorId))
                    port = workerSlots.getPort();

            }
            WorkerSlot targetSlot = new WorkerSlot(supervisorId, port);
            LOG.info(status + " the slot we're trying to create slot port: " + targetSlot.getPort() + "slot host: " + targetSlot.getNodeId() + " \n");

            ArrayList<ExecutorDetails> targetSlotExecutors = targetSchedule.get(targetSlot); // these are the executors on the target slot

            if (targetSlotExecutors != null) {
                Map<ExecutorDetails, String> targetExecutorsToComponents = target.getExecutorToComponent();
                ArrayList<ExecutorDetails> targetComponentExecutors = new ArrayList<>();

                // getting all the executors that belong to that component
                for (Map.Entry<ExecutorDetails, String> targetExecutorToComponent : targetExecutorsToComponents.entrySet()) {
                    LOG.info(status + " component: " + component + "\n");
                    LOG.info(status + " component we are iterating on : " + targetExecutorToComponent.getValue() + "\n");
                    if (targetExecutorToComponent.getValue().equals(component)) {
                        targetComponentExecutors.add(targetExecutorToComponent.getKey()); // this gives us all of the executors that belong to that component
                        LOG.info(status + " executor details: " + targetExecutorToComponent.getKey().toString() + "\n");
                    }
                }
                // removing all executors that belong to this component from all worker slots
                for (Map.Entry<ExecutorDetails, String> targetExecToComp : targetExecutorsToComponents.entrySet()) {
                    LOG.info("status {} targetExectoComp key {} value {}", status, targetExecToComp.getKey(), targetExecToComp.getValue());
                    if (assignmentFromCluster.containsKey(targetExecToComp.getKey()) && targetExecToComp.getValue().equals(component)) {
                        LOG.info("present in assignmentCluster. value {}", assignmentFromCluster.get(targetExecToComp.getKey()));
                        assignmentFromCluster.remove(targetExecToComp.getKey());
                        LOG.info("removed now so should be null in assignmentCluster. value {}", assignmentFromCluster.get(targetExecToComp.getKey()));
                    }

                }
                for(Map.Entry<ExecutorDetails, WorkerSlot> entry: assignmentFromCluster.entrySet()){
                    LOG.info("assignmentFromCluster before printing 1: executor: {}, workerslot: {}", entry.getKey().toString(), entry.getValue().toString());
                }
                // do a deep copy
                ArrayList<ExecutorDetails> targetNonComponentExecutorsOnSlot = new ArrayList<>();
                for (ExecutorDetails ed : targetSlotExecutors) {
                    targetNonComponentExecutorsOnSlot.add(ed);
                }
                targetNonComponentExecutorsOnSlot.removeAll(targetComponentExecutors);
                // these are all the executors that are on the slot but do not belong to the component in question

                ///  we have to find the common subset between them

                int numExecutorsOnSlot = getOldNumExecutorsOnSlot(targetComponentExecutors, targetSlotExecutors) + executorsExchanged;
                //targetComponentExecutors.retainAll(targetSlotExecutors); // now we have the common elements only.
                LOG.info("status {} retrieving common target executors only", status);

                // the number of old all executors across all slots
                int oldNumExecutors = targetComponentExecutors.size();
                int newNumExecutors = oldNumExecutors + executorsExchanged; // for the target, you have to add an executor.
                LOG.info("status {} component old num of executors {}", status, oldNumExecutors);
                LOG.info("status {} component new num of executors {}", status, newNumExecutors);
                if (newNumExecutors > 0) {
                    LOG.info("status {} new executors > 0", status, newNumExecutors);

                    ArrayList<Integer> targetTasks = new ArrayList<>();
                    for (ExecutorDetails targetExecutor : targetComponentExecutors) { // wrong assumption -- that the ranges will be consecutive
                        LOG.info("status {} starting task {} ending task {}", status, targetExecutor.getStartTask(), targetExecutor.getEndTask());
                        targetTasks.add(targetExecutor.getStartTask());
                        targetTasks.add(targetExecutor.getEndTask());
                    }
                    Collections.sort(targetTasks); // this should have all the tasks
                    int start = targetTasks.get(0);
                    int end = targetTasks.get(targetTasks.size() - 1); // end is also the total number of tasks

                    int range = (int) Math.floor((double) (end - start + 1) / (newNumExecutors));
                    int remainder = (end - start + 1) % newNumExecutors;
                    LOG.info("start {} end {} remainder {] range {} status {}", start, end, remainder, range, status);
                    if (range == 0) range = 1;

                    int startingTask = start;
                    int endingTask = 0;
                    ArrayList<ExecutorDetails> newTargetComponentExecutors = new ArrayList<>();
                    for (int i = 0; i < newNumExecutors; i++) {
                        if (i < remainder) {
                            endingTask = startingTask + range;
                            LOG.info("status {} i {} less than remainder {}, starting task  {}, ending task {}", status, i, remainder, startingTask, endingTask);
                        } else {
                            endingTask = startingTask + range - 1;
                            LOG.info("status {} i {} greater than or equal to remainder {}, starting task  {}, ending task {}", status, i, remainder, startingTask, endingTask);
                        }
                        LOG.info("status {} start {} end {} i {}", status, startingTask, endingTask, i);
                        newTargetComponentExecutors.add(new ExecutorDetails(startingTask, endingTask));
                        startingTask = endingTask + 1;
                    } // now we have set up the tasks for the executor

                    ArrayList<ExecutorDetails> newTargetSlotExecutors = new ArrayList<>();
                    for (ExecutorDetails ed : targetNonComponentExecutorsOnSlot) {
                        newTargetSlotExecutors.add(ed); // add the other executors that were on this slot but do not belong to this component
                    }
                    // add as many executors as needed to targetslot

                    int c = 0;
                    for (Iterator<ExecutorDetails> i = newTargetComponentExecutors.iterator(); i.hasNext() && c < numExecutorsOnSlot; c++) {
                        ExecutorDetails ed = i.next();
                        LOG.info("status {} executor added to target slot {} ", status, ed.toString());
                        newTargetSlotExecutors.add(ed);
                        i.remove();
                    }

                    int numExecsPerRemainingSlots = newTargetComponentExecutors.size() / (targetSchedule.size() - 1); // check this line
                    int remainingExecutors = newTargetComponentExecutors.size() % (targetSchedule.size() - 1);
                    LOG.info("status {} numExecsPerRemainingSlots {} remaining Executors {}", status, numExecsPerRemainingSlots, remainingExecutors);
                    cluster.freeSlots(targetSchedule.keySet());
                    //cluster.getAssignmentById(target.getId()).getExecutorToSlot().clear();

                    cluster.assign(targetSlot, target.getId(), newTargetSlotExecutors);


                    for(Map.Entry<ExecutorDetails, WorkerSlot> entry: assignmentFromCluster.entrySet()){
                        LOG.info("assignmentFromCluster before printing 2: executor: {}, workerslot: {}", entry.getKey().toString(), entry.getValue().toString());
                    }
                    Map<WorkerSlot, ArrayList<ExecutorDetails>> flippedAssignment = flipMap(assignmentFromCluster);

                    int index = 0;
                    for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> topologyEntry : flippedAssignment.entrySet()/*targetSchedule.entrySet()*/) {
                        WorkerSlot slot = topologyEntry.getKey();
                        LOG.info("Worker slot {} status {}", slot.toString(), status);
                        int max = numExecsPerRemainingSlots;
                        LOG.info("max {} status {}", max, status);
                        if (!slot.equals(targetSlot)) {
                            LOG.info("status {} slot is not equal to current slot", status);
                            if (cluster.getUsedSlots().contains(slot)) {
                                LOG.info("slot was already assigned. status {}", status);
                                cluster.freeSlot(slot);
                            }
                            LOG.info(" index {} status {}", index, status);
                            if (index < remainingExecutors) {
                                max = max + 1;
                                LOG.info("max is updated {} status {}", max, status);
                            }
                            c = 0;
                            for (Iterator<ExecutorDetails> i = newTargetComponentExecutors.iterator(); i.hasNext() && c < max; c++) {
                                ExecutorDetails ed = i.next();
                                topologyEntry.getValue().add(ed);
                                LOG.info("slot {} executor {} status", slot.toString(), ed.toString(), status);
                                i.remove();
                           }
                           try {
                               LOG.info("status {} what we're trying to assign ", status);
                               for (ExecutorDetails ed: topologyEntry.getValue()) {
                                   LOG.info("status {} slot {} topology {} executor {}", status, slot.toString(), target.getId(), ed.toString());
                               }
                               cluster.assign(slot, target.getId(), topologyEntry.getValue());


                           } catch (Exception e) {
                               LOG.info("Catching already assigned exception {}", e.toString());
                               Map<ExecutorDetails, WorkerSlot>  assignment = cluster.getAssignmentById(target.getId()).getExecutorToSlot();
                               for (ExecutorDetails d : topologyEntry.getValue()) {
                                if (assignment.containsKey(d)) {
                                    LOG.info("Executor {} already present on {}", d.toString(), assignment.get(d).toString());
                                }
                               }
                           }

                        }
                        index++;
                    }
                } else {
                    LOG.info("Num of new executors is now zero for {}", status);
                }
            } else {
                LOG.info("the old {} schedule does not have this worker slot.", status);
            }
        }
    }


    private void printSchedule(SchedulerAssignment currentTargetAssignment, String tag) {
        if (currentTargetAssignment == null || currentTargetAssignment.getExecutorToSlot() == null) {
            LOG.info("Assignment is null");
            return;
        }
        Map<ExecutorDetails, WorkerSlot> map = currentTargetAssignment.getExecutorToSlot();
        LOG.info("Logging for " + tag + "\n");
        for (Entry<ExecutorDetails, WorkerSlot> e : map.entrySet()) {
            LOG.info("Host : " + globalState.getSupervisorToNode().get(e.getValue().getNodeId()).hostname + "Port: " + e.getValue().getPort()+ " Executor {}" + e.getKey().toString() + "\n");
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

    public int getOldNumExecutorsOnSlot(ArrayList<ExecutorDetails> targetComponentExecutors, ArrayList<ExecutorDetails> targetSlotExecutors) {
        ArrayList<ExecutorDetails> temp = new ArrayList<>();
        for (ExecutorDetails a : targetComponentExecutors) {
            temp.add(a);
        }
        temp.retainAll(targetSlotExecutors);
        LOG.info("Number of executors previously on the node {}", temp.size());
        return temp.size();
    }

    public String getSupervisorIdFromNodeName(HashMap<String, Node> supervisorsToNodes, String nodeName) {
        for (Map.Entry<String, Node> supervisorToNode : supervisorsToNodes.entrySet()) {
            Node node = supervisorToNode.getValue();
            LOG.info("supervisor name {} name we are looking for {} nodename we input {}", supervisorToNode.getKey(), node.hostname, nodeName);
            if (node.hostname.equals(nodeName)) {
                return supervisorToNode.getKey();
            }
        }
        return null;
    }

    public Map<WorkerSlot, ArrayList<ExecutorDetails>> flipMap(Map<ExecutorDetails, WorkerSlot> map){
        Map<WorkerSlot, ArrayList<ExecutorDetails>> flippedMap = new HashMap<>();
        for (Map.Entry<ExecutorDetails, WorkerSlot> entry : map.entrySet()) {
            if (!flippedMap.containsKey(entry.getValue())){
                flippedMap.put(entry.getValue(), new ArrayList<ExecutorDetails>());
            }
            flippedMap.get(entry.getValue()).add(entry.getKey());
            LOG.info("Flipped map contains {} {}", entry.getValue(), entry.getKey());
        }
        return flippedMap;
    }
}