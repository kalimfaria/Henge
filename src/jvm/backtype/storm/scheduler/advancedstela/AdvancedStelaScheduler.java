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
        SchedulerAssignment currentTargetAssignment = cluster.getAssignmentById(target.getId());

        writeToFile(same_top, "The slot that has been chosen for the target is: " + targetSlot.toString() + "\n");
        printSchedule(currentTargetAssignment, "current target assignment");

        writeToFile(same_top, "Does the old schedule have this worker slot: " + targetSlot + "\n");
        writeToFile(same_top, "Checking: " + targetSchedule.containsKey(targetSlot) + "\n");
        ArrayList <ExecutorDetails> oldExecutorDetails = new ArrayList<>();
        HashMap <WorkerSlot, ArrayList<ExecutorDetails>> newWorkerSlotToExecutorDetails = new HashMap<>();

        if (targetSchedule.containsKey(targetSlot)) {
            writeToFile(same_top, " Yes, it does \n");
            oldExecutorDetails = targetSchedule.get(targetSlot);
            for (ExecutorDetails oldExecutorDetail : oldExecutorDetails) {
                writeToFile(same_top, "Old executor: "  + oldExecutorDetail.toString() + "\n");
            }
        } else {
            writeToFile(same_top, "Topology is not even deployed on that particular target slot \n");
        }
        Map<ExecutorDetails,WorkerSlot> currentExecutorsToSlots =  currentTargetAssignment.getExecutorToSlot();
        // just flipping the map :)
        // TODO extract into another function
        for (Map.Entry<ExecutorDetails, WorkerSlot> currentExecutorToSlot : currentExecutorsToSlots.entrySet() ) {
            ArrayList<ExecutorDetails> details = new ArrayList<>();
            if (newWorkerSlotToExecutorDetails.containsKey(currentExecutorToSlot.getValue()))
            {
                details = newWorkerSlotToExecutorDetails.get(currentExecutorToSlot.getValue());
            }
            details.add(currentExecutorToSlot.getKey());
            newWorkerSlotToExecutorDetails.put(currentExecutorToSlot.getValue(), details);
        }

        for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> currentExecutorToSlot : newWorkerSlotToExecutorDetails.entrySet()) {
            writeToFile(same_top, "new executor: worker slot "  + currentExecutorToSlot.getKey() + "\n");
            ArrayList<ExecutorDetails> executorDetails = currentExecutorToSlot.getValue();
            for (ExecutorDetails d: executorDetails){
                writeToFile(same_top, "new executor: executor details "  + d.toString() + "\n");
            }
        }

        ArrayList<ExecutorDetails> currentExecutorsForTargetSlot = newWorkerSlotToExecutorDetails.get(targetSlot);
        if (currentExecutorsForTargetSlot.size() > oldExecutorDetails.size()) {
            writeToFile(same_top, "num of old executors on slot  "  + oldExecutorDetails.size() + "\n");
            writeToFile(same_top, "num of new executors on slot  "  + currentExecutorsForTargetSlot.size() + "\n");
            writeToFile(same_top, "don't need to do nothing bro :D \n");
        } else {
            for (Map.Entry <WorkerSlot, ArrayList<ExecutorDetails>> newSlotToDetails : newWorkerSlotToExecutorDetails.entrySet()) {
                writeToFile(same_top, "newSlotToDetails.getValue().size() "  + newSlotToDetails.getValue().size() + "\n");
                writeToFile(same_top, "oldExecutorDetails.size() - numExecutorsExchanged "  + (oldExecutorDetails.size() - numExecutorsExchanged) + "\n");
                writeToFile(same_top, "newSlotToDetails.getKey() "  + newSlotToDetails.getKey().toString() + "\n");
                writeToFile(same_top, "targetSlot "  + targetSlot.toString() + "\n");
                if ((newSlotToDetails.getValue().size() == oldExecutorDetails.size() + numExecutorsExchanged) && !newSlotToDetails.getKey().equals(targetSlot)) {
                    // give this slot one executor from the target slot's executor
                    ArrayList<ExecutorDetails> currentTargetSlotExecutors = newWorkerSlotToExecutorDetails.get(targetSlot);
                    ExecutorDetails executorDetails =   newSlotToDetails.getValue().get(0);
                    // add it to target slot
                    currentTargetSlotExecutors.add(executorDetails);
                    // delete from old slot
                    newSlotToDetails.getValue().remove(0);
                    // put back :D
                    newWorkerSlotToExecutorDetails.put(targetSlot, currentTargetSlotExecutors);
                    break;
                }
            }
        }

        writeToFile(same_top, "Changed the schedule \n");
        for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> currentExecutorToSlot : newWorkerSlotToExecutorDetails.entrySet()) {
            writeToFile(same_top, "new executor: worker slot "  + currentExecutorToSlot.getKey() + "\n");
            ArrayList<ExecutorDetails> executorDetails = currentExecutorToSlot.getValue();
            for (ExecutorDetails d: executorDetails){
                writeToFile(same_top, "new executor: executor details "  + d.toString() + "\n");
            }
        }

        for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> topologyEntry : targetSchedule.entrySet()) {
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

        WorkerSlot victimSlot = new WorkerSlot(victimExecutorSummary.get_host(), victimExecutorSummary.get_port());
        SchedulerAssignment currentVictimAssignment = cluster.getAssignmentById(victim.getId());

        writeToFile(same_top, "The slot that has been chosen is: " + victimSlot.toString() + "\n");
        printSchedule(currentVictimAssignment, "current victim assignment");

        writeToFile(same_top, "Does the old schedule have this worker slot: " + victimSlot + "\n");
        writeToFile(same_top, "Checking: " + victimSchedule.containsKey(victimSlot) + "\n");
        ArrayList <ExecutorDetails> oldExecutorDetails = new ArrayList<>();
        HashMap <WorkerSlot, ArrayList<ExecutorDetails>> newWorkerSlotToExecutorDetails = new HashMap<>();

        if (victimSchedule.containsKey(victimSlot)) {
            writeToFile(same_top, " Yes, it does \n");
            oldExecutorDetails = victimSchedule.get(victimSlot);
            for (ExecutorDetails oldExecutorDetail : oldExecutorDetails) {
                writeToFile(same_top, "Old executor: "  + oldExecutorDetail.toString() + "\n");
            }
        } else {
            writeToFile(same_top, "Topology is not even deployed on that particular slot \n");
        }
        Map<ExecutorDetails,WorkerSlot> currentExecutorsToSlots =  currentVictimAssignment.getExecutorToSlot();
        // just flipping the map :)
        // TODO extract into another function
        for (Map.Entry<ExecutorDetails, WorkerSlot> currentExecutorToSlot : currentExecutorsToSlots.entrySet() ) {
            ArrayList<ExecutorDetails> details = new ArrayList<>();
                if (newWorkerSlotToExecutorDetails.containsKey(currentExecutorToSlot.getValue()))
                {
                    details = newWorkerSlotToExecutorDetails.get(currentExecutorToSlot.getValue());
                }
            details.add(currentExecutorToSlot.getKey());
            newWorkerSlotToExecutorDetails.put(currentExecutorToSlot.getValue(), details);
        }

        for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> currentExecutorToSlot : newWorkerSlotToExecutorDetails.entrySet()) {
            writeToFile(same_top, "new executor: worker slot "  + currentExecutorToSlot.getKey() + "\n");
            ArrayList<ExecutorDetails> executorDetails = currentExecutorToSlot.getValue();
            for (ExecutorDetails d: executorDetails){
                writeToFile(same_top, "new executor: executor details "  + d.toString() + "\n");
            }
        }

        ArrayList<ExecutorDetails> currentExecutorsForVictimSlot = newWorkerSlotToExecutorDetails.get(victimSlot);
        if (currentExecutorsForVictimSlot.size() < oldExecutorDetails.size()) {
            writeToFile(same_top, "num of old executors on slot  "  + oldExecutorDetails.size() + "\n");
            writeToFile(same_top, "num of new executors on slot  "  + currentExecutorsForVictimSlot.size() + "\n");
            writeToFile(same_top, "don't need to do nothing bro :D \n");
        } else {
            for (Map.Entry <WorkerSlot, ArrayList<ExecutorDetails>> newSlotToDetails : newWorkerSlotToExecutorDetails.entrySet()) {
                writeToFile(same_top, "newSlotToDetails.getValue().size() "  + newSlotToDetails.getValue().size() + "\n");
                writeToFile(same_top, "oldExecutorDetails.size() - numExecutorsExchanged "  + (oldExecutorDetails.size() - numExecutorsExchanged) + "\n");
                writeToFile(same_top, "newSlotToDetails.getKey() "  + newSlotToDetails.getKey().toString() + "\n");
                writeToFile(same_top, "victimSlot "  + victimSlot.toString() + "\n");
                if ((newSlotToDetails.getValue().size() == oldExecutorDetails.size() - numExecutorsExchanged) && !newSlotToDetails.getKey().equals(victimSlot)) {
                    // give this slot one executor from the victim slot's executor
                    ArrayList<ExecutorDetails> currentVictimSlotExecutors = newWorkerSlotToExecutorDetails.get(victimSlot);
                    ExecutorDetails executorDetails = currentVictimSlotExecutors.get(0);
                    // add it to new slot
                    newSlotToDetails.getValue().add(executorDetails);
                    // delete from victim slot
                    currentVictimSlotExecutors.remove(0);
                    // put back :D
                    newWorkerSlotToExecutorDetails.put(victimSlot, currentVictimSlotExecutors);
                    break;
                }
            }
        }

        writeToFile(same_top, "Changed the schedule \n");
        for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> currentExecutorToSlot : newWorkerSlotToExecutorDetails.entrySet()) {
            writeToFile(same_top, "new executor: worker slot "  + currentExecutorToSlot.getKey() + "\n");
            ArrayList<ExecutorDetails> executorDetails = currentExecutorToSlot.getValue();
            for (ExecutorDetails d: executorDetails){
                writeToFile(same_top, "new executor: executor details "  + d.toString() + "\n");
            }
        }

        // if the number of executors is the same, then we have a problem. If it is less, that's great, move on
        // if it is the same -- find the one that has the lesser size and then give one to that -- WOO :D

        for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> topologyEntry : victimSchedule.entrySet()) {
            if (cluster.getUsedSlots().contains(topologyEntry.getKey())) {
                cluster.freeSlot(topologyEntry.getKey());
            }
            cluster.assign(topologyEntry.getKey(), victim.getId(), topologyEntry.getValue());
        }
        victims.remove((victim.getId()));
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