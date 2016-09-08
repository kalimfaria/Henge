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

        for (String target : targets.keySet()) {
            LOG.info("target {} ", target);
        }
        for (String victim  : victims.keySet()) {
            LOG.info ("victim {} ", victim);
        }

        if (numTopologiesThatNeedScheduling > 0) {

            if (victims.isEmpty() && targets.isEmpty() && numTopologiesThatNeedScheduling > 0)
            {
                LOG.info ("STORM IS GOING TO PERFORM THE REBALANCING");
                new backtype.storm.scheduler.EvenScheduler().schedule(topologies, cluster);
            } else {
                StringBuffer sb = new StringBuffer();
                LOG.info("cluster.needsSchedulingTopologies(topologies).size() > 0\n");
                LOG.info("Before calling EvenScheduler: \n");
                LOG.info("Size of cluster.needsSchedulingTopologies(topologies): " + cluster.needsSchedulingTopologies(topologies).size() + "\n");
                List<TopologyDetails> topologiesScheduled = cluster.needsSchedulingTopologies(topologies);
                LOG.info("targets.length() : " + targets.size() + "\n");
                LOG.info("victims.length(): " + victims.size() + "\n");

                for (TopologyDetails topologyThatNeedsToBeScheduled : topologiesScheduled) {
                    Collection<ExecutorDetails> unscheduledExecutors =  cluster.getUnassignedExecutors(topologyThatNeedsToBeScheduled);
                    LOG.info( " Topology that needs to be scheduled " + topologyThatNeedsToBeScheduled.getName() + "\n");
                    LOG.info(  "Is there an assignment for this topology: " + (cluster.getAssignmentById(topologyThatNeedsToBeScheduled.getId()) != null)  + "\n");
                    LOG.info( " Number of unassigned executors " + unscheduledExecutors.size() + "\n");
                    for (ExecutorDetails unscheduledExecutor: unscheduledExecutors ) {
                        LOG.info( " Unassigned executor "+ unscheduledExecutor.toString() + "\n");
                    }
                }

                for (TopologyDetails topologyThatNeedsToBeScheduled : topologiesScheduled) {
                    sb.append("Id of topology: " + topologyThatNeedsToBeScheduled.getId() + "\n");
                }

                if (!targets.isEmpty()) {
                    LOG.info("!targets.isEmpty()\n");
                    decideAssignmentForTargets(topologies, cluster);
                   // targets.clear();
                }
                if (!victims.isEmpty()) {
                    LOG.info("!victims.isEmpty()\n");
                    decideAssignmentForVictims(topologies, cluster);
                  //  victims.clear();
                }
                LOG.info(sb.toString());
            /*if (victims.isEmpty() && targets.isEmpty() && numTopologiesThatNeedScheduling > 0)
                new backtype.storm.scheduler.EvenScheduler().schedule(topologies, cluster);*/

            }

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

    private void decideAssignmentForTargets(Topologies topologies, Cluster cluster) {
        List<TopologyDetails> unscheduledTopologies = cluster.needsSchedulingTopologies(topologies);
        for (String target : targets.keySet()) {
            LOG.info("target {} ", target);
        }
        for (TopologyDetails topologyDetails : unscheduledTopologies) {
            LOG.info("in decideAssignmentForTargets");


            LOG.info(" the name of the target we are looking at {} ", topologyDetails.getId());


            if (targets.containsKey(topologyDetails.getId()) /* && cluster.getAssignmentById(topologyDetails.getId()) != null*/) // BY REMOVING THIS, WE CAN FIX THE BUG
            {
                LOG.info("Found topology in targets " + topologyDetails.getId() + "\n");
                findAssignmentForTarget(topologyDetails, cluster, topologyDetails.getId());
            }
        }
    }

    private void decideAssignmentForVictims(Topologies topologies, Cluster cluster) {
        List<TopologyDetails> unscheduledTopologies = cluster.needsSchedulingTopologies(topologies);
        LOG.info("in decideAssignmentForVictims");
        for (String victim : victims.keySet()) {
            LOG.info("victim {} ", victim);
        }
        for (TopologyDetails topologyDetails : unscheduledTopologies) {
            LOG.info(" the name of the victim we are looking at {} ", topologyDetails.getId());
            if (victims.containsKey(topologyDetails.getId()) /* && cluster.getAssignmentById(topologyDetails.getId()) != null()*/) // BY REMOVING THIS SECOND PART, WE CAN FIX THE BUG
            {
                LOG.info("Found topology in victims " + topologyDetails.getId() + "\n");
                findAssignmentForVictim(topologyDetails, cluster, topologyDetails.getId());
            }
        }
    }

    private void findAssignmentForTarget(TopologyDetails target, Cluster cluster, String topologyId) {
        ExecutorPair executorPair = targets.get(topologyId);
        LOG.info("findAssignment for Target " + executorPair.getTargetExecutorSummary().get_host() + " " + executorPair.getTargetExecutorSummary().get_port() + "\n");
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

                    LOG.info( targetCommand + "\n");
                    LOG.info( System.currentTimeMillis() + "\n");
                    LOG.info( victimCommand + "\n");

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

        LOG.info("In reassignTargetNewScheduling \n");
        LOG.info("Target topology " +target.getId()+ " \n");
        Map<WorkerSlot, ArrayList<ExecutorDetails>> targetSchedule = globalState.getTopologySchedules().get(target.getId()).getAssignment();
        ExecutorSummary targetExecutorSummary = executorPair.getTargetExecutorSummary();
        String component = targetExecutorSummary.get_component_id();

        String supervisorId = getSupervisorIdFromNodeName(globalState.getSupervisorToNode(), targetExecutorSummary.get_host());
        if (supervisorId == null ) {
            LOG.info( "Supervisor ID is null for creating the worker slot for the target");
        }

        WorkerSlot targetSlot = new WorkerSlot(supervisorId, targetExecutorSummary.get_port());

        LOG.info( "From the topology\n");
        for (WorkerSlot workerSlots : targetSchedule.keySet()) {
            LOG.info( "slot port: " + workerSlots.getPort() + "slot host: " + workerSlots.getNodeId()+ "\n");
        }
        LOG.info( "the slot we're trying to create slot port: " + targetSlot.getPort() + "slot host: " + targetSlot.getNodeId() +" \n");

        ArrayList <ExecutorDetails> targetSlotExecutors = targetSchedule.get(targetSlot); // these are the executors on the victim slot

        if (targetSlotExecutors != null) {
            Map<ExecutorDetails, String> targetExectorsToComponents = target.getExecutorToComponent();
            ArrayList <ExecutorDetails> targetComponentExecutors = new ArrayList<>();

            for (Map.Entry<ExecutorDetails, String> targetExecutorToComponent: targetExectorsToComponents.entrySet()) {
                LOG.info( "component: " + component + "\n");
                LOG.info( "component we are iterating on : " + targetExecutorToComponent.getValue() + "\n");
                if (targetExecutorToComponent.getValue().equals(component)) {
                    targetComponentExecutors.add(targetExecutorToComponent.getKey()); // this gives us all of the executors that belong to that component
                    LOG.info( "executor details: " + targetExecutorToComponent.getKey().toString() + "\n");
                }
            }
            // do a deep copy
            ArrayList <ExecutorDetails> targetNonComponentExecutorsOnSlot = new ArrayList<>();
            for (ExecutorDetails ed : targetSlotExecutors) {
                targetNonComponentExecutorsOnSlot.add(ed);
            }

            targetNonComponentExecutorsOnSlot.removeAll(targetComponentExecutors);
            // these are all the executors that are on the slot but do not belong to the component in question

            ///  we have to find the common subset between them

            targetComponentExecutors.retainAll(targetSlotExecutors); // now we have the common elements only.
            int oldNumExecutors = targetComponentExecutors.size();
            int newNumExecutors = oldNumExecutors - numExecutorsExchanged;
            if (newNumExecutors > 0) {
                ArrayList <Integer> targetTasks = new ArrayList<>();
                for (ExecutorDetails victimExecutor: targetComponentExecutors) {
                    targetTasks.add(victimExecutor.getStartTask());
                    targetTasks.add(victimExecutor.getEndTask());
                }
                Collections.sort(targetTasks);
                int start = targetTasks.get(0); // assumption -- values are contiguous
                int end = targetTasks.get(targetTasks.size()-1);
                int range = (int) Math.floor((double)(end-start)/(newNumExecutors));
                if (range == 0) range = 1;

                ArrayList <ExecutorDetails> newTargetComponentExecutorsOnSlot = new ArrayList<>();
                for (int i = 0; i < newNumExecutors; i++){
                    int startingTask = start + i * range;
                    int endingTask = start  + (i + 1) * range; // assumption that range needs to be continuous
                    if (endingTask > end)
                        endingTask = end;
                    newTargetComponentExecutorsOnSlot.add(new ExecutorDetails(startingTask, endingTask));
                } // now we have set up the tasks for the executor

                for (ExecutorDetails ed : targetNonComponentExecutorsOnSlot) {
                    newTargetComponentExecutorsOnSlot.add(ed); // add the other executors that were on this slot but do not belong to this component
                }

                //for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> topologyEntry : newExecutorToSlot.entrySet()) {
                if (cluster.getUsedSlots().contains(targetSlot)) {
                    cluster.freeSlot(targetSlot);
                }
                cluster.assign(targetSlot, target.getId(), newTargetComponentExecutorsOnSlot);
                // }
                targets.remove((target.getId()));

            } else {
                LOG.info( "Num of new executors is now zero for target :S");
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
            LOG.info( "the old targets schedule does not have this worker slot.");

        }
    }


    private void printSchedule(SchedulerAssignment currentTargetAssignment, String tag ) {
        Map<ExecutorDetails, WorkerSlot> map = currentTargetAssignment.getExecutorToSlot();
        LOG.info( "Logging for " + tag + "\n");
        for (Entry<ExecutorDetails, WorkerSlot> e : map.entrySet()) {
            LOG.info( "WorkerSlot : " + e.getValue() + " Executor {}" + e.getKey().toString() + "\n");
        }
    }

    private void reassignVictimNewScheduling(TopologyDetails victim, Cluster cluster,
                                             ExecutorPair executorPair) {
        LOG.info("In reassignVictimNewScheduling \n");
        LOG.info("Victim topology " + victim.getId()+ " \n");

        Map<WorkerSlot, ArrayList<ExecutorDetails>> victimSchedule = globalState.getTopologySchedules().get(victim.getId()).getAssignment();
        LOG.info( "From the topology\n");
        for (WorkerSlot workerSlots : victimSchedule.keySet()) {
            LOG.info("slot port: " + workerSlots.getPort() + "slot host: " + workerSlots.getNodeId() + "\n");
        }

        ExecutorSummary victimExecutorSummary = executorPair.getVictimExecutorSummary();
        String supervisorId = getSupervisorIdFromNodeName(globalState.getSupervisorToNode(), victimExecutorSummary.get_host());
        if (supervisorId == null ) {
            writeToFile(same_top, "Supervisor ID is null for creating the worker slot");
            LOG.info("Supervisor ID is null for creating the worker slot");
        }
        String component = victimExecutorSummary.get_component_id();
        WorkerSlot victimSlot = new WorkerSlot(supervisorId, victimExecutorSummary.get_port());
        writeToFile(same_top, "the slot we're trying to create slot port: " + victimSlot.getPort() + "slot host: " + victimSlot.getNodeId() +" \n");
        LOG.info("the slot we're trying to create slot port: " + victimSlot.getPort() + "slot host: " + victimSlot.getNodeId() +" \n");

        ArrayList <ExecutorDetails> victimSlotExecutors = victimSchedule.get(victimSlot); // these are the executors on the victim slot
        if (victimSlotExecutors != null) {


            Map<ExecutorDetails, String> victimExectorsToComponents = victim.getExecutorToComponent();
            ArrayList <ExecutorDetails> victimComponentExecutors = new ArrayList<>();

            for (Map.Entry<ExecutorDetails, String> victimExecutorToComponent: victimExectorsToComponents.entrySet()) {
                writeToFile(same_top, "component: " + component + "\n");
                writeToFile(same_top, "component we are iterating on : " + victimExecutorToComponent.getValue() + "\n");
                LOG.info("component: " + component + "\n");
                LOG.info("component we are iterating on : " + victimExecutorToComponent.getValue() + "\n");
                if (victimExecutorToComponent.getValue().equals(component)) {
                    victimComponentExecutors.add(victimExecutorToComponent.getKey()); // this gives us all of the executors that belong to that component
                    writeToFile(same_top, "executor details: " + victimExecutorToComponent.getKey().toString() + "\n");
                    LOG.info("executor details: " + victimExecutorToComponent.getKey().toString() + "\n");
                }
            }
            // do a deep copy
            ArrayList <ExecutorDetails> victimNonComponentExecutorsOnSlot = new ArrayList<>();
            for (ExecutorDetails ed : victimSlotExecutors) {
                victimNonComponentExecutorsOnSlot.add(ed);
            }

            victimNonComponentExecutorsOnSlot.removeAll(victimComponentExecutors);
            // these are all the executors that are on the slot but do not belong to the component in question
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
                LOG.info("Num of new executors is now zero :S");
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
            LOG.info("the old victims schedule does not have this worker slot.");
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

    public String getSupervisorIdFromNodeName (HashMap<String, Node> supervisorsToNodes, String nodeName) {
        for (Map.Entry<String, Node> supervisorToNode : supervisorsToNodes.entrySet()) {
            Node node = supervisorToNode.getValue();
            LOG.info("supervisor name {} name we are looking for {} nodename we input {}", supervisorToNode.getKey() , node.hostname, nodeName);
            if (node.hostname.equals(nodeName)) {
                return supervisorToNode.getKey();
            }
        }
        return null;
    }
}