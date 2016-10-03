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
    private HashMap<String, Integer> targetsExecutorsCountOnSlot, victimsExecutorsCountOnSlot;
    private HashMap<String, Integer> targetsExecutorsTotal, victimsExecutorsTotal;
    private File juice_log;
    private File same_top;

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
        targetsExecutorsCountOnSlot = new HashMap<String, Integer>();
        victimsExecutorsCountOnSlot = new HashMap<String, Integer>();
        targetsExecutorsTotal = new HashMap<>();
        victimsExecutorsTotal = new HashMap<>();
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
        } else if ((!victims.isEmpty() || !targets.isEmpty()) && numTopologies > 0 && numTopologiesThatNeedScheduling == 0) {
            LOG.info("((!victims.isEmpty() || !targets.isEmpty()) && numTopologies > 0  && numTopologiesThatNeedScheduling == 0)");
            LOG.info("Phase 3");
            // do the rebalancing the way we want it

            for (Map.Entry<String, ExecutorPair> victim : victims.entrySet()) {
                LOG.info("Looping through victims : {}", victim);
                String victimName = victim.getKey();
                if (victimsExecutorsCount.containsKey(victimName)) {
                    LOG.info("Looping through victims executor count: {}", victimsExecutorsCount.get(victimName));
                    int newExecutors = cluster.getAssignments().get(victimName).getExecutors().size();
                    if (newExecutors < victimsExecutorsCount.get(victimName)) {
                        findAssignmentForVictim(topologies.getById(victimName), cluster, victimName);
                    }
                }
            }

            for (Map.Entry<String, ExecutorPair> target : targets.entrySet()) {
                LOG.info("Looping through targets : {}", target);
                String targetName = target.getKey();
                if (targetsExecutorsCount.containsKey(targetName)) {
                    LOG.info("Looping through targets executor count: {}", targetsExecutorsCount.get(targetName));
                    int newExecutors = cluster.getAssignments().get(targetName).getExecutors().size();
                    if (newExecutors > targetsExecutorsCount.get(targetName)) {
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
            reassignNewTargetScheduling(target, cluster, "target", executorPair.getTargetExecutorSummary(), targetsExecutorsCountOnSlot.get(target.getId()), targetsExecutorsCountOnSlot.get(target.getId()) + 1, targetsExecutorsTotal.get(target.getId()));
            targets.remove((target.getId()));
            targetsExecutorsCount.remove(target.getId());
            targetsExecutorsCountOnSlot.remove(target.getId());
            targetsExecutorsTotal.remove(target.getId());
        } catch (Exception e) {
            LOG.info("Exception in findAssignment for target {}", e.toString());
        }

    }

    private void findAssignmentForVictim(TopologyDetails victim, Cluster cluster, String topologyId) {
        try {
            ExecutorPair executorPair = victims.get(topologyId);
            LOG.info("findAssignment for Victim " + executorPair.getVictimExecutorSummary().get_host() + " " + executorPair.getVictimExecutorSummary().get_port() + "\n");
            reassignNewVictimScheduling(victim, cluster, "victim", executorPair.getVictimExecutorSummary(), victimsExecutorsCountOnSlot.get(victim.getId()), victimsExecutorsCountOnSlot.get(victim.getId()) - 1, victimsExecutorsTotal.get(victim.getId()));
            victims.remove((victim.getId()));
            victimsExecutorsCount.remove((victim.getId()));
            victimsExecutorsCountOnSlot.remove(victim.getId());
            victimsExecutorsTotal.remove(victim.getId());
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
            LOG.info("{} topology is only distributed across two machines. Can't do no more bro :D.", target.getId());
        } else {
            String targetCommand = new String();
            Long workers;
            writeToFile(juice_log, "/var/nimbus/storm/bin/storm old-workers: " + numWorkers + "\n");
            if (numWorkers >= 3) workers = numWorkers - 1;
            else workers = 2L; // goes from 3 to
            targetCommand = "/var/nimbus/storm/bin/storm " +
                    "rebalance -w 0 " + targetDetails.getName() + " -n " +
                    workers;
            targetTopology.setWorkers(workers);
            writeToFile(juice_log, System.currentTimeMillis() + "\n");
            writeToFile(juice_log, targetCommand + "\n");
            try {
                Runtime.getRuntime().exec(targetCommand);
            } catch (IOException ex) {
                LOG.info("Exception {} while trying to reduce workers for latency sensitive topology.", ex.toString());
            }
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
                    int numExecutorsOnTargetSlot = findNumOfExecutorsOnSlot(targetDetails, target, executorSummaries.getTargetExecutorSummary());
                    targetsExecutorsCount.put(targetDetails.getId(), targetDetails.getExecutors().size());
                    targetsExecutorsCountOnSlot.put(targetDetails.getId(), numExecutorsOnTargetSlot);
                    targetsExecutorsTotal.put(targetDetails.getId(), targetNewParallelism);

                    LOG.info("Victim old executors count {}", victimDetails.getExecutors().size());
                    int numExecutorsOnVictimSlot = findNumOfExecutorsOnSlot(victimDetails, victim, executorSummaries.getVictimExecutorSummary());
                    victimsExecutorsCountOnSlot.put(victimDetails.getId(), numExecutorsOnVictimSlot);
                    victimsExecutorsCount.put(victimDetails.getId(), victimDetails.getExecutors().size());
                    victimsExecutorsTotal.put(victimDetails.getId(), victimNewParallelism);

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

    private void reassignNewTargetScheduling (TopologyDetails target, Cluster cluster, String status, ExecutorSummary targetExecutorSummary, int oldNumExecutorsOnSlot, int newNumExecutorsOnSlot, int totalNewExecs) {
        LOG.info("In reassignNewTargetScheduling status {}", status);
        LOG.info("{} topology {}", status, target.getId());
        Map<ExecutorDetails, WorkerSlot> assignmentFromCluster = new HashMap<ExecutorDetails, WorkerSlot>();
        Map<ExecutorDetails, WorkerSlot> tmp = cluster.getAssignmentById(target.getId()).getExecutorToSlot();
        for (Map.Entry<ExecutorDetails, WorkerSlot> e : tmp.entrySet()) {
            assignmentFromCluster.put(
                    new ExecutorDetails(e.getKey().getStartTask(), e.getKey().getEndTask()),
                    new WorkerSlot(e.getValue().getNodeId(), e.getValue().getPort()));
        } // deep copy

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
            } else {

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
                    Map<WorkerSlot, ArrayList<ExecutorDetails>> flippedAssignment = flipMap(assignmentFromCluster);
                    Map<WorkerSlot, ArrayList<ExecutorDetails>> newAssignment = flipMap(assignmentFromCluster);
                    Map <ExecutorDetails, String> executorsToComponents =  target.getExecutorToComponent();

                    LOG.info("we want flipped assignment to contain the executors of the component we want on all slots");
                    for (Map.Entry<ExecutorDetails, String> entry : executorsToComponents.entrySet())  {
                        LOG.info("status entry.key {} entry.component {}", status, entry.getKey(), entry.getValue());
                        for (Map.Entry <WorkerSlot, ArrayList<ExecutorDetails>> slotToExecutor :flippedAssignment.entrySet()) {
                            ArrayList <ExecutorDetails> details = slotToExecutor.getValue();
                            LOG.info("Looking at flipped assignment. Worker slot {} ", slotToExecutor.getKey().toString());
                            LOG.info("details.contains(entry.getKey()) {}  entry.getValue().equals(component) {}", details.contains(entry.getKey()) , entry.getValue().equals(component));
                            if (details.contains(entry.getKey()) && !entry.getValue().equals(component)) {
                                details.remove(entry.getKey());
                                LOG.info("Entry key removed from details {}", entry.getKey());
                                flippedAssignment.put(slotToExecutor.getKey(),details);
                            }
                        }
                    }
                    LOG.info("flipped assignment contains the executors of the component we want on all slots");
                    // flipped assignment contains the executors of the component we want on all slots

                    ArrayList <ExecutorDetails> compExecutorsOnSlot = flippedAssignment.get(targetSlot);
                    LOG.info("New expected number of executors on slot {}", newNumExecutorsOnSlot);
                    LOG.info("New actual number of executors on slot {}", oldNumExecutorsOnSlot);
                    if (compExecutorsOnSlot.size() == newNumExecutorsOnSlot) {
                        LOG.info("WE are good to go. Don't do anything else");
                    } else {
                        LOG.info("finding executor slot to exchange with");
                        LOG.info("target newNumExecutorOnSlot {} totalNewExecs {} num workers {}", newNumExecutorsOnSlot, totalNewExecs, flippedAssignment.size());
                        if (totalNewExecs <= flippedAssignment.size())
                            newNumExecutorsOnSlot = newNumExecutorsOnSlot - 1;
                        LOG.info("target After the if condition newNumExecutorOnSlot {} totalNewExecs {} num workers {}", newNumExecutorsOnSlot, totalNewExecs, flippedAssignment.size());
                        WorkerSlot slotToExchangeWith = new WorkerSlot("", 0);
                        for (Map.Entry <WorkerSlot, ArrayList<ExecutorDetails>> entry: flippedAssignment.entrySet()) {
                            if (!entry.getKey().equals(targetSlot)) {
                                int numExecutorsOnSlot = entry.getValue().size();
                                LOG.info("slot {} executors of component on slot {}", entry.getKey().toString(), numExecutorsOnSlot);
                                if (numExecutorsOnSlot >= newNumExecutorsOnSlot) {
                                    slotToExchangeWith = entry.getKey();
                                    LOG.info("chosen slot {}", slotToExchangeWith.toString());
                                    break;
                                }
                            }
                        }
                        LOG.info("Slot chosen {}", slotToExchangeWith.toString());
                        // taking an executor from chosen slot and giving it to the target/victim slot
                        // print schedule before
                        for (Map.Entry <WorkerSlot, ArrayList<ExecutorDetails>> entry: newAssignment.entrySet()) {
                            for (ExecutorDetails ed :  entry.getValue()) {
                                LOG.info(" Slot {} executor {} target slot {} slot exchanged with {}", entry.getKey().toString(), ed.toString(), targetSlot.toString(), slotToExchangeWith.toString());
                            }
                        }
                        ArrayList<ExecutorDetails> slotToExchangeWithExecutors = flippedAssignment.get(slotToExchangeWith);
                        ExecutorDetails executorToBeGiven = slotToExchangeWithExecutors.get(0);
                        newAssignment.get(slotToExchangeWith).remove(executorToBeGiven);
                        newAssignment.get(targetSlot).add(executorToBeGiven);

                        LOG.info("Target slot expected num of executors {}", newNumExecutorsOnSlot);
                        LOG.info("Num of executors on target slot {} ", newAssignment.get(targetSlot).size());
                        LOG.info("Num of executors on other slot {} ", newAssignment.get(slotToExchangeWith).size());

                        // print schedule after
                        for (Map.Entry <WorkerSlot, ArrayList<ExecutorDetails>> entry: newAssignment.entrySet()) {
                            for (ExecutorDetails ed :  entry.getValue()) {
                                LOG.info(" Slot {} executor {} target slot {} slot exchanged with {}", entry.getKey().toString(), ed.toString(), targetSlot.toString(), slotToExchangeWith.toString());
                            }
                        }
                        LOG.info("Did we get a null pointer exception yet? ");
                        cluster.freeSlot(targetSlot);
                        LOG.info("Did we get a null pointer exception yet? 1");
                        cluster.freeSlot(slotToExchangeWith);
                        LOG.info("Did we get a null pointer exception yet? 2");
                        cluster.assign(slotToExchangeWith, target.getId(), newAssignment.get(slotToExchangeWith));
                        LOG.info("Did we get a null pointer exception yet? 3");
                        cluster.assign(targetSlot, target.getId(), newAssignment.get(targetSlot));
                        LOG.info("Did we get a null pointer exception yet? 4");
                    }
                } else {
                    LOG.info("Num of new executors is now zero for {}", status);
                }
            }
        }
    }


    private void reassignNewVictimScheduling (TopologyDetails victim, Cluster cluster, String status, ExecutorSummary victimExecutorSummary, int oldNumExecutorsOnSlot, int newNumExecutorsOnSlot, int numExecutorsTotal) {
        LOG.info("In reassignNewVictimScheduling status {}", status);
        LOG.info("{} topology {}", status, victim.getId());
        Map<ExecutorDetails, WorkerSlot> assignmentFromCluster = new HashMap<ExecutorDetails, WorkerSlot>();
        Map<ExecutorDetails, WorkerSlot> tmp = cluster.getAssignmentById(victim.getId()).getExecutorToSlot();
        for (Map.Entry<ExecutorDetails, WorkerSlot> e : tmp.entrySet()) {
            assignmentFromCluster.put(
                    new ExecutorDetails(e.getKey().getStartTask(), e.getKey().getEndTask()),
                    new WorkerSlot(e.getValue().getNodeId(), e.getValue().getPort()));
        } // deep copy

        if (assignmentFromCluster == null) {
            LOG.info("Assignment from cluster for {} topology is null", status);
        } else {
            Map<WorkerSlot, ArrayList<ExecutorDetails>> victimSchedule = globalState.getTopologySchedules().get(victim.getId()).getAssignment();
            for (WorkerSlot ws : victimSchedule.keySet()) {
                LOG.info("{} schedule worker slot {} ", status, ws.getNodeId(), ws.getPort());
            }
            String component = victimExecutorSummary.get_component_id();
            String supervisorId = getSupervisorIdFromNodeName(globalState.getSupervisorToNode(), victimExecutorSummary.get_host());
            if (supervisorId == null) {
                LOG.info("Supervisor ID is null for creating the worker slot for the {}", status);
            } else {

                LOG.info("From the {} topology", status);
                int port = victimExecutorSummary.get_port();
                for (WorkerSlot workerSlots : victimSchedule.keySet()) {
                    LOG.info(status + " slot port: " + workerSlots.getPort() + " slot host: " + workerSlots.getNodeId() + "\n");
                    if (status.equals("victim") && workerSlots.getNodeId().equals(supervisorId))
                        port = workerSlots.getPort();
                }
                WorkerSlot victimSlot = new WorkerSlot(supervisorId, port);
                LOG.info(status + " the slot we're trying to create slot port: " + victimSlot.getPort() + "slot host: " + victimSlot.getNodeId() + " \n");
                ArrayList<ExecutorDetails> victimSlotExecutors = victimSchedule.get(victimSlot); // these are the executors on the victim slot
                if (victimSlotExecutors != null) {
                    Map<WorkerSlot, ArrayList<ExecutorDetails>> flippedAssignment = flipMap(assignmentFromCluster);
                    Map<WorkerSlot, ArrayList<ExecutorDetails>> newAssignment = flipMap(assignmentFromCluster);
                    Map <ExecutorDetails, String> executorsToComponents =  victim.getExecutorToComponent();

                    LOG.info("we want flipped assignment to contain the executors of the component we want on all slots");
                    for (Map.Entry<ExecutorDetails, String> entry : executorsToComponents.entrySet())  {
                        LOG.info("status entry.key {} entry.component {}", status, entry.getKey(), entry.getValue());
                        for (Map.Entry <WorkerSlot, ArrayList<ExecutorDetails>> slotToExecutor :flippedAssignment.entrySet()) {
                            ArrayList <ExecutorDetails> details = slotToExecutor.getValue();
                            LOG.info("Looking at flipped assignment. Worker slot {} ", slotToExecutor.getKey().toString());
                            LOG.info("details.contains(entry.getKey()) {}  entry.getValue().equals(component) {}", details.contains(entry.getKey()) , entry.getValue().equals(component));
                            if (details.contains(entry.getKey()) && !entry.getValue().equals(component)) {
                                details.remove(entry.getKey());
                                LOG.info("Entry key removed from details {}", entry.getKey());
                                flippedAssignment.put(slotToExecutor.getKey(),details);
                            }
                        }
                    }
                    LOG.info("flipped assignment contains the executors of the component we want on all slots");
                    // flipped assignment contains the executors of the component we want on all slots

                    ArrayList <ExecutorDetails> compExecutorsOnSlot = flippedAssignment.get(victimSlot);
                    LOG.info("New expected number of executors on slot {}", newNumExecutorsOnSlot);
                    LOG.info("New actual number of executors on slot {}", oldNumExecutorsOnSlot);
                    if (compExecutorsOnSlot.size() == newNumExecutorsOnSlot) {
                        LOG.info("WE are good to go. Don't do anything else");
                    } else {

                        LOG.info("victim newNumExecutorsOnSlot on victim {}, ExecutorsTotal {} Num of workers {}", newNumExecutorsOnSlot, numExecutorsTotal, flippedAssignment.size());
                        if (numExecutorsTotal >= flippedAssignment.size())
                            newNumExecutorsOnSlot += 1;
                        LOG.info("victim newNumExecutorsOnSlot on victim {}, ExecutorsTotal {} Num of workers {}", newNumExecutorsOnSlot, numExecutorsTotal, flippedAssignment.size());
                        LOG.info("finding executor slot to exchange with");
                        WorkerSlot slotToExchangeWith = new WorkerSlot("", 0);
                        for (Map.Entry <WorkerSlot, ArrayList<ExecutorDetails>> entry: flippedAssignment.entrySet()) {
                            if (!entry.getKey().equals(victimSlot)) {
                                int numExecutorsOnSlot = entry.getValue().size();
                                LOG.info("slot {} executors of component on slot {}", entry.getKey().toString(), numExecutorsOnSlot);
                                if (numExecutorsOnSlot <= newNumExecutorsOnSlot) {
                                    slotToExchangeWith = entry.getKey();
                                    LOG.info("chosen slot {}", slotToExchangeWith.toString());
                                    break;
                                }
                            }
                        }
                        LOG.info("Slot chosen {}", slotToExchangeWith.toString());
                        // taking an executor from chosen slot and giving it to the target/victim slot
                        // print schedule before
                        for (Map.Entry <WorkerSlot, ArrayList<ExecutorDetails>> entry: newAssignment.entrySet()) {
                            for (ExecutorDetails ed :  entry.getValue()) {
                                LOG.info(" Slot {} executor {} victim slot {} slot exchanged with {}", entry.getKey().toString(), ed.toString(), victimSlot.toString(), slotToExchangeWith.toString());
                            }
                        }
                        ArrayList<ExecutorDetails> slotToExchangeWithExecutors = flippedAssignment.get(victimSlot);
                        ExecutorDetails executorToBeGiven = slotToExchangeWithExecutors.get(0);
                        newAssignment.get(victimSlot).remove(executorToBeGiven);
                        newAssignment.get(slotToExchangeWith).add(executorToBeGiven);

                        LOG.info("Victim slot expected num of executors {}", newNumExecutorsOnSlot);
                        LOG.info("Num of executors on victim slot {} ", newAssignment.get(victimSlot).size());
                        LOG.info("Num of executors on other slot {} ", newAssignment.get(slotToExchangeWith).size());

                        // print schedule after
                        for (Map.Entry <WorkerSlot, ArrayList<ExecutorDetails>> entry: newAssignment.entrySet()) {
                            for (ExecutorDetails ed :  entry.getValue()) {
                                LOG.info(" Slot {} executor {} victim slot {} slot exchanged with {}", entry.getKey().toString(), ed.toString(), victimSlot.toString(), slotToExchangeWith.toString());
                            }
                        }
                        cluster.freeSlot(slotToExchangeWith);
                        cluster.freeSlot(victimSlot);
                        cluster.assign(slotToExchangeWith, victim.getId(), newAssignment.get(slotToExchangeWith));
                        cluster.assign(victimSlot, victim.getId(), newAssignment.get(victimSlot));

                    }
                } else {
                    LOG.info("Num of new executors is now zero for {}", status);
                }
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
            LOG.info("Host : " + globalState.getSupervisorToNode().get(e.getValue().getNodeId()).hostname + "Port: " + e.getValue().getPort() + " Executor {}" + e.getKey().toString() + "\n");
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

    public Map<WorkerSlot, ArrayList<ExecutorDetails>> flipMap(Map<ExecutorDetails, WorkerSlot> map) {
        Map<WorkerSlot, ArrayList<ExecutorDetails>> flippedMap = new HashMap<>();
        for (Map.Entry<ExecutorDetails, WorkerSlot> entry : map.entrySet()) {
            if (!flippedMap.containsKey(entry.getValue())) {
                flippedMap.put(entry.getValue(), new ArrayList<ExecutorDetails>());
            }
            flippedMap.get(entry.getValue()).add(entry.getKey());
            LOG.info("Flipped map contains {} {}", entry.getValue(), entry.getKey());
        }
        return flippedMap;
    }

    public Integer findNumOfExecutorsOnSlot(TopologyDetails topologyDetails, TopologySchedule schedule, ExecutorSummary executorSummary) {
        Map<ExecutorDetails, String> executorToComponent = topologyDetails.getExecutorToComponent();
        ArrayList<ExecutorDetails> executors = new ArrayList<>();
        for (Map.Entry<ExecutorDetails, String> entry : executorToComponent.entrySet()) {
            if (entry.getValue().equals(executorSummary.get_component_id())) {
                executors.add(entry.getKey());
            }
        }
        String supId = getSupervisorIdFromNodeName(globalState.getSupervisorToNode(), executorSummary.get_host());
        ArrayList<ExecutorDetails> executorsOnSlot = schedule.getAssignment().get(new WorkerSlot(supId, executorSummary.get_port()));
        executorsOnSlot.retainAll(executors);
        return executorsOnSlot.size();
    }
}