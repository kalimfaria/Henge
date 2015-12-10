/*package backtype.storm.scheduler.advancedstela;

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
    private File advanced_scheduling_log;

    public void prepare(@SuppressWarnings("rawtypes") Map conf) {
        rebalance_log = new File("/var/nimbus/storm/rebalance.log");
        advanced_scheduling_log = new File("/var/nimbus/storm/advanced_scheduling_log.log");
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
        logUnassignedExecutors(cluster.needsSchedulingTopologies(topologies), cluster);
        if (cluster.needsSchedulingTopologies(topologies).size() > 0) {

            new backtype.storm.scheduler.EvenScheduler().schedule(topologies, cluster);

            List<TopologyDetails> topologiesScheduled = cluster.needsSchedulingTopologies(topologies);


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
        writeToFile(advanced_scheduling_log, "In logUnassignedExecutors\n");
        for (TopologyDetails topologyDetails : topologiesScheduled) {
            Collection<ExecutorDetails> unassignedExecutors = cluster.getUnassignedExecutors(topologyDetails);


            writeToFile(advanced_scheduling_log, "In logUnassignedExecutors \n ******** Topology Assignment " + topologyDetails.getId() + " ********* \n ");

            //  LOG.info("******** Topology Assignment {} *********", topologyDetails.getId());
            //  System.out.println("******** Topology Assignment " + topologyDetails.getId() + " *********");
            LOG.info("Unassigned executors size: {}", unassignedExecutors.size());
            writeToFile(advanced_scheduling_log, "Unassigned executors size: " + unassignedExecutors.size() + "\n");
            StringBuffer forOutputLog = new StringBuffer();
            //   System.out.println("Unassigned executors size: "+unassignedExecutors.size()+"");
            if (unassignedExecutors.size() > 0) {
                for (ExecutorDetails executorDetails : unassignedExecutors) {
                    //   LOG.info(executorDetails.toString());
                    //   System.out.println("executorDetails.toString(): " + executorDetails.toString());
                    forOutputLog.append("executorDetails.toString(): " + executorDetails.toString());
                }
            }

            writeToFile(advanced_scheduling_log, forOutputLog.toString() + " \n end of logUnassignedExecutors \n " + "\n");
        }
    }

    private void rebalanceTwoTopologies(TopologyDetails targetDetails, TopologySchedule target,
                                        TopologyDetails victimDetails, TopologySchedule victim, ExecutorPair executorSummaries) {
        String targetComponent = executorSummaries.getTargetExecutorSummary().get_component_id();
        String targetCommand = "/var/nimbus/storm/bin/storm " +
                "rebalance " + targetDetails.getName() + " -w 0 -e " +
                targetComponent + "=" + (target.getComponents().get(targetComponent).getParallelism() + 1);
        // System.out.println(targetCommand);


        writeToFile(rebalance_log, targetDetails.getName() + "," + targetComponent + "," + (target.getComponents().get(targetComponent).getParallelism() + 1 + "," + System.currentTimeMillis() + "\n"));
        // writeToFile(advanced_scheduling_log, "In rebalance Two Topologies: \n" + targetCommand);
        String victimComponent = executorSummaries.getVictimExecutorSummary().get_component_id();
        String victimCommand = "/var/nimbus/storm/bin/storm " +
                "rebalance " + victimDetails.getName() + " -w 0 -e " +
                victimComponent + "=" + (victim.getComponents().get(victimComponent).getParallelism() - 1);
        // System.out.println(victimCommand);
        writeToFile(rebalance_log, victimDetails.getName() + "," + victimComponent + "," + (victim.getComponents().get(victimComponent).getParallelism() + 1 + "," + System.currentTimeMillis() + "\n"));

        //   writeToFile(advanced_scheduling_log, "In rebalance Two Topologies: \ntargetCommand: " + targetCommand + "\nvictimCommand: "+ victimCommand);
        try {

            //   LOG.info("Triggering rebalance for target: {}, victim: {}\n", targetDetails.getId(), victimDetails.getId());
            //    writeToFile(advanced_scheduling_log, "In rebalance Two Topologies: \ntargetCommand: " + targetCommand + "\nvictimCommand: " + victimCommand);

            //   LOG.info(targetCommand + "\n");
            //   LOG.info(victimCommand + "\n");

            Runtime.getRuntime().exec(targetCommand);
            Runtime.getRuntime().exec(victimCommand);

            targetToVictimMapping.put(target.getId(), victim.getId());
            targetToNodeMapping.put(target.getId(), executorSummaries);
            //   System.out.println("sloObserver.clearTopologySLOs(target.getId());: " + target.getId());
            sloObserver.clearTopologySLOs(target.getId());
            //    System.out.println("sloObserver.clearTopologySLOs(victim.getId());: " + victim.getId());


            sloObserver.clearTopologySLOs(victim.getId());

            writeToFile(advanced_scheduling_log, "In rebalance Two Topologies: \ntargetCommand: " + targetCommand + "\nvictimCommand: " + victimCommand + "\n:sloObserver.clearTopologySLOs(target.getId()): " + target.getId() + "\nvictim.getId()): " + victim.getId() + "\n end of rebalanceTwoTopologies" + "\n");

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

        writeToFile(advanced_scheduling_log, "In reassignNewScheduling: \ntargetSlot: " + targetSlot.toString() + "\nvictimSlot: " + victimSlot.toString() + "\npreviousTargetExecutors for target topology: " + target.getId() + "\n");

        Set<ExecutorDetails> previousTargetExecutors = globalState.getTopologySchedules().get(target.getId()).getExecutorToComponent().keySet();

        writeToFile(advanced_scheduling_log, "\n************** Target Topology **************" + "\n");

        writeToFile(advanced_scheduling_log, "\n****** Previous Target Executors ******" + "\n");
        for (ExecutorDetails executorDetails : previousTargetExecutors) {
            writeToFile(advanced_scheduling_log, executorDetails.toString() + "\n");
        }

        Map<WorkerSlot, ArrayList<ExecutorDetails>> oldTargetAssignment  = globalState.getTopologySchedules().get(target.getId()).getAssignment();

        writeToFile(advanced_scheduling_log, "\n****** Previous Target Assignment ******" + "\n");
        for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> oldAssignmentEntry : oldTargetAssignment.entrySet()) {
            writeToFile(advanced_scheduling_log, " workerslot: " + oldAssignmentEntry.getKey().toString() + " executors: " + oldAssignmentEntry.getValue().toString() + "\n");
        }

        SchedulerAssignment currentTargetAssignment = cluster.getAssignmentById(target.getId());


        Map<WorkerSlot, ArrayList<ExecutorDetails>> targetScheduling = new HashMap<>();//globalState.getTopologySchedules().get(target.getId()).getAssignment();
        Map<ExecutorDetails, WorkerSlot> targetExecutorToSlot = currentTargetAssignment.getExecutorToSlot();
        for (Map.Entry<ExecutorDetails, WorkerSlot> targetExecutorDetails : targetExecutorToSlot.entrySet()) {
            if (targetScheduling.containsKey(targetExecutorDetails.getValue())) // if it contains that worker slot
            {
                ArrayList<ExecutorDetails> targetExecutors = targetScheduling.get(targetExecutorDetails.getValue());
                targetExecutors.add(targetExecutorDetails.getKey());
                targetScheduling.put(targetExecutorDetails.getValue(), targetExecutors);
            } else {
                ArrayList<ExecutorDetails> targetExecutors = new ArrayList<ExecutorDetails>();
                targetExecutors.add(targetExecutorDetails.getKey());
                targetScheduling.put(targetExecutorDetails.getValue(), targetExecutors);

            }

        }

        writeToFile(advanced_scheduling_log, "\n****** currentTargetAssignment ******\n" + currentTargetAssignment.getExecutorToSlot().toString() + "\n");
        if (currentTargetAssignment != null) {
            Set<ExecutorDetails> currentTargetExecutors = currentTargetAssignment.getExecutorToSlot().keySet();

            writeToFile(advanced_scheduling_log, "\n****** Current Target Executors ******" + "\n");
            for (ExecutorDetails executorDetails : currentTargetExecutors) {
                writeToFile(advanced_scheduling_log, executorDetails.toString() + "\n");
            }
            currentTargetExecutors.removeAll(previousTargetExecutors); // contains the new executors

            for (ExecutorDetails newExecutor : currentTargetExecutors) {
                writeToFile(advanced_scheduling_log, "\n********************** Found new Target executor *********************" + "\n");
                writeToFile(advanced_scheduling_log, newExecutor.toString() + "\n");
            }


            if (currentTargetExecutors != null) {
                for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> targetScheduleEntry : targetScheduling.entrySet()) {

                    writeToFile(advanced_scheduling_log, "Old scheduling for target: " + targetScheduleEntry.toString() + " \n");
                    if (targetScheduleEntry.getKey() == targetSlot) // add it to the slot
                    {
                        ArrayList<ExecutorDetails> executorDetailsForTargetSlot = targetScheduleEntry.getValue();
                        executorDetailsForTargetSlot.addAll(currentTargetExecutors);
                        targetScheduling.put(targetSlot, executorDetailsForTargetSlot);
                        writeToFile(advanced_scheduling_log, "New scheduling for target slot: " + targetSlot.toString() + " executorDetails: " + executorDetailsForTargetSlot.toString() + " \n");
                    }
                    // delete from other places
                    else {
                        ArrayList<ExecutorDetails> executorDetailsForTargetSlot = targetScheduleEntry.getValue();
                        ArrayList<ExecutorDetails> temp = executorDetailsForTargetSlot;
                        for (ExecutorDetails newExecutor : currentTargetExecutors) {
                            if (executorDetailsForTargetSlot.contains(newExecutor)) {
                                temp.remove(newExecutor);
                            }
                        }
                        targetScheduling.put(targetScheduleEntry.getKey(), temp);
                    }
                }
            }

            if (currentTargetExecutors == null)
                writeToFile(advanced_scheduling_log, "currentTargetExecutors is null\n");
        }


        writeToFile(advanced_scheduling_log, "\n************** Victim Topology **************" + "\n");
        writeToFile(advanced_scheduling_log, "previousTargetExecutors for victim topology: " + victim.getId() + "\n");

        Set<ExecutorDetails> previousVictimExecutors = globalState.getTopologySchedules().get(victim.getId()).getExecutorToComponent().keySet();
        SchedulerAssignment currentVictimAssignment = cluster.getAssignmentById(victim.getId());
        writeToFile(advanced_scheduling_log, "\n****** currentVictimAssignment ******\n" + currentVictimAssignment.getExecutorToSlot().toString() + "\n");
        writeToFile(advanced_scheduling_log, "\n****** Previous Victim Executors ******" + "\n");

        Map<WorkerSlot, ArrayList<ExecutorDetails>> oldVictimAssignment  = globalState.getTopologySchedules().get(victim.getId()).getAssignment();


        writeToFile(advanced_scheduling_log, "\n****** Previous Victim Assignment ******" + "\n");
        for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> oldAssignmentEntry : oldVictimAssignment.entrySet()) {
            writeToFile(advanced_scheduling_log, " workerslot: " + oldAssignmentEntry.getKey().toString() + " executors: " + oldAssignmentEntry.getValue().toString() + "\n");
        }

        Map<WorkerSlot, ArrayList<ExecutorDetails>> victimScheduling = new HashMap<>();//globalState.getTopologySchedules().get(target.getId()).getAssignment();
        Map<ExecutorDetails, WorkerSlot> victimExecutorToSlot = currentVictimAssignment.getExecutorToSlot();
        for (Map.Entry<ExecutorDetails, WorkerSlot> victimExecutorDetails : victimExecutorToSlot.entrySet()) {
            if (victimScheduling.containsKey(victimExecutorDetails.getValue())) // if it contains that worker slot
            {
                ArrayList<ExecutorDetails> victimExecutors = victimScheduling.get(victimExecutorDetails.getValue());
                victimExecutors.add(victimExecutorDetails.getKey());
                victimScheduling.put(victimExecutorDetails.getValue(), victimExecutors);
            } else {
                ArrayList<ExecutorDetails> victimExecutors = new ArrayList<ExecutorDetails>();
                victimExecutors.add(victimExecutorDetails.getKey());
                victimScheduling.put(victimExecutorDetails.getValue(), victimExecutors);

            }

        }


        for (ExecutorDetails executorDetails : previousVictimExecutors) {
            writeToFile(advanced_scheduling_log, executorDetails.toString());
        }

        if (currentVictimAssignment != null) {
            Set<ExecutorDetails> currentVictimExecutors = currentVictimAssignment.getExecutorToSlot().keySet();

            writeToFile(advanced_scheduling_log, "\n****** Current Victim Executors ******" + "\n");
            for (ExecutorDetails executorDetails : currentVictimExecutors) {
                writeToFile(advanced_scheduling_log, executorDetails.toString() + "\n");
            }
            previousVictimExecutors.removeAll(currentVictimExecutors);

            for (ExecutorDetails executorToRemove : previousVictimExecutors) {
                writeToFile(advanced_scheduling_log, "********************** Removed Victim executor *********************" + "\n");
                writeToFile(advanced_scheduling_log, executorToRemove.toString() + "\n");
            }

         //   if (previousVictimExecutors != null ) {
//
  //              for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> victimScheduleEntry : victimScheduling.entrySet()) {
//
//                    writeToFile(advanced_scheduling_log, "Old scheduling for victim: " + victimScheduleEntry.toString() + " \n");//
                    if (victimScheduleEntry.getKey() == victimSlot) // add it to the slot
                    //{
                      //  ArrayList<ExecutorDetails> executorDetailsForVictimSlot = victimScheduleEntry.getValue();
                       // executorDetailsForVictimSlot.addAll(currentVictimExecutors);
                        //victimScheduling.put(victimSlot, executorDetailsForVictimSlot);
                        //writeToFile(advanced_scheduling_log, "New scheduling for target slot: " + victimSlot.toString() + " executorDetails: " + executorDetailsForVictimSlot.toString() + " \n");
                   // }
                    // delete from other places
                   /// else {
                     //   ArrayList<ExecutorDetails> executorDetailsForVictimSlot = victimScheduleEntry.getValue();
                      //  for (ExecutorDetails newExecutor : currentVictimExecutors) // there is an assumption here  - that executors to add should be on this slot
                       // {
                        //    if (executorDetailsForVictimSlot.contains(newExecutor))
                        //        executorDetailsForVictimSlot.remove(newExecutor);
//
  //                      }
    //                    victimScheduling.put(victimSlot, executorDetailsForVictimSlot);
      //              }
        //        }
          //  }

            if (previousVictimExecutors == null)
                writeToFile(advanced_scheduling_log, "previousVictimExecutors is null\n");
        }

        writeToFile(advanced_scheduling_log, "Assigning target topology: \n");
        for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> targetscheduling : targetScheduling.entrySet()) {
            writeToFile(advanced_scheduling_log, "Assigning worker slot: " + targetscheduling.getKey() + " executor details: " + targetscheduling.getValue() + "\n");
            cluster.assign(targetscheduling.getKey(), target.getId(), targetscheduling.getValue());
        }

        writeToFile(advanced_scheduling_log, "Assigning victim topology: \n");
        for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> victimscheduling : victimScheduling.entrySet()) {
            writeToFile(advanced_scheduling_log, "Assigning worker slot: " + victimscheduling.getKey() + " executor details: " + victimscheduling.getValue() + "\n");
            cluster.assign(victimscheduling.getKey(), victim.getId(), victimscheduling.getValue());
        }

        targetToVictimMapping.remove(target.getId());
        targetToNodeMapping.remove(target.getId());

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
}
*/

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
    private File advanced_scheduling_log;

    public void prepare(@SuppressWarnings("rawtypes") Map conf) {
        rebalance_log = new File("/var/nimbus/storm/rebalance.log");
        advanced_scheduling_log = new File("/var/nimbus/storm/advanced_scheduling_log.log");
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


            if (targetToVictimMapping.size() > 0) {
                applyRebalancedScheduling(cluster, topologies);
            }

            globalState.collect(cluster, topologies);
            globalStatistics.collect();


        } else if (cluster.needsSchedulingTopologies(topologies).size() == 0 && topologies.getTopologies().size() > 0){

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
        writeToFile(advanced_scheduling_log, "In logUnassignedExecutors\n");

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
        String targetComponent = executorSummaries.getTargetExecutorSummary().get_component_id();
        String targetCommand = "/Users/sharanyabathey/courses/mcs-fall2015/individual-study/multitenant-stela/runs/nimbus/apache-storm-0.10.1-SNAPSHOT/bin/storm " +
                "rebalance " + targetDetails.getName() + " -e " +
                targetComponent + "=" + (target.getComponents().get(targetComponent).getParallelism() + 1);

        String victimComponent = executorSummaries.getVictimExecutorSummary().get_component_id();
        String victimCommand = "/Users/sharanyabathey/courses/mcs-fall2015/individual-study/multitenant-stela/runs/nimbus/apache-storm-0.10.1-SNAPSHOT/bin/storm " +
                "rebalance " + victimDetails.getName() + " -e " +
                victimComponent + "=" + (victim.getComponents().get(victimComponent).getParallelism() - 1);

        try {

            writeToFile(advanced_scheduling_log, "Triggering rebalance for target: " + targetDetails.getId()+  ", victim: " + victimDetails.getId() + "\n");
            writeToFile(advanced_scheduling_log, targetCommand + "\n");
            writeToFile(advanced_scheduling_log, victimCommand + "\n");

            Runtime.getRuntime().exec(targetCommand);
            Runtime.getRuntime().exec(victimCommand);

            targetToVictimMapping.put(target.getId(), victim.getId());
            targetToNodeMapping.put(target.getId(), executorSummaries);
            sloObserver.clearTopologySLOs(target.getId());
            sloObserver.clearTopologySLOs(victim.getId());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void removeAlreadySelectedPairs(ArrayList<String> receivers, ArrayList<String> givers) {
        for (String target: targetToVictimMapping.keySet()) {
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
        for (Map.Entry<String, String> targetToVictim: targetToVictimMapping.entrySet()) {
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

        Map <WorkerSlot, ArrayList<ExecutorDetails>> targetSchedule = globalState.getTopologySchedules().get(target.getId()).getAssignment();
        Map <WorkerSlot, ArrayList<ExecutorDetails>> victimSchedule = globalState.getTopologySchedules().get(victim.getId()).getAssignment();

        Set<ExecutorDetails> previousTargetExecutors = globalState.getTopologySchedules().get(target.getId()).getExecutorToComponent().keySet();

        writeToFile(advanced_scheduling_log, "\n************** Target Topology **************" + "\n");

        writeToFile(advanced_scheduling_log, "\n****** Previous Executors ******" + "\n");
        for (ExecutorDetails executorDetails: previousTargetExecutors) {
            writeToFile(advanced_scheduling_log, executorDetails.toString() + "\n");
        }

        SchedulerAssignment currentTargetAssignment = cluster.getAssignmentById(target.getId());
        if (currentTargetAssignment != null) {
            Set<ExecutorDetails> currentTargetExecutors = currentTargetAssignment.getExecutorToSlot().keySet();


            writeToFile(advanced_scheduling_log, "\n****** Current Executors ******\n");
            for (ExecutorDetails executorDetails: currentTargetExecutors) {
                writeToFile(advanced_scheduling_log, executorDetails.toString() + "\n");
            }
            currentTargetExecutors.removeAll(previousTargetExecutors);
            writeToFile(advanced_scheduling_log, "\n********************** Found new executor *********************\n");
            for (ExecutorDetails newExecutor: currentTargetExecutors) {
                writeToFile(advanced_scheduling_log, newExecutor.toString() + "\n");
            }

            for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> topologyEntry : targetSchedule.entrySet())
            {
                if (topologyEntry.getKey().equals(targetSlot)) // they will not be in the old map
                {
                    // add the two executors
                    ArrayList<ExecutorDetails> executorsOfOldTarget =  topologyEntry.getValue();
                    executorsOfOldTarget.addAll(currentTargetExecutors);
                    targetSchedule.put(targetSlot,executorsOfOldTarget);
                }
            }
        }

        writeToFile(advanced_scheduling_log, "\n************** Victim Topology **************\n");

        Set<ExecutorDetails> previousVictimExecutors = globalState.getTopologySchedules().get(victim.getId()).getExecutorToComponent().keySet();
        SchedulerAssignment currentVictimAssignment = cluster.getAssignmentById(victim.getId());

        writeToFile(advanced_scheduling_log, "\n****** Previous Executors ******\n");
        for (ExecutorDetails executorDetails: previousVictimExecutors) {
            writeToFile(advanced_scheduling_log, executorDetails.toString() + "\n");
        }

        if (currentVictimAssignment != null) {
            Set<ExecutorDetails> currentVictimExecutors = currentVictimAssignment.getExecutorToSlot().keySet();


            writeToFile(advanced_scheduling_log, "\n****** Current Executors ******");
            for (ExecutorDetails executorDetails: currentVictimExecutors) {
                writeToFile(advanced_scheduling_log, executorDetails.toString() + "\n");
            }
            previousVictimExecutors.removeAll(currentVictimExecutors);
            for (ExecutorDetails newExecutor: previousVictimExecutors) {
                writeToFile(advanced_scheduling_log, "********************** Removed executor *********************\n");
                writeToFile(advanced_scheduling_log, newExecutor.toString() + "\n");
            }

            for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> topologyEntry : victimSchedule.entrySet())
            {
                if (topologyEntry.getKey().equals(victimSlot)) // they will not be in the old map
                {
                    // add the two executors
                    ArrayList<ExecutorDetails> executorsOfOldVictim =  topologyEntry.getValue();
                    executorsOfOldVictim.removeAll(previousVictimExecutors);
                    victimSchedule.put(victimSlot,executorsOfOldVictim);
                }
            }
        }


        writeToFile(advanced_scheduling_log,"New Assignment for Target Topology: \n");
        for (Map.Entry <WorkerSlot, ArrayList<ExecutorDetails>> topologyEntry: targetSchedule.entrySet())
        {
            writeToFile(advanced_scheduling_log, "Worker: " + topologyEntry.getKey() + " Executors: " + topologyEntry.getValue().toString() + "\n");
            cluster.assign(topologyEntry.getKey(), target.getId(), topologyEntry.getValue());

        }

        writeToFile(advanced_scheduling_log,"New Assignment for Victim Topology: \n");
        for (Map.Entry <WorkerSlot, ArrayList<ExecutorDetails>> topologyEntry: victimSchedule.entrySet())
        {
            writeToFile(advanced_scheduling_log, "Worker: " + topologyEntry.getKey() + " Executors: " + topologyEntry.getValue().toString() + "\n");
            cluster.assign(topologyEntry.getKey(), victim.getId(), topologyEntry.getValue());

        }

        targetToVictimMapping.remove(target.getId());
        targetToNodeMapping.remove(target.getId());
    }

    private List<ExecutorDetails> difference(Collection<ExecutorDetails> execs1,
                                             Collection<ExecutorDetails> execs2) {
        List<ExecutorDetails> result = new ArrayList<ExecutorDetails>();
        for (ExecutorDetails exec : execs1) {
            if(!execs2.contains(exec)) {
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