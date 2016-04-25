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
import java.util.Map.Entry;

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
    private File flatline_log;
    private File outlier_log;
    private File same_top;

    public void prepare(@SuppressWarnings("rawtypes") Map conf) {
        juice_log = new File("/tmp/output.log");
        outlier_log = new File("/tmp/outlier.log");
        flatline_log = new File("/tmp/flat_line.log");
        same_top = new File("/tmp/same_top.log");

        config = conf;
        sloObserver = new Observer(conf);
        globalState = new GlobalState(conf);
        globalStatistics = new GlobalStatistics(conf);
        selector = new Selector();
        victims = new HashMap<String, ExecutorPair>();
        targets = new HashMap<String, ExecutorPair>();

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
        writeToFile(same_top, "In AdvancedStelaScheduler *");
        writeToFile(same_top, "In schedule function");
        logUnassignedExecutors(cluster.needsSchedulingTopologies(topologies), cluster);
        if (cluster.needsSchedulingTopologies(topologies).size() > 0) {

            StringBuffer sb = new StringBuffer();
            sb.append("cluster.needsSchedulingTopologies(topologies).size() > 0\n");
            sb.append("Before calling EvenScheduler: \n");
            sb.append("Size of cluster.needsSchedulingTopologies(topologies): " + cluster.needsSchedulingTopologies(topologies).size() +  "\n");


             List<TopologyDetails> topologiesScheduled = cluster.needsSchedulingTopologies(topologies);

            sb.append("targets.length() : " + targets.size() + "\n");
            sb.append( "victims.length(): " + victims.size() + "\n");

            for (TopologyDetails topologyThatNeedsToBeScheduled : topologiesScheduled) {
                sb.append("Id of topology: " + topologyThatNeedsToBeScheduled.getId() + "\n");
            }

          /*  if (targetToVictimMapping.size() > 0) {
                applyRebalancedScheduling(cluster, topologies);
            }
*/
            if (!targets.isEmpty()) {
                sb.append("!targets.isEmpty()\n");
                decideAssignmentForTargets(topologies, cluster);
            }

            if (!victims.isEmpty()) {
                sb.append("!victims.isEmpty()\n");
                decideAssignmentForVictims(topologies, cluster);

            }

            writeToFile(flatline_log, sb.toString());
            new backtype.storm.scheduler.EvenScheduler().schedule(topologies, cluster);
            runAdvancedStelaComponents(cluster, topologies);
        } else if (cluster.needsSchedulingTopologies(topologies).size() == 0 && topologies.getTopologies().size() > 0) {

            runAdvancedStelaComponents(cluster, topologies);

            TopologyPairs topologiesToBeRescaled = sloObserver.getTopologiesToBeRescaled();
            ArrayList <String> receivers = topologiesToBeRescaled.getReceivers();
            ArrayList <String> givers = topologiesToBeRescaled.getGivers();

            writeToFile(same_top, "Got the topology pairs in schedule()");
            writeToFile(same_top, "Checking after topologies are set into the variables");
            writeToFile(same_top, "Givers:");
            for (String t: givers)
                writeToFile(same_top, "topology: " + t);
            writeToFile(same_top, "Receivers:");
            for (String t: receivers)
                writeToFile(same_top, "topology: " + t);

            removeAlreadySelectedPairs(receivers, givers);

            if (receivers.size() > 0 && givers.size() > 0) {

                ArrayList <String> topologyPair = new TopologyPicker().worstTargetBestVictim(receivers, givers);
                String receiver = topologyPair.get(0);
                String giver = topologyPair.get(1);
                TopologyDetails target = topologies.getById(receiver);
                TopologySchedule targetSchedule = globalState.getTopologySchedules().get(receiver);
                TopologyDetails victim = topologies.getById(giver);
                TopologySchedule victimSchedule = globalState.getTopologySchedules().get(giver);
                ExecutorPair executorSummaries =
                        selector.selectPair(globalState, globalStatistics, receiver, giver);

                if (executorSummaries.bothPopulated()) {

                    writeToFile(flatline_log, "Trying to rebalance\n");
                    writeToFile(flatline_log, "victim: "+victim.getId()+"\n");
                    writeToFile(flatline_log, "target: "+target.getId() + "\n"); ;;; /// WHAT?
                    rebalanceTwoTopologies(target, targetSchedule, victim, victimSchedule, executorSummaries);
                } else {
                    writeToFile(flatline_log, "Cannot find 2 pairs of executor summaries - BOO\n");
                }
            } else if (givers.size() == 0) {
                StringBuffer sb = new StringBuffer();
                sb.append("There are no givers! *Sob* \n");
                sb.append("Receivers:  \n");

                for (int i = 0; i < receivers.size(); i++)
                    sb.append(receivers.get(i) + "\n");
                writeToFile(flatline_log, sb.toString() );

            }
        }
    }

    private void decideAssignmentForTargets(Topologies topologies, Cluster cluster) {
        List<TopologyDetails> unscheduledTopologies = cluster.needsSchedulingTopologies(topologies);
        writeToFile(flatline_log, "decideAssignmentForTargets \n" );
        for (TopologyDetails topologyDetails: unscheduledTopologies) {
            if (targets.containsKey(topologyDetails.getId()) && cluster.getAssignmentById(topologyDetails.getId()) != null) {
                writeToFile(flatline_log, "topologyDetails.getId():  " + topologyDetails.getId() + "\n"  );
                findAssignmentForTarget(topologyDetails, cluster, topologyDetails.getId());

            }
        }
    }

    private void decideAssignmentForVictims(Topologies topologies, Cluster cluster) {
        writeToFile(flatline_log, "decideAssignmentForVictims \n" );
        List<TopologyDetails> unscheduledTopologies = cluster.needsSchedulingTopologies(topologies);
        for (TopologyDetails topologyDetails: unscheduledTopologies) {
            if (victims.containsKey(topologyDetails.getId()) && cluster.getAssignmentById(topologyDetails.getId()) != null) {
                writeToFile(flatline_log, "topologyDetails.getId():  " + topologyDetails.getId() + "\n"  );
                findAssignmentForVictim(topologyDetails, cluster, topologyDetails.getId());

            }
        }
    }

    private void findAssignmentForTarget(TopologyDetails target, Cluster cluster, String topologyId) {
        writeToFile(flatline_log, topologyId + " findAssignmentForTarget \n"  );
        ExecutorPair executorPair = targets.get(topologyId);
        reassignTargetNewScheduling(target, cluster, executorPair);
    }

    private void findAssignmentForVictim(TopologyDetails victim, Cluster cluster, String topologyId) {
        writeToFile(flatline_log, topologyId + " findAssignmentForVictim\n"  );
        ExecutorPair executorPair = victims.get(topologyId);
        reassignVictimNewScheduling(victim, cluster, executorPair);

    }

    private void runAdvancedStelaComponents(Cluster cluster, Topologies topologies) {
        writeToFile(same_top, "In AdvancedStelaScheduler **");
        writeToFile(same_top, "In schedule function");
        sloObserver.run();
        globalState.collect(cluster, topologies);
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

    private void rebalanceTwoTopologies(TopologyDetails targetDetails, TopologySchedule target,
                                        TopologyDetails victimDetails, TopologySchedule victim, ExecutorPair executorSummaries) {
        if (config != null) {
            try {
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
                // FORMAT /var/nimbus/storm/bin/storm henge-rebalance  production-topology1 -e bolt_output_sink=13 xyz production-topology2 -e spout_head=12 xyz  production-topology3 -e bolt_output_sink=13 xyz production-topology4 -e spout_head=12
                try {


                    writeToFile(outlier_log, targetCommand + "\n");
                    writeToFile(outlier_log, System.currentTimeMillis() + "\n");
                    writeToFile(outlier_log, victimCommand + "\n");
                    writeToFile(juice_log, targetCommand + "\n");
                    writeToFile(juice_log, System.currentTimeMillis() + "\n");
                    writeToFile(juice_log, victimCommand + "\n");

                    Runtime.getRuntime().exec(targetCommand);
                    Runtime.getRuntime().exec(victimCommand);

                    sloObserver.updateLastRebalancedTime(target.getId(),System.currentTimeMillis() / 1000);
                    sloObserver.updateLastRebalancedTime(victim.getId(),System.currentTimeMillis() / 1000);

                    targets.put(target.getId(), executorSummaries);
                    victims.put(victim.getId(), executorSummaries);
                    sloObserver.clearTopologySLOs(target.getId());
                    sloObserver.clearTopologySLOs(victim.getId());


                 //   writeToFile(advanced_scheduling_log, "Name of target topology: " + targetID + "\n");
                 //   writeToFile(advanced_scheduling_log, "Name of victim topology: " + victimID + "\n");
                 //   writeToFile(advanced_scheduling_log, "End of rebalanceTwoTopologies\n");
                 //   writeToFile(advanced_scheduling_log, "\n targetComponent  :" + targetComponent + "\n");

                } catch (Exception e) {
                    e.printStackTrace();
                }
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
        }
    }

    private void removeAlreadySelectedPairs(ArrayList <String> receivers, ArrayList <String> givers) {


        for (String target : targets.keySet()) {
            int targetIndex = receivers.indexOf(target);
            if (targetIndex != -1) {
                receivers.remove(targetIndex);
                writeToFile(same_top, "Removed " + target + "from receivers as it was already chosen previously");
                //writeToFile(flatline_log, target + " removed from receivers \n");
            }
        }
        for (String victim : victims.keySet()) {
            int victimIndex = givers.indexOf(victim);
            if (victimIndex != -1) {
                givers.remove(victimIndex);
                writeToFile(same_top, "Removed " + victim + "from receivers as it was already chosen previously");
                //writeToFile(flatline_log, victim + " removed from receivers \n");
            }
        }
    }

 /*   private void applyRebalancedScheduling(Cluster cluster, Topologies topologies) {
        for (Map.Entry<String, String> targetToVictim : targetToVictimMapping.entrySet()) {
            final TopologyDetails target = topologies.getById(targetToVictim.getKey());
            TopologyDetails victim = topologies.getById(targetToVictim.getValue());
            ExecutorPair executorPair = targetToNodeMapping.get(targetToVictim.getKey());
            writeToFile(advanced_scheduling_log, "\n New schedule for the target is created: " + (cluster.getAssignmentById(target.getId()) != null) + "\n");
            if (cluster.getAssignmentById(target.getId()) != null)
                reassignTargetNewScheduling(target, cluster, executorPair);
            writeToFile(advanced_scheduling_log, "\n New schedule for the victim is created: " + (cluster.getAssignmentById(victim.getId()) != null) + "\n");
            if (cluster.getAssignmentById(victim.getId()) != null)
                reassignVictimNewScheduling(victim, cluster, executorPair);
            if (targetID.length() < 1 && victimID.length() < 1) {
                targetToVictimMapping.remove(target.getId());
                targetToNodeMapping.remove(target.getId());
            }
        }
    }
    */

    private void reassignTargetNewScheduling(TopologyDetails target, Cluster cluster,
                                             ExecutorPair executorPair) {



        writeToFile(flatline_log, "Reassign Target New Scheduling:  "  + target.getName() + "\n");


        ExecutorSummary targetExecutorSummary = executorPair.getTargetExecutorSummary();

        WorkerSlot targetSlot = new WorkerSlot(targetExecutorSummary.get_host(), targetExecutorSummary.get_port());
        writeToFile(flatline_log, "Target Worker Slot:  " + targetSlot.toString() + "\n");


        Map<WorkerSlot, ArrayList<ExecutorDetails>> targetSchedule = globalState.getTopologySchedules().get(target.getId()).getAssignment();
        Set<ExecutorDetails> previousTargetExecutors = globalState.getTopologySchedules().get(target.getId()).getExecutorToComponent().keySet();
        
        writeToFile(flatline_log, "\n************** Target Topology **************" + "\n");

        ArrayList<Integer> previousTargetTasks = new ArrayList<Integer>();
        ArrayList<Integer> currentTargetTasks = new ArrayList<Integer>();
        SchedulerAssignment currentTargetAssignment = cluster.getAssignmentById(target.getId());
        writeToFile(flatline_log, "\n************** Target Topology **************" + "\n");
        printSchedule(currentTargetAssignment);
        if (currentTargetAssignment != null) {
            Set<ExecutorDetails> currentTargetExecutors = currentTargetAssignment.getExecutorToSlot().keySet();

           // writeToFile(advanced_scheduling_log, "\n****** Current Executors ******\n");
            for (ExecutorDetails executorDetails : currentTargetExecutors) {
                currentTargetTasks.add(executorDetails.getStartTask());
                currentTargetTasks.add(executorDetails.getEndTask());
            }

            currentTargetExecutors.removeAll(previousTargetExecutors);
            currentTargetTasks.removeAll(previousTargetTasks);
            LOG.info("Affected TargetSlot Executors:");
            for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> topologyEntry : targetSchedule.entrySet()) {
                if (topologyEntry.getKey().equals(targetSlot)) {
                    ArrayList<ExecutorDetails> executorsOfOldTarget = topologyEntry.getValue();
                    for(int i=0;i<executorsOfOldTarget.size();i++){
                    	LOG.info("{}",executorsOfOldTarget.get(i));
                    }
                    executorsOfOldTarget.addAll(currentTargetExecutors);
                    targetSchedule.put(targetSlot, executorsOfOldTarget);
                }
            }
        }
        for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> topologyEntry : targetSchedule.entrySet()) {
            if (cluster.getUsedSlots().contains(topologyEntry.getKey())) {
                cluster.freeSlot(topologyEntry.getKey());
            }
            cluster.assign(topologyEntry.getKey(), target.getId(), topologyEntry.getValue());
        }

        targets.remove(target.getId());
        writeToFile(flatline_log, "Removed " + target.getId() + "from targets \n");
      //  targetID = new String();
    }


    private void printSchedule(SchedulerAssignment currentTargetAssignment) {
		// TODO Auto-generated method stub
    	Map<ExecutorDetails, WorkerSlot> map = currentTargetAssignment.getExecutorToSlot();
    	for(Entry<ExecutorDetails, WorkerSlot> e: map.entrySet()){
            writeToFile(flatline_log,  "WorkerSlot : " + e.getValue() +  " Executor {}" +  e.getKey().toString() + "\n");
    	}
    	
	}

	private void reassignVictimNewScheduling(TopologyDetails victim, Cluster cluster,
                                             ExecutorPair executorPair) {

       // writeToFile(advanced_scheduling_log, "Only the victim topology needs to be rescheduled. Woot, we made it to stage II");

        writeToFile(flatline_log, "Reassign Victim New Scheduling:  " + victim.getName() + "\n");

        Map<WorkerSlot, ArrayList<ExecutorDetails>> victimSchedule = globalState.getTopologySchedules().get(victim.getId()).getAssignment();
        ExecutorSummary victimExecutorSummary = executorPair.getVictimExecutorSummary();
        WorkerSlot victimSlot = new WorkerSlot(victimExecutorSummary.get_host(), victimExecutorSummary.get_port());
        writeToFile(flatline_log, "Victim Worker Slot: "  +  victimSlot.toString() + " \n");
        writeToFile(flatline_log, "slot for Victim: " + victimSlot.toString() + "\n");
      //  writeToFile(advanced_scheduling_log, "\n************** Victim Topology **************\n");
        Set<ExecutorDetails> previousVictimExecutors = globalState.getTopologySchedules().get(victim.getId()).getExecutorToComponent().keySet();
        SchedulerAssignment currentVictimAssignment = cluster.getAssignmentById(victim.getId());
        writeToFile(flatline_log, "Current Victim Assignment: \n");
        printSchedule(currentVictimAssignment);
        ArrayList<Integer> previousVictimTasks = new ArrayList<Integer>();
        ArrayList<Integer> otherTaskofVictimExecutor = new ArrayList<Integer>();
        for (ExecutorDetails executorDetails : previousVictimExecutors) {
            previousVictimTasks.add(executorDetails.getStartTask());
            previousVictimTasks.add(executorDetails.getEndTask());
        }

        if (currentVictimAssignment != null) {
            Set<ExecutorDetails> currentVictimExecutors = currentVictimAssignment.getExecutorToSlot().keySet();
            ArrayList<Integer> currentVictimTasks = new ArrayList<Integer>();
            for (ExecutorDetails executorDetails : currentVictimExecutors) {
                currentVictimTasks.add(executorDetails.getStartTask());
                currentVictimTasks.add(executorDetails.getEndTask());
            }

            previousVictimExecutors.removeAll(currentVictimExecutors);
            previousVictimTasks.removeAll(currentVictimTasks);

            for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> topologyEntry : victimSchedule.entrySet()) {

                for (Integer removedTasks : previousVictimTasks) {

                    ArrayList<ExecutorDetails> oldExecutors = topologyEntry.getValue();
                    Iterator iterator = oldExecutors.iterator();

                    while (iterator.hasNext()) {
                        ExecutorDetails oldExecutor = (ExecutorDetails) iterator.next();
                        if (removedTasks == oldExecutor.getStartTask()) {
                            otherTaskofVictimExecutor.add(oldExecutor.getEndTask());
                            iterator.remove();
                        } else if (removedTasks == oldExecutor.getEndTask()) {
                            otherTaskofVictimExecutor.add(oldExecutor.getStartTask());
                            iterator.remove();
                        }
                    }
                    victimSchedule.put(topologyEntry.getKey(), oldExecutors);
                }
            }

            for (Map.Entry<ExecutorDetails, WorkerSlot> topologyEntry : currentVictimAssignment.getExecutorToSlot().entrySet()) {
                for (Integer taskToAddBack : otherTaskofVictimExecutor) {
                    if (taskToAddBack == topologyEntry.getKey().getStartTask() || taskToAddBack == topologyEntry.getKey().getEndTask()) {
                        victimSchedule.get(victimSlot).add(topologyEntry.getKey());
                    }
                }
            }
        }

        for (Map.Entry<WorkerSlot, ArrayList<ExecutorDetails>> topologyEntry : victimSchedule.entrySet()) {
            if (cluster.getUsedSlots().contains(topologyEntry.getKey())) {
                cluster.freeSlot(topologyEntry.getKey());
            }
            cluster.assign(topologyEntry.getKey(), victim.getId(), topologyEntry.getValue());
        }
        victims.remove((victim.getId()));
        writeToFile(flatline_log, "Removed " + victim.getId() + "from victim \n");
    }

    public void writeToFile(File file, String data) {
        try {
            FileWriter fileWriter = new FileWriter(file, true);
            BufferedWriter bufferWriter = new BufferedWriter(fileWriter);
            bufferWriter.append(data);
            bufferWriter.close();
            fileWriter.close();
          //  LOG.info("wrote to file {}", data);
        } catch (IOException ex) {
            LOG.info("error! writing to file {}", ex);
        }
    }
}
