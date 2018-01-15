package backtype.storm.scheduler.advancedstela;

import backtype.storm.scheduler.*;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.advancedstela.etp.*;
import backtype.storm.scheduler.advancedstela.slo.Latencies;
import backtype.storm.scheduler.advancedstela.slo.Observer;
import backtype.storm.scheduler.advancedstela.slo.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.SynchronousQueue;

public class AdvancedStelaScheduler implements IScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(AdvancedStelaScheduler.class);
    Long time, upForMoreThan;
    boolean didWeDoRebalance, doWeStop, didWeReduce;
    @SuppressWarnings("rawtypes")
    private Map config;
    private Observer sloObserver;
    private GlobalState globalState;
    private GlobalStatistics globalStatistics;
    private OperatorSelector selector;
    private File juice_log;
    private ArrayList<History> history;
    private ArrayList<BriefHistory> briefHistory;
    private int areWeStable;
    private File etpLog;
    private String stormCommand = "/var/redis/nimbus/bin/storm ";

    public void prepare(@SuppressWarnings("rawtypes") Map conf) {
        juice_log = new File("/tmp/output.log");
        etpLog = new File("/tmp/etp.log");
        config = conf;
        sloObserver = new Observer(conf);
        globalState = new GlobalState(conf);
        globalStatistics = new GlobalStatistics(conf);
        selector = new OperatorSelector();
        history = new ArrayList<>();
        upForMoreThan = time = System.currentTimeMillis();
        didWeDoRebalance = false;
        doWeStop = false;
        didWeReduce = false;
        areWeStable = 0;
        briefHistory = new ArrayList<>();
    }

    public void schedule(Topologies topologies, Cluster cluster) {
        logUnassignedExecutors(cluster.needsSchedulingTopologies(topologies), cluster);
        int numTopologiesThatNeedScheduling = cluster.needsSchedulingTopologies(topologies).size();
        LOG.info("numTopologiesThatNeedScheduling {}", numTopologiesThatNeedScheduling);
        boolean failures = runAdvancedStelaComponents(cluster, topologies);
        LOG.info("Failures {}" , failures);
        LOG.info("cluster utilization {}", globalState.isClusterUtilization());
        if (!failures) {  // false means failures
            LOG.info("Failures!!! Clearing state");
            briefHistory.clear();
            history.clear();
            didWeDoRebalance = false;
            doWeStop = false;
            didWeReduce = false;
            areWeStable = 0;
            upForMoreThan = System.currentTimeMillis();
            // reset value to 5 only
            backtype.storm.scheduler.advancedstela.slo.Topologies.UP_TIME = 60 * 5;
            LOG.info("State reset.");


        }
        else {
            if (numTopologiesThatNeedScheduling > 0) {
                LOG.info("STORM IS GOING TO PERFORM THE REBALANCING");
                new backtype.storm.scheduler.EvenScheduler().schedule(topologies, cluster);
                doWeStop = false;
            } else if (numTopologiesThatNeedScheduling == 0
                    && (System.currentTimeMillis() - time) / 1000 > 60
                    && (System.currentTimeMillis() - upForMoreThan) / 1000 > backtype.storm.scheduler.advancedstela.slo.Topologies.UP_TIME) {

                LOG.info("((victims.isEmpty() && targets.isEmpty()) && numTopologiesThatNeedScheduling == 0 && numTopologies > 0)");
                rebalanceHelper(topologies);
                time = System.currentTimeMillis(); //-- this forces rebalance to occur every 5 mins instead -_-

            } 
        }
    }


    private void rebalanceHelper(Topologies topologies) {
        LOG.info("rebalance helper");
        LOG.info("history size {}", history.size());
        History now = createHistory(topologies);
        boolean didUtilityFall = false;
        if (history.size() > 0) {
            didUtilityFall = history.get(history.size() - 1).doWeNeedToRevert(now);
        }
        LOG.info("didUtilityFall {}", didUtilityFall);
        if (didUtilityFall) {
            if (!doWeStop && didWeDoRebalance) {
                /// NOW CHECK CPU UTIL
                boolean wasReductionSuccessful = false;
                LOG.info("going to check utilization");
                if (globalState.isClusterUtilization() && !didWeReduce) {
                    wasReductionSuccessful = doReduction(topologies);
                }
                if (!wasReductionSuccessful) { // we did not do a reduction
                    History bestHistory = findBestHistory(now);
                    boolean wasReversionSuccessful = revertHistory(bestHistory);
                    if (wasReversionSuccessful) {
                        doWeStop = true;
                        decrementStability();
                    }
                }
                // not in convergence state // ADD IF - CHECK UTIL - DO REDUCE IF NEEDED ELSE DO REDUCE AND STOP -- do revert and then stop
                didWeDoRebalance = false;
                return; // break out
            } else if (doWeStop) {
                // in convergence state
                // get out of convergence state and do rebalance now
                doWeStop = false;
                history.clear();
                briefHistory.clear();
                didWeDoRebalance = false;
                didWeReduce = false;
            }
        }
        LOG.info("doWeStop {} didWeDoRebalance {} didWeReduce {}", doWeStop, didWeDoRebalance, didWeReduce);

        ArrayList<Topology> receiver_topologies = sloObserver.getFailingTopologies();
        LOG.info("Size of receiver topologies: {}, doWeStop: {}, history.size : {} ", receiver_topologies.size(), doWeStop, history.size());
        if (receiver_topologies.size() == 0 && doWeStop && history.size() == 0) {
            saveHistory(now);
            LOG.info("Saving history so that we have something to compare to later to");
            writeToFile(juice_log, "TriggeredInstability\n");
         //   doWeStop = false;
        }

        if (!doWeStop) {
            if (receiver_topologies.size() == 0) {
                LOG.info("There are no receivers!\n");
                // if this persists for 4 rounds, then truncate history. We be stable yo!
                LOG.info("Houston,we're stable");
                incrementStability();
                return;
            }

            boolean wasRebalanceSuccessful = false;
            int topologyNum = 0;
            ArrayList<Topology> sorted_receivers = new TopologyPicker().pickTopology(receiver_topologies, briefHistory);
            LOG.info("Length of receivers {}", receiver_topologies.size());

            while (!wasRebalanceSuccessful && topologyNum < receiver_topologies.size()) {
                Topology receiver = sorted_receivers.get(topologyNum);
                if (receiver != null) {
                    LOG.info("topologyNum {} ", topologyNum);
                    topologyNum ++;
                    // ONE TOPOLOGY THAT IS REBALANCED
                    LOG.info("Picked the topology for rebalance");
                    TopologyDetails target = topologies.getById(receiver.getId());
                    TopologySchedule targetSchedule = globalState.getTopologySchedules().get(receiver.getId());
                    //Component targetComponent = selector.selectOperator(globalState, globalStatistics, receiver);
                    ArrayList<ResultComponent> targetComponents = selector.selectAllOperators(globalState, globalStatistics, receiver);
                    LOG.info("target before rebalanceTwoTopologies {} ", target.getId());
                    if (targetComponents != null) {
                        LOG.info("topology {} target component", receiver, targetComponents.size());
                        wasRebalanceSuccessful = rebalanceTopology(target, targetSchedule, targetComponents, receiver, now);
                        if (wasRebalanceSuccessful) {
                            sloObserver.updateLastRebalancedTime(target.getId(), System.currentTimeMillis() / 1000);
                        }
                        LOG.info("topologyNum {} wasRebalanceSuccessful {} ", topologyNum, wasRebalanceSuccessful);
                    }
                } else  {
                    LOG.info("receivers are null");
                }
            }
            decrementStability();
            didWeDoRebalance = true;
        }
    }

    private boolean revertHistory(History bestHistory) {
        if (config != null && bestHistory != null) {
            try {
                boolean areWeDone = false;

                HashMap<String, TopologyDetails> topologySchedules = bestHistory.getTopologiesSchedule();
                for (Map.Entry<String, TopologyDetails> schedule : topologySchedules.entrySet()) {
                    String topologyName = schedule.getKey();
                    TopologyDetails details = schedule.getValue();
                    LOG.info("Reverting :) topology name {}", topologyName);
                    writeToFile(juice_log, "Reverting\n");
                    String targetCommand = stormCommand +
                            "rebalance " + topologyName + " -w 0 ";
                    Map<String, Integer> componentToExecutor = new Helpers().flipExecsMap(details.getExecutorToComponent());
                    boolean localAreWeDone = false;
                    for (Map.Entry<String, Integer> entry : componentToExecutor.entrySet()) {
                        targetCommand += " -e " + entry.getKey() + "=" + entry.getValue();
                        areWeDone = true;
                        localAreWeDone = true;
                    }
                    if (localAreWeDone) {
                        writeToFile(juice_log, targetCommand + "\n");
                        writeToFile(juice_log, System.currentTimeMillis() + "\n");
                        LOG.info(targetCommand + "\n");
                        LOG.info(System.currentTimeMillis() + "\n");
                        Runtime.getRuntime().exec(targetCommand);
                        Runtime.getRuntime().exec("fab delete");
                        //sloObserver.updateLastRebalancedTime(target.getId(), System.currentTimeMillis() / 1000);
                        sloObserver.clearTopologySLOs(topologyName);
                    }
                }
                return areWeDone;
            } catch (Exception e) {
                LOG.info(e.toString());
                LOG.info("Revert history in only exception");
                return false;
            }
         } else {
            return false;
        }
    }

    private boolean doReduction(Topologies topologies) {
        LOG.info("do reduction");
        ArrayList<Topology> successfulTopologies = sloObserver.getSuccesfulTopologies();
        HashMap<String, TopologySchedule> topologySchedules = globalState.getTopologySchedules();
        if (successfulTopologies.size() == 0) return false;
        boolean reduction = false;

        for (Topology successfulTopology : successfulTopologies) {
            TopologySchedule schedule = topologySchedules.get(successfulTopology.getId());
            ArrayList<Component> uncongestedComponents = schedule.getCapacityWiseUncongestedOperators();
            writeToFile(juice_log, "Reduction\n");
            String targetCommand = stormCommand +
                    "rebalance " + topologies.getById(successfulTopology.getId()).getName() + " -w 0 ";

            for (Component comp : uncongestedComponents) {
                reduction = true;
                LOG.info("Parallelism: {} ", comp.getParallelism());
                int reducedExecutors = (int) (comp.getParallelism() * 0.2);
                LOG.info("Before reducing executors to 1 " + reducedExecutors + "\n");
                if (reducedExecutors <= 0) reducedExecutors = 1;
                targetCommand += "-e " + comp.getId() + "=" + reducedExecutors + " ";
                LOG.info("Reduced executors " + targetCommand + "\n");
                LOG.info(System.currentTimeMillis() + "\n");
                LOG.info("running the rebalance using storm's rebalance command \n");
                LOG.info("Component : {}", comp);
                for (Map.Entry<String, Component> print : schedule.getComponents().entrySet()) {
                    LOG.info("Name of component {}, Component {} ", print.getKey(), print.getValue().getId());
                }
                schedule.getComponents().get(comp.getId()).setParallelism(reducedExecutors);
            }
            try {
                writeToFile(juice_log, targetCommand + "\n");
                writeToFile(juice_log, System.currentTimeMillis() + "\n");
                Runtime.getRuntime().exec(targetCommand);
                Runtime.getRuntime().exec("fab delete");
                // sloObserver.updateLastRebalancedTime(target.getId(), System.currentTimeMillis() / 1000);
                sloObserver.clearTopologySLOs(schedule.getId());

            } catch (Exception e) {
                LOG.info(e.toString());
            }
        }
        if (!reduction) return false;

        decrementStability();
        history.clear();
        briefHistory.clear(); // DO WE NEED TO DO THIS?
        didWeReduce = true;
        return true;
    }

    /*private void rebalanceTopology(TopologyDetails targetDetails,
                                   TopologySchedule target,
                                   Component component,
                                   Topology targetTopology,
                                   History now) {
        LOG.info("In rebalance topology");
        if (config != null) {
            try {
                //int one = 2;
                int one = targetTopology.getExecutorsForRebalancing();
                String targetComponent = component.getId();
                Integer targetOldParallelism = target.getComponents().get(targetComponent).getParallelism();
                Integer targetNewParallelism = targetOldParallelism + one;
                Integer targetTasks = target.getNumTasks(targetComponent);
                LOG.info("Num of tasks {} new Parallelism {}", targetTasks, targetNewParallelism);
                if (targetNewParallelism > targetTasks && targetOldParallelism < targetTasks) // so this is the turning point
                    targetNewParallelism = targetTasks;
                if (targetTasks >= targetNewParallelism) {
                    saveHistory(now); // There is no point in saving history if you don't plan on doing rebalance
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

                        briefHistory.add(new BriefHistory(targetDetails.getId(), System.currentTimeMillis(), targetTopology.getCurrentUtility()));
                        Runtime.getRuntime().exec(targetCommand);
                        // sloObserver.updateLastRebalancedTime(target.getId(), System.currentTimeMillis() / 1000);
                        sloObserver.clearTopologySLOs(target.getId());
                    } catch (Exception e) {
                        LOG.info(e.toString());
                        LOG.info("In first exception");
                        //e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                LOG.info(e.toString());
                LOG.info("In second exception");
                return;
            }
        }
    }
 */

    private boolean rebalanceTopology(TopologyDetails targetDetails,
                                   TopologySchedule target,
                                   ArrayList<ResultComponent> components,
                                   Topology targetTopology,
                                   History now) {
        int first_time = 0;
        LOG.info("In rebalance topology");
        if (config != null) {
            try {
                String targetCommand = stormCommand +
                        "rebalance " + targetDetails.getName() + " -w 0 ";

                for (ResultComponent comp:  components) {
                    int one = comp.getExecutorsForRebalancing();
                    String targetComponent = comp.component.getId();
                    Integer targetOldParallelism = target.getComponents().get(targetComponent).getParallelism();
                    Integer targetNewParallelism = targetOldParallelism + one;
                    Integer targetTasks = target.getNumTasks(targetComponent);
                    LOG.info("Num of tasks {} new Parallelism {} oldParallelism 1 {}", targetTasks, targetNewParallelism, targetOldParallelism);
                    if (targetNewParallelism > targetTasks) { // so this is the turning point
                        targetNewParallelism = targetTasks;
                    }
                    LOG.info("Num of tasks {} new Parallelism {} oldParallelism 2 {}", targetTasks, targetNewParallelism, targetOldParallelism);
                    if (targetOldParallelism < targetNewParallelism) {
                        LOG.info("targetOldParallelism < targetNewParallelism for operator {}", targetComponent);
                        if (first_time == 0) {
                            first_time = 1;
                            LOG.info("Saved history");
                            saveHistory(now); // There is no point in saving history if you don't plan on doing rebalance
                        }
                        targetCommand +=  " -e " + targetComponent + "=" + targetNewParallelism;
                        target.getComponents().get(targetComponent).setParallelism(targetNewParallelism);
                    }
                }

                try {
                    if (first_time == 1) {
                        LOG.info("Can perform a rebalance");
                        writeToFile(juice_log, targetCommand + "\n");
                        writeToFile(juice_log, System.currentTimeMillis() + "\n");
                        LOG.info(targetCommand + "\n");
                        LOG.info(System.currentTimeMillis() + "\n");
                        LOG.info("running the rebalance using storm's rebalance command \n");
                        LOG.info("Target old executors count {}", targetDetails.getExecutors().size());

                        briefHistory.add(new BriefHistory(targetDetails.getId(), System.currentTimeMillis(), targetTopology.getCurrentUtility()));
                        Runtime.getRuntime().exec(targetCommand);
                        Runtime.getRuntime().exec("fab delete");
                        // sloObserver.updateLastRebalancedTime(target.getId(), System.currentTimeMillis() / 1000);
                        sloObserver.clearTopologySLOs(target.getId());
                        return true;
                    }
                } catch (Exception e) {
                    LOG.info(e.toString());
                    LOG.info("In first exception");
                    //e.printStackTrace();
                    return false;
                }
            } catch (Exception e) {
                LOG.info(e.toString());
                LOG.info("In second exception");
                return false;
            }
        }
        return false;
    }

    private boolean runAdvancedStelaComponents(Cluster cluster, Topologies topologies) {
        sloObserver.run();
        boolean failures = globalState.collect(cluster, topologies);
        globalState.setCapacities(sloObserver.getAllTopologies());
        globalStatistics.collect();
        return failures;
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


    public History findBestHistory(History now) {
        Collections.sort(history);
        LOG.info("Finding best histories");
        if (history.size() > 0) {
            History currentBest = history.get(history.size() - 1);
            if (currentBest.equals(now)) {
                LOG.info("currentBest.equals(now) NOT");
                return null;
            }
            LOG.info("currentBest.equals(now)");
            return currentBest; // getting the last one
        }
        return null;
    }

    public History createHistory(Topologies topologies) {
        LOG.info("create History");

        HashMap<String, TopologyDetails> tops = new HashMap<>();

        for (TopologyDetails t : topologies.getTopologies()) {
            tops.put(t.getId(), t);
        }
        History h = new History(tops, sloObserver.getAllTopologies());

        // LOGGING INFO
        HashMap<String, Topology> topologiesPerformance = h.getTopologiesPerformance();
        HashMap<String, TopologyDetails> topologySchedules = h.getTopologiesSchedule();

        for (Map.Entry<String, Topology> entry : topologiesPerformance.entrySet()) {
            Topology t = entry.getValue();
            LOG.info(" name {} topology {} average latency {} latency slo {} juice {} throughput slo {} ", entry.getKey(), t.getId(), t.getAverageLatency(), t.getUserSpecifiedLatencySLO(), t.getUserSpecifiedSLO(), t.getMeasuredSLO());
        }

        for (Map.Entry<String, TopologyDetails> entry : topologySchedules.entrySet()) {
            TopologyDetails t = entry.getValue();
            LOG.info(" name {} topology {} workers {} conf {} num of executors {}" +
                            " executors {} ",
                    entry.getKey(),
                    t.getId(),
                    t.getNumWorkers(),
                    t.getConf(),
                    t.getExecutorToComponent().size(),
                    t.getExecutors());
        }
        return h;
    }

    public void saveHistory(History h) {
        LOG.info("save History");

        history.add(h);

        // LOGGING INFO
        HashMap<String, Topology> topologiesPerformance = h.getTopologiesPerformance();
        HashMap<String, TopologyDetails> topologySchedules = h.getTopologiesSchedule();

        for (Map.Entry<String, Topology> entry : topologiesPerformance.entrySet()) {
            Topology t = entry.getValue();
            LOG.info(" name {} topology {} average latency {} latency slo {} juice {} throughput slo {} ", entry.getKey(), t.getId(), t.getAverageLatency(), t.getUserSpecifiedLatencySLO(), t.getUserSpecifiedSLO(), t.getMeasuredSLO());
        }

        for (Map.Entry<String, TopologyDetails> entry : topologySchedules.entrySet()) {
            TopologyDetails t = entry.getValue();
            LOG.info(" name {} topology {} workers {} conf {} num of executors {}" +
                            " executors {} ",
                    entry.getKey(),
                    t.getId(),
                    t.getNumWorkers(),
                    t.getConf(),
                    t.getExecutorToComponent().size(),
                    t.getExecutors());
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

    public void decrementStability() {
        areWeStable--;
        if (areWeStable < 0) areWeStable = 0;
        LOG.info("From decrement stability: {} ", areWeStable);
    }

    public void incrementStability() {
        areWeStable++;
        if (areWeStable == 4) {
            history.clear();
            briefHistory.clear();
            areWeStable = 0;
            didWeReduce = false;
            doWeStop = true;
        }
        LOG.info("From increment stability: {} ", areWeStable);
    }
}