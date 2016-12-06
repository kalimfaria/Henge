package backtype.storm.scheduler.advancedstela;

import backtype.storm.scheduler.*;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.advancedstela.etp.*;
import backtype.storm.scheduler.advancedstela.slo.Observer;
import backtype.storm.scheduler.advancedstela.slo.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.util.*;

public class AdvancedStelaScheduler implements IScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(AdvancedStelaScheduler.class);
    Long time;
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

    public void prepare(@SuppressWarnings("rawtypes") Map conf) {
        juice_log = new File("/tmp/output.log");
        etpLog = new File("/tmp/etp.log");

        config = conf;
        sloObserver = new Observer(conf);
        globalState = new GlobalState(conf);
        globalStatistics = new GlobalStatistics(conf);
        selector = new OperatorSelector();
        history = new ArrayList<>();
        time = System.currentTimeMillis();
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
        runAdvancedStelaComponents(cluster, topologies);
        // new backtype.storm.scheduler.EvenScheduler().schedule(topologies, cluster);
        LOG.info("cluster utilization {}", globalState.isClusterUtilization());
        if (numTopologiesThatNeedScheduling > 0) {
            LOG.info("STORM IS GOING TO PERFORM THE REBALANCING");
            new backtype.storm.scheduler.EvenScheduler().schedule(topologies, cluster);
        } else if (numTopologiesThatNeedScheduling == 0 && (System.currentTimeMillis() - time) / 1000 > 5 * 60) {
            LOG.info("((victims.isEmpty() && targets.isEmpty()) && numTopologiesThatNeedScheduling == 0 && numTopologies > 0)");
            boolean wasReductionSuccessful = false;
            if (globalState.isClusterUtilization() && !didWeReduce) {
                LOG.info("going to check utilization");
                wasReductionSuccessful = doReduction(topologies);
                LOG.info("Did first reduce {}", wasReductionSuccessful);
            }
            if (!wasReductionSuccessful) {
                LOG.info("going to go to rebalance helper");
                rebalanceHelper(topologies);
            }
            time = System.currentTimeMillis(); //-- this forces rebalance to occur every 5 mins instead -_-
        }
    }


    private void rebalanceHelper(Topologies topologies) {
        LOG.info("rebalance helper");
        ArrayList<Topology> receiver_topologies = sloObserver.getFailingTopologies();
        History now = createHistory(topologies);
        boolean doWeNeedToRevert = false;
        if (history.size() > 0) {
            doWeNeedToRevert = history.get(history.size() - 1).doWeNeedToRevert(now);
        }

        if (doWeStop && doWeNeedToRevert) {
            LOG.info("We stopped rebalancing earlier but it " +
                            "looks like workload has changed doWeStop {} doWeNeedToRevert {} time {}", doWeStop, doWeNeedToRevert,
                    System.currentTimeMillis());
            LOG.info("Flushing history");
            history.clear();
            doWeStop = false;
            didWeReduce = false;
            LOG.info("did we stop {} did we reduce {}", doWeStop, didWeReduce);
            // if we have stopped rebalancing, and yet system utility falls suddenly.
            // now, this could happen because of two reasons. 1) We see poor performance because of increasing latency etc without changing workload
            // 2) the workload changes. So we set a threshold of 10% again. allow it to fall and then flush history and do rebalance
            // we go with 2) and flush history so no reversions can happen and start again
        }

        if (didWeDoRebalance) { // if there was a rebalance, then check if it was a bad idea
            LOG.info("Did we do rebalance? Yes");
            if (doWeNeedToRevert && !doWeStop) {
                History bestHistory = findBestHistory();
                revertHistory(bestHistory);
                decrementStability();
                doWeStop = true;
                LOG.info("Finished reverting");
                LOG.info("Now stopping all rebalance");
            }
            didWeDoRebalance = false;
        }

        if (!doWeStop) {
            LOG.info("Length of receivers {}", receiver_topologies.size());
            if (receiver_topologies.size() > 0) {
                // ONE TOPOLOGY THAT IS REBALANCED
                Topology receiver = new TopologyPicker().pickTopology(receiver_topologies, briefHistory);
                LOG.info("Picked the topology for rebalance");
                TopologyDetails target = topologies.getById(receiver.getId());
                TopologySchedule targetSchedule = globalState.getTopologySchedules().get(receiver.getId());
                Component targetComponent = selector.selectOperator(globalState, globalStatistics, receiver);
                LOG.info("target before rebalanceTwoTopologies {} ", target.getId());
                if (targetComponent != null) {
                    LOG.info("topology {} target component", receiver, targetComponent.getId());
                    rebalanceTopology(target, targetSchedule, targetComponent, receiver, now);
                    decrementStability();

                    didWeDoRebalance = true;
                }
// ALL DEM TOPOLOGIES ARE REBALANCED ALL TOGETHER
             /*   for (Topology receiver: receiver_topologies) {
                    LOG.info("Picked the topology for rebalance");
                    TopologyDetails target = topologies.getById(receiver.getId());
                    TopologySchedule targetSchedule = globalState.getTopologySchedules().get(receiver.getId());
                    Component targetComponent = selector.selectOperator(globalState, globalStatistics, receiver);
                    LOG.info("target before rebalanceTwoTopologies {} ", target.getId());
                    if (targetComponent != null) {
                        LOG.info("topology {} target component", receiver, targetComponent.getId());
                        rebalanceTopology(target, targetSchedule, targetComponent, receiver, now);
                        didWeDoRebalance = true;
                    }
                }
                decrementStability(); */
            } else if (receiver_topologies.size() == 0) {
                LOG.info("There are no receivers!\n");
                // if this persists for 4 rounds, then truncate history. We be stable yo!
                LOG.info("Houston,we're stable");
                incrementStability();

            }
        }
    }

    private void revertHistory(History bestHistory) {

        if (config != null) {
            try {
                HashMap<String, TopologyDetails> topologySchedules = bestHistory.getTopologiesSchedule();
                for (Map.Entry<String, TopologyDetails> schedule : topologySchedules.entrySet()) {
                    String topologyName = schedule.getKey();
                    TopologyDetails details = schedule.getValue();
                    LOG.info("Reverting :) topology name {}", topologyName);
                    writeToFile(juice_log, "Reverting\n");
                    String targetCommand = "/var/nimbus/storm/bin/storm " +
                            "rebalance " + topologyName + " -w 0 ";

                    Map<String, Integer> componentToExecutor = flipExecsMap(details.getExecutorToComponent());
                    for (Map.Entry<String, Integer> entry : componentToExecutor.entrySet()) {
                        targetCommand += " -e " + entry.getKey() + "=" + entry.getValue();
                    }
                    writeToFile(juice_log, targetCommand + "\n");
                    writeToFile(juice_log, System.currentTimeMillis() + "\n");
                    LOG.info(targetCommand + "\n");
                    LOG.info(System.currentTimeMillis() + "\n");
                    Runtime.getRuntime().exec(targetCommand);
                    //sloObserver.updateLastRebalancedTime(target.getId(), System.currentTimeMillis() / 1000);
                    sloObserver.clearTopologySLOs(topologyName);
                }
            } catch (Exception e) {
                LOG.info(e.toString());
                LOG.info("Revert history in only exception");
            }
        }
    }

    private boolean doReduction(Topologies topologies) {
        LOG.info("do reduction");
        decrementStability();
        ArrayList<Topology> successfulTopologies = sloObserver.getSuccesfulTopologies();
        HashMap<String, TopologySchedule> topologySchedules = globalState.getTopologySchedules();
        if (successfulTopologies.size() == 0) return false;

        for (Topology successfulTopology : successfulTopologies) {
            TopologySchedule schedule = topologySchedules.get(successfulTopology.getId());
            ArrayList<Component> uncongestedComponents = schedule.getCapacityWiseUncongestedOperators();

            writeToFile(juice_log, "Reduction\n");
            String targetCommand = "/var/nimbus/storm/bin/storm " +
                    "rebalance " + topologies.getById(successfulTopology.getId()).getName() + " -w 0 ";
            for (Component comp : uncongestedComponents) {
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
                // sloObserver.updateLastRebalancedTime(target.getId(), System.currentTimeMillis() / 1000);
                sloObserver.clearTopologySLOs(schedule.getId());

            } catch (Exception e) {
                LOG.info(e.toString());
            }
        }
        history.clear();
        briefHistory.clear(); // DO WE NEED TO DO THIS?
        didWeReduce = true;
        return true;
    }

    private void rebalanceTopology(TopologyDetails targetDetails,
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

    private void runAdvancedStelaComponents(Cluster cluster, Topologies topologies) {
        sloObserver.run();
        globalState.collect(cluster, topologies);
        globalState.setCapacities(sloObserver.getAllTopologies());
        globalStatistics.collect();

        for (TopologyDetails t : topologies.getTopologies()) {
            TopologySchedule targetSchedule = globalState.getTopologySchedules().get(t.getId());
            TopologyStatistics targetStatistics = globalStatistics.getTopologyStatistics().get(t.getId());

            ETPStrategy targetStrategy = new ETPStrategy(targetSchedule, targetStatistics);
            ArrayList<ResultComponent> component = targetStrategy.topologyETPRankDescending();

            if (component != null) {
                for (ResultComponent comp : component) {
                    writeToFile(etpLog, "topology name " + t.getId() + " " +
                            "congested chosen component: " + comp.component.getId() + " " + comp.etpValue + " " +
                            System.currentTimeMillis() + "\n");
                }
            }
        }
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


    public History findBestHistory() {
        Collections.sort(history);
        LOG.info("Finding best histories");
        if (history.size() > 0) {
            History currentBest = history.get(history.size() - 1);
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

    public Map<String, Integer> flipExecsMap(Map<ExecutorDetails, String> ExecutorsToComponents) {
        HashMap<String, Integer> flippedMap = new HashMap<>();
        for (Map.Entry<ExecutorDetails, String> entry : ExecutorsToComponents.entrySet()) {
            if (flippedMap.containsKey(entry.getValue())) {
                flippedMap.put(entry.getValue(), (flippedMap.get(entry.getValue()) + 1));
            } else {
                flippedMap.put(entry.getValue(), 1);
            }
        }
        return flippedMap;
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
            areWeStable = 0;
        }
        LOG.info("From increment stability: {} ", areWeStable);
    }
}
