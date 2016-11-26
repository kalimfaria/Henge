package backtype.storm.scheduler.advancedstela.etp;

import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.WorkerSlot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TopologySchedule {
    private final double CAPACITY_CONGESTION_THRESHOLD = 0.3; // all capacities above this value are "congested"
    private String id;
    private Integer numberOFWorkers;
    private HashMap<ExecutorDetails, Component> executorToComponent;
    private HashMap<WorkerSlot, ArrayList<ExecutorDetails>> assignment;
    private HashMap<String, Component> components;
    private static final Logger LOG = LoggerFactory.getLogger(TopologySchedule.class);

    public ArrayList<Component> getCapacityWiseCongestedOperators () {
        ArrayList<Component> result = new ArrayList<>();
        for (Map.Entry<String, Component> component:  components.entrySet()) {
            if (component.getValue().getCapacity() > CAPACITY_CONGESTION_THRESHOLD)
                result.add(component.getValue());
        }

        return result;
    }


    public ArrayList<Component> getCapacityWiseUncongestedOperators () {
        ArrayList<Component> result = new ArrayList<>();
        for (Map.Entry<String, Component> component:  components.entrySet()) {
            if (!component.getKey().contains("spout"))
                if (component.getValue().getCapacity() < CAPACITY_CONGESTION_THRESHOLD)
                    result.add(component.getValue());
        }
        Collections.sort(result);
        return result;
    }

    public TopologySchedule(String identifier, int workerCount) {
        id = identifier;
        numberOFWorkers = workerCount;
        components = new HashMap<String, Component>();
        executorToComponent = new HashMap<ExecutorDetails, Component>();
        assignment = new HashMap<WorkerSlot, ArrayList<ExecutorDetails>>();
    }

    public String getId() {
        return id;
    }

    public void setNumberOFWorkers(Integer numberOFWorkers) {
        numberOFWorkers = numberOFWorkers;
    }

    public void addExecutorToComponent(ExecutorDetails details, String componentId) {
        executorToComponent.put(details, components.get(componentId));
    }

    public void addAssignment(WorkerSlot slot, ExecutorDetails details) {
        if (!assignment.containsKey(slot)) {
            assignment.put(slot, new ArrayList<ExecutorDetails>());
        }
        ArrayList<ExecutorDetails> executorDetails = assignment.get(slot);
        executorDetails.add(details);
    }

    public void addComponents(String id, Component component) {
        components.put(id, component);
    }

    public Integer getNumberOFWorkers() {
        return numberOFWorkers;
    }

    public HashMap<ExecutorDetails, Component> getExecutorToComponent() {
        return executorToComponent;
    }

    public HashMap<WorkerSlot, ArrayList<ExecutorDetails>> getAssignment() {
        return assignment;
    }

    public HashMap<String, Component> getComponents() {
        return components;
    }

    public int getNumTasks(String component) {

        ArrayList<ExecutorDetails> executorsOfComponent = new ArrayList<>();
        for (Map.Entry<ExecutorDetails, Component> executor : executorToComponent.entrySet()) {
            if (executor.getValue().getId().equals(component)) {
                executorsOfComponent.add(executor.getKey());
            }
        }
        int start = Integer.MAX_VALUE, end = Integer.MIN_VALUE;

        for (ExecutorDetails e: executorsOfComponent){
            if (start > e.getStartTask())
                start = e.getStartTask();
            if (end < e.getEndTask())
                end = e.getEndTask();
        }

        LOG.info("Component {} Start {} End {} Range {}", component, start, end, (end-start+1));
        return end-start+1;// offset because tasks start with 1 :)
    }
}
