package backtype.storm.scheduler.advancedstela.etp;

import java.util.HashMap;
import java.util.List;

public class TopologyStatistics {
    private String id;
    private Long startupTime;
    private HashMap<String, ComponentStatistics> componentStatistics;
    public HashMap<String, List<Integer>> transferThroughputHistory;
    public HashMap<String, List<Integer>> emitThroughputHistory;
    public HashMap<String, List<Integer>> executeThroughputHistory;

    public TopologyStatistics(String identifier) {
        id = identifier;
        startupTime = (long) 0;
        componentStatistics = new HashMap<String, ComponentStatistics>();
        transferThroughputHistory = new HashMap<String, List<Integer>>();
        emitThroughputHistory = new HashMap<String, List<Integer>>();
        executeThroughputHistory = new HashMap<String, List<Integer>>();
    }

    public Long getStartupTime() {
        return startupTime;
    }

    public void setStartupTime(Long startupTime) {
        this.startupTime = startupTime;
    }

    public void clearComponentStatistics() {
        componentStatistics.clear();
    }

    public HashMap<String, ComponentStatistics> getComponentStatistics() {
        return componentStatistics;
    }

    public HashMap<String, List<Integer>> getTransferThroughputHistory() {
        return transferThroughputHistory;
    }

    public HashMap<String, List<Integer>> getEmitThroughputHistory() {
        return emitThroughputHistory;
    }

    public HashMap<String, List<Integer>> getExecuteThroughputHistory() {
        return executeThroughputHistory;
    }

    public void addComponentStatistics(String componentId, ComponentStatistics statistics) {
        componentStatistics.put(componentId, statistics);
    }
}
