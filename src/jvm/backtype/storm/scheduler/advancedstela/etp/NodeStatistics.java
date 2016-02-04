package backtype.storm.scheduler.advancedstela.etp;

import backtype.storm.generated.ExecutorSummary;

import java.util.ArrayList;
import java.util.HashMap;

public class NodeStatistics {
    private static final String TRANSFER = "transfer";
    private static final String EMIT = "emit";

    private String id;
    private Integer transferThroughput;
    private Integer emitThroughput;
    private ArrayList<ExecutorSummary> spouts;
    private ArrayList<ExecutorSummary> bolts;
    public HashMap<String, Integer> throughputForBolts;
    public HashMap<String, Integer> throughputForSpouts;

    public NodeStatistics(String host) {
        id = host;
        transferThroughput = 0;
        emitThroughput = 0;
        spouts = new ArrayList<ExecutorSummary>();
        bolts = new ArrayList<ExecutorSummary>();

        throughputForBolts = new HashMap<String, Integer>();
        throughputForBolts.put(TRANSFER, 0);
        throughputForBolts.put(EMIT, 0);

        throughputForSpouts = new HashMap<String, Integer>();
        throughputForSpouts.put(TRANSFER, 0);
        throughputForSpouts.put(EMIT, 0);
    }

    public void addSpout(ExecutorSummary executorSummary) {
        spouts.add(executorSummary);
    }

    public void addBolt(ExecutorSummary executorSummary) {
        bolts.add(executorSummary);
    }

    public void addTransferThroughput(Integer value) {
        transferThroughput += value;
    }

    public void addEmitThroughput(Integer value) {
        emitThroughput += value;
    }
}
