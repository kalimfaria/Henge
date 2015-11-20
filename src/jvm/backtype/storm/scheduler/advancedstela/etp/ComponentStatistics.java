package backtype.storm.scheduler.advancedstela.etp;

public class ComponentStatistics {
    public String componentId;
    public Integer totalEmitThroughput;
    public Integer totalTransferThroughput;
    public Integer totalExecuteThroughput;

    public ComponentStatistics(String id) {
        this.componentId = id;
        this.totalEmitThroughput = 0;
        this.totalTransferThroughput = 0;
        this.totalExecuteThroughput = 0;
    }
}
