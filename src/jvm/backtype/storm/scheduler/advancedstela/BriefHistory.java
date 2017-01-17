package backtype.storm.scheduler.advancedstela;

/**
 * Created by fariakalim on 10/31/16.
 */
public class BriefHistory {

    private Long time;
    private String topology;
    private Double utility;

    public BriefHistory(String topology, Long time, Double utility) {
        this.topology = topology;
        this.time = time;
        this.utility = utility;
    }

    public Long getTime() {
        return time;
    }

    public String getTopology() {
        return topology;
    }

    public Double getUtility() {
        return utility;
    }

    @Override
    public String toString () {
        return new String("Brief History: " + time + " " + topology + " " + utility);
    }
}
