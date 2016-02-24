package backtype.storm.scheduler.advancedstela.slo;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

public class Topology implements Comparable<Topology> {
    private static final Integer SLO_WINDOW = 30;

    private String id;
    private Double userSpecifiedSLO;
    private Queue<Double> measuredSLOs;
    private HashMap<String, Component> spouts;
    private HashMap<String, Component> bolts;

    public Topology(String topologyId, Double slo) {
        id = topologyId;
        userSpecifiedSLO = slo;
        measuredSLOs = new LinkedList<Double>();
        spouts = new HashMap<String, Component>();
        bolts = new HashMap<String, Component>();
    }

    public String getId() {
        return id;
    }

    public Double getUserSpecifiedSLO() {
        return userSpecifiedSLO;
    }

    public void setMeasuredSLOs(Double value) {
        if (measuredSLOs.size() == SLO_WINDOW) {
            measuredSLOs.remove();
        }
        measuredSLOs.add(value);
    }

    public Double getMeasuredSLO() {
        double result = 0.0;
        for (Double value : measuredSLOs) {
            result += value;
        }

        return measuredSLOs.size() == 0 ? 0.0 : (result / measuredSLOs.size());
    }

    public void addSpout(String id, Component component) {
        spouts.put(id, component);
    }

    public HashMap<String, Component> getSpouts() {
        return spouts;
    }

    public void addBolt(String id, Component component) {
        bolts.put(id, component);
    }

    public HashMap<String, Component> getBolts() {
        return bolts;
    }

    public HashMap<String, Component> getAllComponents() {
        HashMap<String, Component> components = new HashMap<String, Component>(

        );
        components.putAll(spouts);
        components.putAll(bolts);
        return components;
    }


    public int compareTo(Topology other) {
        return getMeasuredSLO().compareTo(other.getMeasuredSLO());
    }

    public boolean sloViolated() {
        return getMeasuredSLO() < userSpecifiedSLO;
    }

    public String printSLOs() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(" ");
        for (Double measuredSLO : measuredSLOs) {
            stringBuilder.append(measuredSLO).append(" ");
        }
        return stringBuilder.toString();
    }
}