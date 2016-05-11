package backtype.storm.scheduler.advancedstela.slo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

public class Topology implements Comparable<Topology> {
    private static final Integer SLO_WINDOW = 30;

    private String id;
    private String sensitivity;
    

	private Double userSpecifiedSLO;
    private Double userSpecifiedLatencySLO;
    private Queue<Double> measuredSLOs;
    private Queue<Double> measuredLatency;
    private HashMap<String, Component> spouts;
    private HashMap<String, Component> bolts;

    public HashMap<HashMap<String, String>, Double> latencies;
    private Double averageLatency;
    private Double tailLatency;
    private Long numWorkers;
    static public String sortingStrategy;


    private File same_top;

    public Topology(String topologyId, Double slo, Double latency_slo, String sensitivity, Long numWorkers) {
        id = topologyId;
        userSpecifiedSLO = slo;
        measuredSLOs = new LinkedList<Double>();
        spouts = new HashMap<String, Component>();
        bolts = new HashMap<String, Component>();
        userSpecifiedLatencySLO = latency_slo;
        measuredLatency = new LinkedList<Double>();
        this.sensitivity = sensitivity;
        latencies = new HashMap<HashMap<String, String>, Double>();
        same_top = new File("/tmp/same_top.log");
        tailLatency = Double.MAX_VALUE;
        averageLatency = 0.0;
        sortingStrategy = "ascending";
        this.numWorkers =  numWorkers;
    }

    public Long getWorkers() {
        return numWorkers;
    }

    public void setWorkers(Long numWorkers) {
        this.numWorkers = numWorkers;
    }

    public String getSensitivity() {
		return sensitivity;
	}

    public Double getTailLatency() {
        return tailLatency;
    }

    public void setTailLatency(Double latency) {
        tailLatency = latency;
    }

    public Double getAverageLatency() {
        return averageLatency;
    }

    public void setAverageLatency(Double latency) {
        averageLatency = latency;

        if (measuredLatency.size() == SLO_WINDOW) {
            measuredLatency.remove();
        }
        measuredLatency.add(latency);

    }

    public String getId() {
        return id;
    }

    public Double getUserSpecifiedSLO() {
        return userSpecifiedSLO;
    }

  /*  public void setMeasuredLatency(Double value) {
        if (measuredLatency.size() == SLO_WINDOW) {
            measuredLatency.remove();
        }
        writeToFile(same_top, "Setting topology's average complete latency (per 30 values)\n");
        writeToFile(same_top, "Topology ID: "+id +" latency: "+ value+"\n");
        measuredLatency.add(value);
    }

    public Double getMeasuredLatency() {
        double result = 0.0;
        for (Double value : measuredLatency) {
            result += value;
        }
        return measuredLatency.size() == 0 ? 0.0 : (result / measuredLatency.size());
    }*/

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

    public boolean allReadingsViolateSLO() {

        for (Double value : measuredSLOs) {
            if (value > userSpecifiedSLO)
                return false;
        }

        return true;
    }

    public boolean allLatencyReadingsViolateSLO() {

        for (Double value : measuredLatency) {
            if (value < userSpecifiedLatencySLO)
                return false;
        }

        return true;
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


  /*  public int compareTo(Topology other) {
        return getMeasuredSLO().compareTo(other.getMeasuredSLO());
    }*/

    public int compareTo(Topology other) {

        switch (sortingStrategy) {
            case "ascending":
                return slo_ascending(other);
            case "descending":
                return slo_descending(other);
            case "latency-ascending":
                return latency_slo_ascending(other);
            case "latency-descending":
                return latency_slo_descending(other);
            case "throughput-ascending":
                return throughput_slo_ascending(other);
            case "throughput-descending":
                return throughput_slo_descending(other);
            default:
                return 0;
        }

    }

    public int slo_ascending(Topology other){

        Double slo_perc = 0.0;
        if (sensitivity.equals("latency"))
        {
            slo_perc = Math.abs((userSpecifiedLatencySLO - getAverageLatency())/userSpecifiedLatencySLO);
        }
        else
        {
            slo_perc = Math.abs((userSpecifiedSLO - getMeasuredSLO())/userSpecifiedSLO);
        }

        Double other_slo_perc = 0.0;
        if (sensitivity.equals("latency"))
        {
            other_slo_perc = Math.abs((other.userSpecifiedLatencySLO - other.getAverageLatency())/other.userSpecifiedLatencySLO);
        }
        else
        {
            other_slo_perc = Math.abs((other.userSpecifiedSLO - other.getMeasuredSLO())/other.userSpecifiedSLO);
        }

        return (slo_perc.compareTo(other_slo_perc));
    }

    public int slo_descending(Topology other){

        Double slo_perc = 0.0;
        if (sensitivity.equals("latency"))
        {
            slo_perc = Math.abs((userSpecifiedLatencySLO - getAverageLatency())/userSpecifiedLatencySLO);
        }
        else
        {
            slo_perc = Math.abs((userSpecifiedSLO - getMeasuredSLO())/userSpecifiedSLO);
        }

        Double other_slo_perc = 0.0;
        if (sensitivity.equals("latency"))
        {
            other_slo_perc = Math.abs((other.userSpecifiedLatencySLO - other.getAverageLatency())/other.userSpecifiedLatencySLO);
        }
        else
        {
            other_slo_perc = Math.abs((other.userSpecifiedSLO - other.getMeasuredSLO())/other.userSpecifiedSLO);
        }

        return (other_slo_perc.compareTo(slo_perc));
    }

    public int latency_slo_ascending(Topology other){

        Integer my_sens, other_sens;
        Double slo_perc = 0.0;
        if (sensitivity.equals("latency"))
        {
            slo_perc = Math.abs((userSpecifiedLatencySLO - getAverageLatency())/userSpecifiedLatencySLO);
            my_sens = 1;
        }
        else
        {
            slo_perc = Math.abs((userSpecifiedSLO - getMeasuredSLO())/userSpecifiedSLO);
            my_sens = 2;
        }

        Double other_slo_perc = 0.0;
        if (sensitivity.equals("latency"))
        {
            other_slo_perc = Math.abs((other.userSpecifiedLatencySLO - other.getAverageLatency())/other.userSpecifiedLatencySLO);
            other_sens = 1;
        }
        else
        {
            other_slo_perc = Math.abs((other.userSpecifiedSLO - other.getMeasuredSLO())/other.userSpecifiedSLO);
            other_sens = 2;
        }
        if (my_sens.compareTo(other_sens) != 0)
            return my_sens.compareTo(other_sens);
        else
            return (slo_perc.compareTo(other_slo_perc));
    }


    public int latency_slo_descending(Topology other){ /// PUT LATENCY FIRST NO MATTER SENSITIVITY

        Integer my_sens, other_sens;
        Double slo_perc = 0.0;
        if (sensitivity.equals("latency"))
        {
            slo_perc = Math.abs((userSpecifiedLatencySLO - getAverageLatency())/userSpecifiedLatencySLO);
            my_sens = 1;
        }
        else
        {
            slo_perc = Math.abs((userSpecifiedSLO - getMeasuredSLO())/userSpecifiedSLO);
            my_sens = 2;
        }

        Double other_slo_perc = 0.0;
        if (sensitivity.equals("latency"))
        {
            other_slo_perc = Math.abs((other.userSpecifiedLatencySLO - other.getAverageLatency())/other.userSpecifiedLatencySLO);
            other_sens = 1;
        }
        else
        {
            other_slo_perc = Math.abs((other.userSpecifiedSLO - other.getMeasuredSLO())/other.userSpecifiedSLO);
            other_sens = 2;
        }
        if (my_sens.compareTo(other_sens) != 0)
            return my_sens.compareTo(other_sens);
        else
            return (other_slo_perc.compareTo(slo_perc));
    }




    public int throughput_slo_ascending(Topology other){

        Integer my_sens, other_sens;
        Double slo_perc = 0.0;
        if (sensitivity.equals("latency"))
        {
            slo_perc = Math.abs((userSpecifiedLatencySLO - getAverageLatency())/userSpecifiedLatencySLO);
            my_sens = 2;
        }
        else
        {
            slo_perc = Math.abs((userSpecifiedSLO - getMeasuredSLO())/userSpecifiedSLO);
            my_sens = 1;
        }

        Double other_slo_perc = 0.0;
        if (sensitivity.equals("latency"))
        {
            other_slo_perc = Math.abs((other.userSpecifiedLatencySLO - other.getAverageLatency())/other.userSpecifiedLatencySLO);
            other_sens = 2;
        }
        else
        {
            other_slo_perc = Math.abs((other.userSpecifiedSLO - other.getMeasuredSLO())/other.userSpecifiedSLO);
            other_sens = 1;
        }
        if (my_sens.compareTo(other_sens) != 0)
            return my_sens.compareTo(other_sens);
        else
            return (slo_perc.compareTo(other_slo_perc));
    }


    public int throughput_slo_descending(Topology other){ /// PUT LATENCY FIRST NO MATTER SENSITIVITY

        Integer my_sens, other_sens;
        Double slo_perc = 0.0;
        if (sensitivity.equals("latency"))
        {
            slo_perc = Math.abs((userSpecifiedLatencySLO - getAverageLatency())/userSpecifiedLatencySLO);
            my_sens = 2;
        }
        else
        {
            slo_perc = Math.abs((userSpecifiedSLO - getMeasuredSLO())/userSpecifiedSLO);
            my_sens = 1;
        }

        Double other_slo_perc = 0.0;
        if (sensitivity.equals("latency"))
        {
            other_slo_perc = Math.abs((other.userSpecifiedLatencySLO - other.getAverageLatency())/other.userSpecifiedLatencySLO);
            other_sens = 2;
        }
        else
        {
            other_slo_perc = Math.abs((other.userSpecifiedSLO - other.getMeasuredSLO())/other.userSpecifiedSLO);
            other_sens = 1;
        }
        if (my_sens.compareTo(other_sens) != 0)
            return my_sens.compareTo(other_sens);
        else
            return (other_slo_perc.compareTo(slo_perc));
    }






    public boolean sloViolated() {
        writeToFile(same_top, "In the function: sloViolated() \n");
        writeToFile(same_top, "Topology name: " + id + "\n");
        writeToFile(same_top, "Topology SLO: " + userSpecifiedSLO + "\n");
        writeToFile(same_top, "Topology Measured SLO: " + getMeasuredSLO() + "\n");
        writeToFile(same_top, "Topology Latency SLO: " + userSpecifiedLatencySLO + "\n");
        writeToFile(same_top, "Topology Measured Latency SLO: " + getAverageLatency() + "\n");

        if (sensitivity != null) {
            if (sensitivity.equals("throughput")) {
                //return (allReadingsViolateSLO()); //
                return getMeasuredSLO() < userSpecifiedSLO;
            }            else if (sensitivity.equals("latency")) {
                //return allLatencyReadingsViolateSLO() ;//
                return (getAverageLatency() > userSpecifiedLatencySLO); //
            }
        }
      //  return (allReadingsViolateSLO());
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

    public void writeToFile(File file, String data) {
        try {
            FileWriter fileWriter = new FileWriter(file, true);
            BufferedWriter bufferWriter = new BufferedWriter(fileWriter);
            bufferWriter.append(data);
            bufferWriter.close();
            fileWriter.close();
        } catch (IOException ex) {

            System.out.println(ex.toString());
        }
    }
}