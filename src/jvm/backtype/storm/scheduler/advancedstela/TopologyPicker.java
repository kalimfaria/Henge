package backtype.storm.scheduler.advancedstela;
import backtype.storm.scheduler.advancedstela.slo.Topology;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class TopologyPicker {

    private File same_top;
    public TopologyPicker() {
        same_top = new File("/tmp/same_top.log");
    }


    public ArrayList<String> unifiedStrategy(ArrayList<Topology>receiver_topologies, ArrayList<Topology>giver_topologies)
    {
        String strategy_name = "BTBV";

        switch(strategy_name)
        {
            case "BTBV": {
                Topology.sortingStrategy = "ascending";
                Collections.sort(receiver_topologies);
                Topology.sortingStrategy = "ascending";
                Collections.sort(giver_topologies);
            }
            case "BTWV": {
                Topology.sortingStrategy = "ascending";
                Collections.sort(receiver_topologies);
                Topology.sortingStrategy = "descending";
                Collections.sort(giver_topologies);
            }
            case "WTBV": {
                Topology.sortingStrategy = "descending";
                Collections.sort(receiver_topologies);
                Topology.sortingStrategy = "ascending";
                Collections.sort(giver_topologies);
            }
            case "WTWV": {
                Topology.sortingStrategy = "descending";
                Collections.sort(receiver_topologies);
                Topology.sortingStrategy = "descending";
                Collections.sort(giver_topologies);
            }

        }
        ArrayList<String> topologies = new ArrayList<>();
        topologies.add(receiver_topologies.get(0).getId());
        topologies.add(giver_topologies.get(0).getId());
        return topologies;
    }

    public ArrayList<String> classBasedStrategy(ArrayList<Topology>receiver_topologies, ArrayList<Topology>giver_topologies)
    {
        String strategy_name = "BTBV";

        switch(strategy_name)
        {
            case "BTBV": {
                Topology.sortingStrategy = "latency-ascending";
                Collections.sort(receiver_topologies);
                Topology.sortingStrategy = "throughput-ascending";
                Collections.sort(giver_topologies);
            }
            case "BTWV": {
                Topology.sortingStrategy = "latency-ascending";
                Collections.sort(receiver_topologies);
                Topology.sortingStrategy = "throughput-descending";
                Collections.sort(giver_topologies);
            }
            case "WTBV": {
                Topology.sortingStrategy = "latency-descending";
                Collections.sort(receiver_topologies);
                Topology.sortingStrategy = "throughput-ascending";
                Collections.sort(giver_topologies);
            }
            case "WTWV": {
                Topology.sortingStrategy = "latency-descending";
                Collections.sort(receiver_topologies);
                Topology.sortingStrategy = "throughput-descending";
                Collections.sort(giver_topologies);
            }
        }

     /*   writeToFile(same_top, "In class based strategy : BTBV\nReceiver Topologies:\n");
        for (Topology t : receiver_topologies)
            writeToFile(same_top, "Topology: " + t.getId() + "," + t.getSensitivity() + "," + t.getAverageLatency() + "," + t.getMeasuredSLO() + "\n");

        writeToFile(same_top, "Giver Topologies:\n");
        for (Topology t : giver_topologies)
            writeToFile(same_top, "Topology: " + t.getId() + "," + t.getSensitivity() + "," + t.getAverageLatency() + "," + t.getMeasuredSLO() + "\n");
*/
        ArrayList<String> topologies = new ArrayList<>();
        topologies.add(receiver_topologies.get(0).getId());
        topologies.add(giver_topologies.get(0).getId());
        return topologies;
    }


    public ArrayList<String> bestTargetWorstVictim (ArrayList<String> receivers, ArrayList<String>  givers)

    {

        writeToFile(same_top, "worstTargetBestVictim: \n");
        writeToFile(same_top, "Receivers: \n");
        for (String name: receivers)
        {
            writeToFile(same_top, name + "\n");
        }

        writeToFile(same_top, "Givers: \n");
        for (String name: givers)
        {
            writeToFile(same_top, name + "\n");
        }
        ArrayList<String> topologyPair = new ArrayList<>();
        topologyPair.add(receivers.get(0));
        //topologyPair.add(givers.get(givers.size() - 1));
        topologyPair.add(givers.get(0));
        return topologyPair;
    }

    public ArrayList<String> bestTargetBestVictim (ArrayList<String>  receivers, ArrayList<String>  givers)

    {
        writeToFile(same_top, "worstTargetBestVictim: \n");
        writeToFile(same_top, "Receivers: \n");
        for (String name: receivers)
        {
            writeToFile(same_top, name + "\n");
        }

        writeToFile(same_top, "Givers: \n");
        for (String name: givers)
        {
            writeToFile(same_top, name + "\n");
        }

        ArrayList<String> topologyPair = new ArrayList<>();
        topologyPair.add(receivers.get(0));
        topologyPair.add(givers.get(givers.size() - 1));
        return topologyPair;
    }

    public ArrayList<String> worstTargetBestVictim (ArrayList<String>  receivers, ArrayList<String>  givers)
    {
        writeToFile(same_top, "worstTargetBestVictim: \n");
        writeToFile(same_top, "Receivers: \n");
        for (String name: receivers)
        {
            writeToFile(same_top, name + "\n");
        }

        writeToFile(same_top, "Givers: \n");
        for (String name: givers)
        {
            writeToFile(same_top, name + "\n");
        }
        ArrayList<String> topologyPair = new ArrayList<>();
        topologyPair.add(receivers.get(receivers.size() - 1));
        topologyPair.add(givers.get(givers.size() - 1));
        return topologyPair;

    }

    public ArrayList<String> worstTargetWorstVictim (ArrayList<String>  receivers, ArrayList<String>  givers)

    {
        writeToFile(same_top, "worstTargetBestVictim: \n");
        writeToFile(same_top, "Receivers: \n");
        for (String name: receivers)
        {
            writeToFile(same_top, name + "\n");
        }

        writeToFile(same_top, "Givers: \n");
        for (String name: givers)
        {
            writeToFile(same_top, name + "\n");
        }
        ArrayList<String> topologyPair = new ArrayList<>();
        topologyPair.add(receivers.get(receivers.size() - 1));
        topologyPair.add(givers.get(0));
        return topologyPair;

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
