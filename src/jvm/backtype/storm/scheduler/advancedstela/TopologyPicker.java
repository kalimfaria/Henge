package backtype.storm.scheduler.advancedstela;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class TopologyPicker {

    private File same_top;
    public TopologyPicker() {
        same_top = new File("/tmp/same_top.log");
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
        //topologyPair.add(givers.get(0));
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
        //topologyPair.add(givers.get(0));
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
            //LOG.info("wrote to slo file {}",  data);
        } catch (IOException ex) {
            // LOG.info("error! writing to file {}", ex);
            System.out.println(ex.toString());
        }
    }
}
