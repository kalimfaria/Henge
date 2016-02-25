package backtype.storm.scheduler.advancedstela.slo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class TopologyPairs {
    private ArrayList<String> givers; // the first index is for the topology ID and the second one is for the SLO-juice :D
    private ArrayList<String> receivers;
    private HashMap<String, Double> givers_temp;
    private HashMap<String, Double> receivers_temp;
    private File flatline_log;

    public TopologyPairs() {
        givers = new ArrayList <String> ();
        givers_temp = new HashMap <String, Double> ();
        receivers = new ArrayList <String> ();
        receivers_temp = new HashMap <String, Double> ();
        flatline_log = new File("/tmp/flat_line.log");
    }

    public ArrayList<String> getGivers() {
        return givers;
    }

    public void setGivers(ArrayList<Topology> giverTopologies) {
        for (Topology topology: giverTopologies) {
            givers_temp.put(topology.getId(), Math.abs(topology.getUserSpecifiedSLO() - topology.getMeasuredSLO()));
        }


        List list = new LinkedList(givers_temp.entrySet());

        Collections.sort(list, new Comparator() {
            public int compare(Object o1, Object o2) {

                System.out.println(((Comparable) ((HashMap.Entry) (o1)).getValue()).compareTo(((HashMap.Entry) (o2)).getValue()));
                return ((Comparable) ((HashMap.Entry) (o1)).getValue())
                        .compareTo(((HashMap.Entry) (o2)).getValue());
            }
        });

        StringBuffer log = new StringBuffer();
        log.append("The sorted order of givers: \n");
        for (int i = 0; i < list.size(); i++)
        {
            givers.add((String) ((HashMap.Entry)list.get(i)).getKey());
            log.append((String) ((HashMap.Entry)list.get(i)).getKey() + " " + (Double) ((HashMap.Entry)list.get(i)).getValue()+ "\n");
        }

        log.append("***");
        writeToFile(flatline_log,log.toString());
    }

    public ArrayList<String> getReceivers() {
        return receivers;
    }

    public void setReceivers(ArrayList<Topology> receiverTopologies) {
        for (Topology topology: receiverTopologies) {
            receivers_temp.put(topology.getId(), Math.abs(topology.getUserSpecifiedSLO() - topology.getMeasuredSLO()));
        }

        List list = new LinkedList(receivers_temp.entrySet());
        Collections.sort(list, new Comparator() {
            public int compare(Object o1, Object o2) {

                System.out.println(((Comparable) ((HashMap.Entry) (o1)).getValue()).compareTo(((HashMap.Entry) (o2)).getValue()));
                return ((Comparable) ((HashMap.Entry) (o1)).getValue())
                        .compareTo(((HashMap.Entry) (o2)).getValue());
            }
        });


        StringBuffer log = new StringBuffer();
        log.append("The sorted order of receivers: \n");
        for (int i = 0; i < list.size(); i++)
        {
            givers.add((String) ((HashMap.Entry)list.get(i)).getKey());
            log.append((String) ((HashMap.Entry)list.get(i)).getKey() + " " + (Double) ((HashMap.Entry)list.get(i)).getValue() + "\n");
        }

        log.append("***");
        writeToFile(flatline_log, log.toString());



        for (HashMap.Entry receiver : receivers_temp.entrySet())
        {
            receivers.add((String)receiver.getKey());
            log.append(receiver.getKey() + " " + receiver.getValue() + "\n");

        }
        log.append("***");
        writeToFile(flatline_log, log.toString());
    }

    private void writeToFile(File file, String data) {
        try {
            FileWriter fileWritter = new FileWriter(file, true);
            BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
            bufferWritter.append(data);
            bufferWritter.close();
            fileWritter.close();
        } catch (IOException ex) {
            System.out.println(ex.toString());
        }
    }

}
