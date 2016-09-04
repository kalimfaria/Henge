package backtype.storm.scheduler.advancedstela.slo;

import backtype.storm.Config;
import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;


public class Topologies {
    private static final Integer UP_TIME = 60 * 15 ; // 60 * 10
    private static final Integer REBALANCING_INTERVAL = 60 * 10;//30 previously

    private Map config;
    private NimbusClient nimbusClient;
    private HashMap<String, Topology> stelaTopologies;
    private HashMap<String, Long> topologiesUptime;
    private HashMap<String, Long> lastRebalancedAt;
    private File flatline_log;
    private File same_top;
    private int numHosts;

    public Topologies(Map conf) {
        config = conf;
        stelaTopologies = new HashMap<String, Topology>();
        topologiesUptime = new HashMap<String, Long>();
        lastRebalancedAt = new HashMap<String, Long>();
        flatline_log = new File("/tmp/flat_line.log");
        same_top = new File("/tmp/same_top.log");
    }

    public HashMap<String, Topology> getStelaTopologies() {
        return stelaTopologies;
    }

    public TopologyPairs getTopologyPairScaling() { // when trying to add topologies to either of these

        writeToFile(same_top, "In topologies:  getTopologyPairScaling\n");

        // when clearing topology SLO, mark the time
        // when adding topologies back, I can check if that old time is greater than that time + the amount I want to stagger it for
        ArrayList<Topology> failingTopologies = new ArrayList<Topology>();

        ArrayList<Topology> successfulTopologies = new ArrayList<Topology>();

        for (Topology topology : stelaTopologies.values()) {
            long lastRebalancedAtTime = 0;
            if (lastRebalancedAt.containsKey(topology.getId()))
                lastRebalancedAtTime = lastRebalancedAt.get(topology.getId());


            if ((System.currentTimeMillis() / 1000 >= lastRebalancedAtTime + REBALANCING_INTERVAL) && upForMoreThan(topology.getId())) {
                writeToFile(same_top, "The topology can be successful or failed \n");
                writeToFile(same_top, "Topology name: " + topology.getId() + "\n");
                boolean violated = topology.sloViolated();

                if (violated) {
                    writeToFile(same_top, topology.getId() + " violated the SLO \n");
                    failingTopologies.add(topology);
                } else if (!violated) {
                    writeToFile(same_top, topology.getId() + " did not violate the SLO \n");
                    successfulTopologies.add(topology);
                }
            }
        }

        writeToFile(same_top, "Failing Topologies: \n");
        for (Topology t : failingTopologies)
            writeToFile(same_top, "Failing : " + t.getId() + "\n");
        writeToFile(same_top, "Successful Topologies: \n");
        for (Topology t : successfulTopologies)
            writeToFile(same_top, "Successful : " + t.getId() + "\n");

        TopologyPairs topologyPair = new TopologyPairs();
        topologyPair.setReceivers(failingTopologies);
        topologyPair.setGivers(successfulTopologies);

        writeToFile(same_top, "Checking after topologies are set into the variables\n");
        writeToFile(same_top, "Givers:\n");
        for (String t : topologyPair.getGivers())
            writeToFile(same_top, "topology: " + t + "\n");
        writeToFile(same_top, "Receivers:\n");
        for (String t : topologyPair.getReceivers())
            writeToFile(same_top, "topology: " + t + "\n");

        return topologyPair;
    }

    public void updateLastRebalancedTime(String topologyId, Long time) {
        lastRebalancedAt.put(topologyId, time);
    }

    public void constructTopologyGraphs() {
        if (config != null) {
            try {
                nimbusClient = new NimbusClient(config, (String) config.get(Config.NIMBUS_HOST));

                List<TopologySummary> topologies = nimbusClient.getClient().getClusterInfo().get_topologies();
                StringBuffer log = new StringBuffer();
                log.append("TOPOLOGY INFO \n");
                for (TopologySummary topologySummary : topologies) {

                    log.append(topologySummary.get_id() + "\n");
                    log.append("Topology Uptime: " + nimbusClient.getClient().getTopologyInfo(topologySummary.get_id()).get_uptime_secs() + "\n");
                    log.append("Topology Status: " + nimbusClient.getClient().getTopologyInfo(topologySummary.get_id()).get_status() + "\n");
                    log.append("Topology Sched Status: " + nimbusClient.getClient().getTopologyInfo(topologySummary.get_id()).get_sched_status() + "\n");
                    log.append("Topology Num Workers: " + topologySummary.get_num_workers() + "\n");

                    String id = topologySummary.get_id();
                    StormTopology stormTopology = nimbusClient.getClient().getTopology(id);

                    if (!topologiesUptime.containsKey(id)) {
                        topologiesUptime.put(id, System.currentTimeMillis());
                    }

                    if (!stelaTopologies.containsKey(id) /*&& upForMoreThan(id)*/) {
                        //String sortingStrategy = "Unified"; // other - Class-Based
                        Double userSpecifiedSlo = getUserSpecifiedSLOFromConfig(id);
                        Double userLatencySLO = getUserSpecifiedLatencySLOFromConfig(id);
                        String sensitivity = getUserSLOSensitivityFromConfig(id);
                        Long numWorkers = getNumWorkersFromConfig(id);
                        Topology topology = new Topology(id, userSpecifiedSlo, userLatencySLO, sensitivity, numWorkers);

                        addSpoutsAndBolts(stormTopology, topology);
                        constructTopologyGraph(stormTopology, topology);

                        stelaTopologies.put(id, topology);

                    } else if (stelaTopologies.containsKey(id)) {
                        TopologyInfo topologyInfo = nimbusClient.getClient().getTopologyInfo(id);
                        updateParallelismHintsForTopology(topologyInfo, id, stormTopology);
                        updateNumWorkersForTopology(topologySummary, id);
                    }
                }

                getLatencies();

                log.append("********* END CLUSTER INFO **********\n");
                writeToFile(flatline_log, log.toString());

            } catch (NotAliveException e) {
                e.printStackTrace();
            } catch (TTransportException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            } catch (TException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean upForMoreThan(String id) {
        Long time = System.currentTimeMillis();
        Long topologyUpAt = topologiesUptime.get(id);
        return true; // TEMP ONLY
  //      return ((time - topologyUpAt) / 1000) > UP_TIME;
    }

    private Double getUserSpecifiedSLOFromConfig(String id) {
        Double topologySLO = 1.0;
        JSONParser parser = new JSONParser();
        try {
            Map conf = (Map) parser.parse(nimbusClient.getClient().getTopologyConf(id));

            topologySLO = (Double) conf.get(Config.TOPOLOGY_SLO);
            writeToFile(same_top, "In the function: getUserSpecifiedSLOFromConfig\n");
            writeToFile(same_top, "Topology name: " + id + "\n");
            writeToFile(same_top, "Topology SLO: " + topologySLO + "\n");
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        } catch (NotAliveException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }

        /*DEBUG*/
        return (topologySLO == null ? 1.0 : topologySLO);
//        return topologySLO;
    }

    private String getUserSLOSensitivityFromConfig(String id) {
        String sensitivity = new String();
        JSONParser parser = new JSONParser();
        try {
            Map conf = (Map) parser.parse(nimbusClient.getClient().getTopologyConf(id));
            sensitivity = (String) conf.get(Config.TOPOLOGY_SENSITIVITY);
            writeToFile(same_top, "In the function:  getUserSLOSensitivityFromConfig\n");
            writeToFile(same_top, "Topology name: " + id + "\n");
            writeToFile(same_top, "Topology Sensitivity: " + sensitivity + "\n");
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        } catch (NotAliveException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
        return sensitivity;
    }

    private Long getNumWorkersFromConfig(String id) {
        Long workers = 0L;
        JSONParser parser = new JSONParser();
        try {
            Map conf = (Map) parser.parse(nimbusClient.getClient().getTopologyConf(id));
            workers = (Long) conf.get(Config.TOPOLOGY_WORKERS);
            writeToFile(same_top, "In the function:  getNumWorkersFromConfig\n");
            writeToFile(same_top, "Topology name: " + id + "\n");
            writeToFile(same_top, "Topology workers: " + workers + "\n");
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        } catch (NotAliveException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
        return workers;
    }


    private Double getUserSpecifiedLatencySLOFromConfig(String id) {
        Double topologyLatencySLO = 1.0;
        JSONParser parser = new JSONParser();
        try {
            Map conf = (Map) parser.parse(nimbusClient.getClient().getTopologyConf(id));

            topologyLatencySLO = (Double) conf.get(Config.TOPOLOGY_LATENCY_SLO);
            writeToFile(same_top, "In the function: getUserSpecifiedLatencySLOFromConfig\n");
            writeToFile(same_top, "Topology name: " + id + "\n");
            writeToFile(same_top, "Topology Latency SLO: " + topologyLatencySLO + "\n");
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        } catch (NotAliveException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
        //   return topologyLatencySLO;
        return (topologyLatencySLO == null ? 50.0 : topologyLatencySLO);
    }

    private void addSpoutsAndBolts(StormTopology stormTopology, Topology topology) {
        for (Map.Entry<String, SpoutSpec> spout : stormTopology.get_spouts().entrySet()) {
            if (!spout.getKey().matches("(__).*")) {
                topology.addSpout(spout.getKey(), new Component(spout.getKey(),
                        spout.getValue().get_common().get_parallelism_hint()));
            }
        }

        for (Map.Entry<String, Bolt> bolt : stormTopology.get_bolts().entrySet()) {
            if (!bolt.getKey().matches("(__).*")) {
                topology.addBolt(bolt.getKey(), new Component(bolt.getKey(),
                        bolt.getValue().get_common().get_parallelism_hint()));
            }
        }
    }

    private void constructTopologyGraph(StormTopology topology, Topology stelaTopology) {
        for (Map.Entry<String, Bolt> bolt : topology.get_bolts().entrySet()) {
            if (!bolt.getKey().matches("(__).*")) {
                Component component = stelaTopology.getBolts().get(bolt.getKey());

                for (Map.Entry<GlobalStreamId, Grouping> parent : bolt.getValue().get_common().get_inputs().entrySet()) {
                    String parentId = parent.getKey().get_componentId();

                    if (stelaTopology.getBolts().get(parentId) == null) {
                        stelaTopology.getSpouts().get(parentId).addChild(component.getId());
                    } else {
                        stelaTopology.getBolts().get(parentId).addChild(component.getId());
                    }

                    component.addParent(parentId);
                }
            }
        }
    }

    private void updateParallelismHintsForTopology(TopologyInfo topologyInfo, String topologyId, StormTopology stormTopology) {

        HashMap<String, Integer> parallelism_hints = new HashMap<>();
        List<ExecutorSummary> execSummary = topologyInfo.get_executors();
        for (int i = 0; i < execSummary.size(); i++) {
            if (parallelism_hints.containsKey(execSummary.get(i).get_component_id()))
                parallelism_hints.put(execSummary.get(i).get_component_id(), parallelism_hints.get(execSummary.get(i).get_component_id()) + 1);
            else
                parallelism_hints.put(execSummary.get(i).get_component_id(), 1);
        }

        Topology topology = stelaTopologies.get(topologyId);
        HashMap<String, Component> allComponents = topology.getAllComponents();

        if (topologyInfo.get_executors().size() == 0) {
            for (Map.Entry<String, SpoutSpec> spout : stormTopology.get_spouts().entrySet()) {
                if (allComponents.containsKey(spout.getKey())) {
                    allComponents.get(spout.getKey()).updateParallelism(spout.getValue().get_common().
                            get_parallelism_hint());
                }
            }

            for (Map.Entry<String, Bolt> bolt : stormTopology.get_bolts().entrySet()) {
                if (allComponents.containsKey(bolt.getKey())) {
                    allComponents.get(bolt.getKey()).updateParallelism(bolt.getValue().get_common().get_parallelism_hint());
                }
            }
        } else {
            for (Map.Entry<String, SpoutSpec> spout : stormTopology.get_spouts().entrySet()) {
                if (allComponents.containsKey(spout.getKey())) {
                    allComponents.get(spout.getKey()).updateParallelism(parallelism_hints.get(spout.getKey()));
                }
            }

            for (Map.Entry<String, Bolt> bolt : stormTopology.get_bolts().entrySet()) {
                if (allComponents.containsKey(bolt.getKey())) {
                    allComponents.get(bolt.getKey()).updateParallelism(parallelism_hints.get(bolt.getKey()));
                }
            }
        }
    }


    private void updateNumWorkersForTopology(TopologySummary topologySummary, String topologyId) {
        Long numWorkers = new Long(topologySummary.get_num_workers());
        stelaTopologies.get(topologyId).setWorkers(numWorkers);
    }


    public void remove(String topologyId) {
        stelaTopologies.remove(topologyId);
    }

    public void getLatencies() {


        HashMap<String, HashMap<HashMap<String, String>, ArrayList<Double>>> data = ReadFiles();

        //    System.out.println("In function: getLatencies()");
        Double average_latency = 0.0, tail_latency = Double.MIN_VALUE; // measure
        int k = 0;
        for (String topology : stelaTopologies.keySet()) {
            Topology topology_ = stelaTopologies.get(topology);
            if (data.containsKey(topology)) {
                HashMap<HashMap<String, String>, ArrayList<Double>> top_data = data.get(topology);
                for (HashMap<String, String> op_pairs : top_data.keySet()) {
                    //              System.out.println("In getLatencies:");

                    Set sources = op_pairs.keySet();
                    ArrayList<Double> times = top_data.get(op_pairs);

        /*            Iterator it = sources.iterator();
                    while (it.hasNext()) {
                        String spout_ = (String)it.next();

        //                System.out.println("Source: " + spout_ );//+ " Sink: " + pairs.get(it.next())
        //                System.out.println("Sink: " + op_pairs.get(spout_) );
                    }

                    for (Double t : times)
                    {
                        System.out.println("Time: " + t);//+ " Sink: " + pairs.get(it.next())
                    }
        */
                    //    SummaryStatistics latencies = new SummaryStatistics();
                    Double final_time = 0.0;
                    for (int i = 0; i < times.size(); i++) {

                        k++;
                        final_time += times.get(i);
                        //        latencies.addValue(times.get(i));
                        if (tail_latency < times.get(i)) tail_latency = times.get(i);
                    }

                    //  double standardDeviation = latencies.getStandardDeviation();
                    // double mean = latencies.getMean();

                    //latencies.clear();

                   /* for (int i = 0; i < times.size(); i++) {

                        if ((times.get(i) - mean) <= 2 * standardDeviation)
                            latencies.addValue(times.get(i));
                    }*/

                    topology_.latencies.put(op_pairs, final_time / times.size()); //latencies.getMean()
                    average_latency += final_time;//latencies.getSum(); //


                    //       System.out.println("Average Latency: " + average_latency);
                    //       System.out.println("k: " + k);
                }
                //   System.out.println("Set value of average latency: " + average_latency / (double) k);
                topology_.setAverageLatency(average_latency / (double) k);

                topology_.setTailLatency(tail_latency);

                average_latency = 0.0;
                tail_latency = Double.MIN_VALUE;
                k = 0;
            } else {
                writeToFile(same_top, "No latency data for topology: " + topology + " \n");
            }
        }

        writeToFile(same_top, "Let's get some latency data \n");

        for (String topology : stelaTopologies.keySet()) {
            HashMap<HashMap<String, String>, Double> top_data = stelaTopologies.get(topology).latencies;
            for (HashMap<String, String> op_pairs : top_data.keySet()) {
                Double latency = top_data.get(op_pairs);
                for (String spouts : op_pairs.keySet()) {
                    writeToFile(same_top, "Topology: " + topology + " spout: " + spouts + " sinks: " + op_pairs.get(spouts) + " latency: " + latency + ". \n");
                }

                writeToFile(same_top, "Topology: " + topology + " average latency: " + stelaTopologies.get(topology).getAverageLatency() + " tail latency: " + stelaTopologies.get(topology).getTailLatency() + ". \n");
            }
        }
    }


    public HashMap<String, HashMap<HashMap<String, String>, ArrayList<Double>>> ReadFiles() {
        String topology = new String();
        String spout = new String();
        String sink = new String();
        Double latency = 0.0;

        HashMap<HashMap<String, String>, ArrayList<Double>> op_latency = new HashMap<HashMap<String, String>, ArrayList<Double>>();
        HashMap<String, String> op_temp = new HashMap<String, String>();
        HashMap<String, HashMap<HashMap<String, String>, ArrayList<Double>>> top_op_latency = new HashMap<String, HashMap<HashMap<String, String>, ArrayList<Double>>>();

        //   System.out.println("In function: ReadFiles()");

        final File folder = new File("/users/kalim2/output/");

        try {
            for (final File file : folder.listFiles()) {
                if (!file.isDirectory()) {
                    //System.out.println(file.getName());

                    FileChannel channel = new RandomAccessFile(file, "rw").getChannel();

                    FileLock lock = channel.tryLock();
                    while (lock == null) {
                        lock = channel.tryLock();
                    }


                    BufferedReader br = null;
                    String line = "";
                    String cvsSplitBy = ",";

                    try {

                        br = new BufferedReader(new FileReader(file));
                        while ((line = br.readLine()) != null) {
                           // System.out.println(line);
                            // use comma as separator
                            String[] split_line = line.split(cvsSplitBy);

                            topology = split_line[0];
                            spout = split_line[1];
                            sink = split_line[2];
                            latency = Double.parseDouble(split_line[3]);

                            if (top_op_latency.containsKey(topology)) {
                                op_latency = top_op_latency.get(topology);
                                boolean flag = false;
                                for (HashMap<String, String> ops : op_latency.keySet()) {
                                    if (ops.containsKey(spout) && ops.get(spout).equals(sink)) {
                                        ArrayList<Double> times = op_latency.get(ops);
                                        times.add(latency);
                                        flag = true;
                                        op_latency.put(ops, times); // PUT IT BACK
                                        top_op_latency.put(topology, op_latency);
                                    }
                                }
                                if (!flag) {
                                    op_temp = new HashMap<>();
                                    op_temp.put(spout, sink);
                                    ArrayList<Double> t = new ArrayList<Double>();
                                    t.add(latency);
                                    op_latency.put(op_temp, t); // PUT IT BACK
                                    top_op_latency.put(topology, op_latency);
                                }
                            } else {
                                op_latency = new HashMap<HashMap<String, String>, ArrayList<Double>>();
                                op_temp = new HashMap<>();
                                op_temp.put(spout, sink);
                                ArrayList<Double> t = new ArrayList<Double>();
                                t.add(latency);
                                op_latency.put(op_temp, t);
                                top_op_latency.put(topology, op_latency);
                            }
                        }
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        if (br != null) {
                            try {
                                br.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }

                    lock.release();
                    channel.close();

                }
            }

        } catch (Exception ex) {
            System.out.println(ex.toString());
        }

    /*    System.out.println("Reading the file :D");
        for (String ID : top_op_latency.keySet()) {
            System.out.println("ID: " + ID);
            HashMap<HashMap<String, String>, ArrayList<Double>> something = top_op_latency.get(ID);
            for (Map.Entry some : something.entrySet()) {
                ArrayList<Double> times = (ArrayList<Double>) some.getValue();
                HashMap<String, String> pairs = (HashMap<String, String>) some.getKey();
                Set<String> sources = pairs.keySet();
                Iterator it = sources.iterator();
                while (it.hasNext()) {
                    String spout_ = (String) it.next();

                    System.out.println("Source: " + spout_);//+ " Sink: " + pairs.get(it.next())
                    System.out.println("Sink: " + pairs.get(spout_));
                }
                for (Double x : times) {
                    System.out.println("Time value: " + x);
                }
            }
        }*/
        return top_op_latency;
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