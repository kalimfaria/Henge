import backtype.storm.scheduler.advancedstela.BriefHistory;
import backtype.storm.scheduler.advancedstela.TopologyPicker;
import backtype.storm.scheduler.advancedstela.etp.Component;
import backtype.storm.scheduler.advancedstela.etp.SupervisorInfo;
import backtype.storm.scheduler.advancedstela.etp.TopologySchedule;
import backtype.storm.scheduler.advancedstela.slo.Sensitivity;
import backtype.storm.scheduler.advancedstela.slo.Topology;
import com.google.gson.Gson;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.Before;
import org.junit.Test;

import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by fariakalim on 10/31/16.
 */
public class TestTopology {

   /* Topology t1;
    Topology t2;
    Topology t3;

    @Before
    public void setup() {
        t1 = new Topology("T1", 1.0, 0.0, 5.0, 5L); // throughput sensitive
        t2 = new Topology("T2", 0.0, 50.0, 25.0, 5L); // latency sensitive
        t3 = new Topology("T3", 1.0, 50.0, 35.0, 5L); // both
    }

    @Test
    public void testSensitivityType() {
        Sensitivity s1 = t1.getSensitivity();
        Sensitivity s2 = t2.getSensitivity();
        Sensitivity s3 = t3.getSensitivity();

        assertEquals(Sensitivity.JUICE, s1);
        assertEquals(Sensitivity.LATENCY, s2);
        assertEquals(Sensitivity.BOTH, s3);
    }

    @Test
    public void testJuiceUtilityCalculations() {
        t1.setMeasuredSLOs(0.5);
        t1.setMeasuredSLOs(1.0);
        Double measuredUtility = t1.getCurrentUtility();
        assertEquals((Double) 3.75, measuredUtility);
    }

    @Test
    public void testLatencyUtilityCalculations() {
        t2.setAverageLatency(70.0);
        Double measuredUtility = t2.getCurrentUtility();
        assertEquals((Double) 17.857142857142858, measuredUtility);
    }

    @Test
    public void testBothUtilityCalculations() {
        t3.setAverageLatency(70.0);
        t3.setMeasuredSLOs(0.5);
        t3.setMeasuredSLOs(1.0);
        Double measuredUtility = t3.getCurrentUtility();
        assertEquals((Double) (25.0 * 0.5 + 0.5 * 26.25), measuredUtility);
    }

    @Test
    public void testSloViolated() {

        t1.setMeasuredSLOs(0.5);
        t1.setMeasuredSLOs(1.0);

        assertTrue(t1.sloViolated());

        t2.setAverageLatency(70.0);
        assertTrue(t2.sloViolated());

        t3.setAverageLatency(70.0);
        t3.setMeasuredSLOs(0.5);
        t3.setMeasuredSLOs(1.0);
        assertTrue(t3.sloViolated());
    }

    @Test
    public void testWhichSensitivityToHandleFirst() {
        t3.setAverageLatency(70.0);
        t3.setMeasuredSLOs(0.5);
        t3.setMeasuredSLOs(1.0);
        assertEquals(Sensitivity.JUICE, t3.whichSLOToSatisfyFirst());
        t3.setAverageLatency(51.0);
        assertEquals(Sensitivity.LATENCY, t3.whichSLOToSatisfyFirst());
        t3.setAverageLatency(49.0);
        assertEquals(Sensitivity.JUICE, t3.whichSLOToSatisfyFirst());
        t3.setAverageLatency(60.0);
        for (int i = 0; i < 40; i++)
            t3.setMeasuredSLOs(1.0);
        assertEquals(Sensitivity.LATENCY, t3.whichSLOToSatisfyFirst());
    }

    @Test
    public void testSort() {
        ArrayList<Topology> list = new ArrayList<>();
        t1.setMeasuredSLOs(0.5);
        t1.setMeasuredSLOs(1.0);

        t2.setAverageLatency(70.0);

        t3.setAverageLatency(70.0);
        t3.setMeasuredSLOs(0.5);
        t3.setMeasuredSLOs(1.0);
        list.add(t3);
        list.add(t1);
        list.add(t2);
        Collections.sort(list);
        assertEquals(list.get(0).getId(), "T1");
        assertEquals(list.get(1).getId(), "T2");
        assertEquals(list.get(2).getId(), "T3");
        Topology.sortingStrategy = "descending-current-utility";
        Collections.sort(list);
        assertEquals(list.get(0).getId(), "T3");
        assertEquals(list.get(1).getId(), "T2");
        assertEquals(list.get(2).getId(), "T1");
    }

    @Test
    public void testPickTopologySimple() {
        ArrayList<Topology> list = new ArrayList<>();
        t1.setMeasuredSLOs(0.5);
        t1.setMeasuredSLOs(1.0);

        t2.setAverageLatency(70.0);

        t3.setAverageLatency(70.0);
        t3.setMeasuredSLOs(0.5);
        t3.setMeasuredSLOs(1.0);
        list.add(t3);
        list.add(t1);
        list.add(t2);

        TopologyPicker picker = new TopologyPicker();
        Topology topology = picker.pickTopology(list, new ArrayList<BriefHistory>());
        assertEquals(topology.getId(), "T3");

    }

    @Test
    public void testPickTopologyNotRemoveRebalanced() {
        ArrayList<Topology> list = new ArrayList<>();
        t1.setMeasuredSLOs(0.5);
        t1.setMeasuredSLOs(1.0);

        t2.setAverageLatency(70.0);

        t3.setAverageLatency(70.0);
        t3.setMeasuredSLOs(0.5);
        t3.setMeasuredSLOs(1.0);


        list.add(t3);
        list.add(t1);
        list.add(t2);

        TopologyPicker picker = new TopologyPicker();
        Topology topology = picker.pickTopology(list, new ArrayList<BriefHistory>());
        assertEquals(topology.getId(), "T3");

    }

    @Test
    public void testPickTopologyRemoveRebalanced() {
        ArrayList<Topology> list = new ArrayList<>();
        t1.setMeasuredSLOs(0.5);
        t1.setMeasuredSLOs(1.0);

        t2.setAverageLatency(70.0);

        t3.setAverageLatency(70.0);
        t3.setMeasuredSLOs(0.5);
        t3.setMeasuredSLOs(1.0);
        list.add(t3);
        list.add(t1);
        list.add(t2);

        TopologyPicker picker = new TopologyPicker();
        BriefHistory history = new BriefHistory("T3", System.currentTimeMillis(), 25.625);
        ArrayList<BriefHistory> briefHistory = new ArrayList<BriefHistory>();
        briefHistory.add(history);
        Topology topology = picker.pickTopology(list, briefHistory);
        assertEquals(topology.getId(), "T2");
    }

    @Test
    public void testPickTopologySimpleSameSLOs() {
        ArrayList<Topology> list = new ArrayList<>();
        t1.setMeasuredSLOs(0.5);
        t1.setMeasuredSLOs(1.0);

        t2.setAverageLatency(70.0);

        t3.setAverageLatency(70.0);
        t3.setMeasuredSLOs(0.5);
        t3.setMeasuredSLOs(1.0);

        Topology t4 = new Topology("T4", 1.0, 50.0, 35.0, 5L); // both
        t4.setAverageLatency(55.0);
        t4.setMeasuredSLOs(0.5);
        t4.setMeasuredSLOs(1.0);

        list.add(t3);
        list.add(t1);
        list.add(t2);
        list.add(t4);

        TopologyPicker picker = new TopologyPicker();
        Topology topology = picker.pickTopology(list, new ArrayList<BriefHistory>());
        assertEquals(topology.getId(), "T3");

        t4 = new Topology("T4", 1.0, 50.0, 35.0, 5L); // both
        t4.setAverageLatency(100.0);
        t4.setMeasuredSLOs(0.5);
        t4.setMeasuredSLOs(1.0);

        list = new ArrayList<>();
        list.add(t3);
        list.add(t1);
        list.add(t2);
        list.add(t4);

        picker = new TopologyPicker();
        topology = picker.pickTopology(list, new ArrayList<BriefHistory>());
        assertEquals(topology.getId(), "T4");
    }

    @Test
    public void testParseSupervisorHosts() {
        String response = "{\n" +
                "         \"supervisors\": [\n" +
                "         {\n" +
                "             \"id\": \"21dee14c-b2c3-4a41-a7a0-dff6fcf4fa34\",\n" +
                "                 \"host\": \"pc427.emulab.net\",\n" +
                "                 \"uptime\": \"36m 40s\",\n" +
                "                 \"slotsTotal\": 4,\n" +
                "                 \"slotsUsed\": 2,\n" +
                "                 \"version\": \"0.10.1-SNAPSHOT\"\n" +
                "         },\n" +
                "         {\n" +
                "             \"id\": \"6c741fe4-3cf8-4153-b363-0ab4d21d3b99\",\n" +
                "                 \"host\": \"pc557.emulab.net\",\n" +
                "                 \"uptime\": \"36m 37s\",\n" +
                "                 \"slotsTotal\": 4,\n" +
                "                 \"slotsUsed\": 1,\n" +
                "                 \"version\": \"0.10.1-SNAPSHOT\"\n" +
                "         },\n" +
                "         {\n" +
                "             \"id\": \"d81c5f22-ee13-4ba7-bdc7-9b39192c6666\",\n" +
                "                 \"host\": \"pc436.emulab.net\",\n" +
                "                 \"uptime\": \"36m 46s\",\n" +
                "                 \"slotsTotal\": 4,\n" +
                "                 \"slotsUsed\": 1,\n" +
                "                 \"version\": \"0.10.1-SNAPSHOT\"\n" +
                "         },\n" +
                "         {\n" +
                "             \"id\": \"7c0f6b70-8438-4387-988f-ba08bdcc4f17\",\n" +
                "                 \"host\": \"pc538.emulab.net\",\n" +
                "                 \"uptime\": \"36m 54s\",\n" +
                "                 \"slotsTotal\": 4,\n" +
                "                 \"slotsUsed\": 1,\n" +
                "                 \"version\": \"0.10.1-SNAPSHOT\"\n" +
                "         },\n" +
                "         {\n" +
                "             \"id\": \"675925af-c7b9-4e4e-8c7b-ba11f618ca97\",\n" +
                "                 \"host\": \"pc553.emulab.net\",\n" +
                "                 \"uptime\": \"36m 58s\",\n" +
                "                 \"slotsTotal\": 4,\n" +
                "                 \"slotsUsed\": 1,\n" +
                "                 \"version\": \"0.10.1-SNAPSHOT\"\n" +
                "         }\n" +
                "         ]}";
        SupervisorInfo supervisorInfo = new SupervisorInfo();
        String[] supervisors = supervisorInfo.getSupervisorHosts(response);

        assertEquals(supervisors[0], "pc427.emulab.net");
        assertEquals(supervisors[1], "pc557.emulab.net");
        assertEquals(supervisors[2], "pc436.emulab.net");
        assertEquals(supervisors[3], "pc538.emulab.net");
        assertEquals(supervisors[4], "pc553.emulab.net");
    }

    @Test
    public void testSupervisorInfos() {
        String response = "{\"recentLoad\": \"1.2\"," +
                "         \"minLoad\": \"1.2\"," +
                "         \"fiveMinsLoad\": \"1.4\"," +
                "         \"freeMem\": \"4.0\"," +
                "         \"usedMemory\":\"4.0\",\n" +
                "         \"usedMemPercent\": \"4.0\"," +
                "         \"time\": \"12312434434\"}";
        Gson gson = new Gson();
        SupervisorInfo.Info info = gson.fromJson(response.toString(), SupervisorInfo.Info.class);
        assertEquals(info.fiveMinsLoad, new Double(1.4));
        assertEquals(info.freeMem, new Double(4.0));
        assertEquals(info.minLoad, new Double(1.2));
        assertEquals(info.recentLoad, new Double(1.2));
        assertEquals(info.time, new Long(12312434434L));
        assertEquals(info.usedMemory, new Double(4.0));
        assertEquals(info.usedMemPercent, new Double(4.0));
    }

    @Test
    public void testSupervisorOverUtilizationQuorum() {
        SupervisorInfo supervisorInfo = new SupervisorInfo();
        supervisorInfo.supervisors = new String[3];
        HashMap<String, SupervisorInfo.Info> infos = new HashMap<>();

        String response = "{\"recentLoad\": \"4.0\"," +
                "         \"minLoad\": \"1.2\"," +
                "         \"fiveMinsLoad\": \"1.4\"," +
                "         \"freeMem\": \"4.0\"," +
                "         \"usedMemory\":\"4.0\",\n" +
                "         \"usedMemPercent\": \"4.0\"," +
                "         \"time\": \"12312434434\"}";
        Gson gson = new Gson();

        SupervisorInfo.Info info = gson.fromJson(response.toString(), SupervisorInfo.Info.class);
        String[] supervisors = {"pc427.emulab.net", "pc553.emulab.net", "pc538.emulab.net"};
        for (int i = 0; i < supervisorInfo.HISTORY_SIZE; i++) {
            int j = 0;
            for (String supervisor : supervisors) {
                if (j == 0 || j == 1) info.recentLoad = supervisorInfo.MAXIMUM_LOAD_PER_MACHINE;
                else info.recentLoad = 0.0;
                infos.put(supervisor, info);
                info = gson.fromJson(response.toString(), SupervisorInfo.Info.class);
                j++;
            }
            supervisorInfo.insertInfo(infos);
        }

        assertEquals(true, supervisorInfo.areSupervisorsOverUtilizedQuorum());
    }

    @Test
    public void testSupervisorOverUtilization() {
        SupervisorInfo supervisorInfo = new SupervisorInfo();
        HashMap<String, SupervisorInfo.Info> infos = new HashMap<>();

        String response = "{\"recentLoad\": \"4.0\"," +
                "         \"minLoad\": \"1.2\"," +
                "         \"fiveMinsLoad\": \"1.4\"," +
                "         \"freeMem\": \"4.0\"," +
                "         \"usedMemory\":\"4.0\",\n" +
                "         \"usedMemPercent\": \"4.0\"," +
                "         \"time\": \"12312434434\"}";
        Gson gson = new Gson();
        SupervisorInfo.Info info = gson.fromJson(response.toString(), SupervisorInfo.Info.class);
        String[] supervisors = {"pc427.emulab.net", "pc553.emulab.net", "pc538.emulab.net"};
        for (int i = 0; i < supervisorInfo.HISTORY_SIZE; i++) {
            int j = 0;
            for (String supervisor : supervisors) {
                if (j == 0) info.recentLoad = supervisorInfo.MAXIMUM_LOAD_PER_MACHINE;
                else info.recentLoad = 0.0;
                infos.put(supervisor, info);
                info = gson.fromJson(response.toString(), SupervisorInfo.Info.class);
                j++;
            }
            supervisorInfo.insertInfo(infos);
        }

        assertEquals(true, supervisorInfo.areSupervisorsOverUtilized());
    }

    @Test
    public void testTopologyCapacityFormat() {
        String response = " {\n" +
                " \t\"name\": \"WordCount3\",\n" +
                " \t\"id\": \"WordCount3-1-1402960825\",\n" +
                " \t\"workersTotal\": 3,\n" +
                " \t\"window\": \"600\",\n" +
                " \t\"status\": \"ACTIVE\",\n" +
                " \t\"tasksTotal\": 28,\n" +
                " \t\"executorsTotal\": 28,\n" +
                " \t\"uptime\": \"29m 19s\",\n" +
                " \t\"msgTimeout\": 30,\n" +
                " \t\"windowHint\": \"10m 0s\",\n" +
                " \t\"topologyStats\": [{\n" +
                " \t\t\"windowPretty\": \"10m 0s\",\n" +
                " \t\t\"window\": \"600\",\n" +
                " \t\t\"emitted\": 397960,\n" +
                " \t\t\"transferred\": 213380,\n" +
                " \t\t\"completeLatency\": \"0.000\",\n" +
                " \t\t\"acked\": 213460,\n" +
                " \t\t\"failed\": 0\n" +
                " \t}, {\n" +
                " \t\t\"windowPretty\": \"3h 0m 0s\",\n" +
                " \t\t\"window\": \"10800\",\n" +
                " \t\t\"emitted\": 1190260,\n" +
                " \t\t\"transferred\": 638260,\n" +
                " \t\t\"completeLatency\": \"0.000\",\n" +
                " \t\t\"acked\": 638280,\n" +
                " \t\t\"failed\": 0\n" +
                " \t}, {\n" +
                " \t\t\"windowPretty\": \"1d 0h 0m 0s\",\n" +
                " \t\t\"window\": \"86400\",\n" +
                " \t\t\"emitted\": 1190260,\n" +
                " \t\t\"transferred\": 638260,\n" +
                " \t\t\"completeLatency\": \"0.000\",\n" +
                " \t\t\"acked\": 638280,\n" +
                " \t\t\"failed\": 0\n" +
                " \t}, {\n" +
                " \t\t\"windowPretty\": \"All time\",\n" +
                " \t\t\"window\": \":all-time\",\n" +
                " \t\t\"emitted\": 1190260,\n" +
                " \t\t\"transferred\": 638260,\n" +
                " \t\t\"completeLatency\": \"0.000\",\n" +
                " \t\t\"acked\": 638280,\n" +
                " \t\t\"failed\": 0\n" +
                " \t}],\n" +
                " \t\"spouts\": [{\n" +
                " \t\t\"executors\": 5,\n" +
                " \t\t\"emitted\": 28880,\n" +
                " \t\t\"completeLatency\": \"0.000\",\n" +
                " \t\t\"transferred\": 28880,\n" +
                " \t\t\"acked\": 0,\n" +
                " \t\t\"spoutId\": \"spout\",\n" +
                " \t\t\"tasks\": 5,\n" +
                " \t\t\"lastError\": \"\",\n" +
                " \t\t\"errorLapsedSecs\": null,\n" +
                " \t\t\"failed\": 0\n" +
                " \t}],\n" +
                " \t\"bolts\": [{\n" +
                " \t\t\"executors\": 12,\n" +
                " \t\t\"emitted\": 184580,\n" +
                " \t\t\"transferred\": 0,\n" +
                " \t\t\"acked\": 184640,\n" +
                " \t\t\"executeLatency\": \"0.048\",\n" +
                " \t\t\"tasks\": 12,\n" +
                " \t\t\"executed\": 184620,\n" +
                " \t\t\"processLatency\": \"0.043\",\n" +
                " \t\t\"boltId\": \"count\",\n" +
                " \t\t\"lastError\": \"\",\n" +
                " \t\t\"errorLapsedSecs\": null,\n" +
                " \t\t\"capacity\": \"0.003\",\n" +
                " \t\t\"failed\": 0\n" +
                " \t}, {\n" +
                " \t\t\"executors\": 8,\n" +
                " \t\t\"emitted\": 184500,\n" +
                " \t\t\"transferred\": 184500,\n" +
                " \t\t\"acked\": 28820,\n" +
                " \t\t\"executeLatency\": \"0.024\",\n" +
                " \t\t\"tasks\": 8,\n" +
                " \t\t\"executed\": 28780,\n" +
                " \t\t\"processLatency\": \"2.112\",\n" +
                " \t\t\"boltId\": \"split\",\n" +
                " \t\t\"lastError\": \"\",\n" +
                " \t\t\"errorLapsedSecs\": null,\n" +
                " \t\t\"capacity\": \"0.000\",\n" +
                " \t\t\"failed\": 0\n" +
                " \t}],\n" +
                " \t\"configuration\": {\n" +
                " \t\t\"storm.id\": \"WordCount3-1-1402960825\",\n" +
                " \t\t\"dev.zookeeper.path\": \"/tmp/dev-storm-zookeeper\",\n" +
                " \t\t\"topology.tick.tuple.freq.secs\": null,\n" +
                " \t\t\"topology.builtin.metrics.bucket.size.secs\": 60,\n" +
                " \t\t\"topology.fall.back.on.java.serialization\": true,\n" +
                " \t\t\"topology.max.error.report.per.interval\": 5,\n" +
                " \t\t\"zmq.linger.millis\": 5000,\n" +
                " \t\t\"topology.skip.missing.kryo.registrations\": false,\n" +
                " \t\t\"storm.messaging.netty.client_worker_threads\": 1,\n" +
                " \t\t\"ui.childopts\": \"-Xmx768m\",\n" +
                " \t\t\"storm.zookeeper.session.timeout\": 20000,\n" +
                " \t\t\"nimbus.reassign\": true,\n" +
                " \t\t\"topology.trident.batch.emit.interval.millis\": 500,\n" +
                " \t\t\"storm.messaging.netty.flush.check.interval.ms\": 10,\n" +
                " \t\t\"nimbus.monitor.freq.secs\": 10,\n" +
                " \t\t\"logviewer.childopts\": \"-Xmx128m\",\n" +
                " \t\t\"java.library.path\": \"/usr/local/lib:/opt/local/lib:/usr/lib\",\n" +
                " \t\t\"topology.executor.send.buffer.size\": 1024,\n" +
                " \t\t\"storm.local.dir\": \"storm-local\",\n" +
                " \t\t\"storm.messaging.netty.buffer_size\": 5242880,\n" +
                " \t\t\"supervisor.worker.start.timeout.secs\": 120,\n" +
                " \t\t\"topology.enable.message.timeouts\": true,\n" +
                " \t\t\"nimbus.cleanup.inbox.freq.secs\": 600,\n" +
                " \t\t\"nimbus.inbox.jar.expiration.secs\": 3600,\n" +
                " \t\t\"drpc.worker.threads\": 64,\n" +
                " \t\t\"topology.worker.shared.thread.pool.size\": 4,\n" +
                " \t\t\"nimbus.host\": \"hw10843.local\",\n" +
                " \t\t\"storm.messaging.netty.min_wait_ms\": 100,\n" +
                " \t\t\"storm.zookeeper.port\": 2181,\n" +
                " \t\t\"transactional.zookeeper.port\": null,\n" +
                " \t\t\"topology.executor.receive.buffer.size\": 1024,\n" +
                " \t\t\"transactional.zookeeper.servers\": null,\n" +
                " \t\t\"storm.zookeeper.root\": \"/storm\",\n" +
                " \t\t\"storm.zookeeper.retry.intervalceiling.millis\": 30000,\n" +
                " \t\t\"supervisor.enable\": true,\n" +
                " \t\t\"storm.messaging.netty.server_worker_threads\": 1\n" +
                " \t}\n" +
                " }";

        JSONObject object = new JSONObject(response);
        String[] keys = JSONObject.getNames(object);
        for (String key : keys) {
            if (key.equals("bolts")) {
                JSONArray value = (JSONArray) object.get(key);
                assertEquals(2, value.length());
                JSONObject obj = (JSONObject) value.get(0);
                assertEquals("count", obj.get("boltId"));
                assertEquals("0.003", obj.get("capacity"));
            }
        }
    }

    @Test
    public void testTopologyCapacityWiseUnCongestedOperators() {
        TopologySchedule schedule = new TopologySchedule("Topology1", 5);
        schedule.addComponents("spout_head", new Component("spout_head", 10));
        schedule.addComponents("bolt_aggregate", new Component("bolt_aggregate", 10));
        schedule.addComponents("bolt_transform", new Component("bolt_transform", 10));
        schedule.addComponents("bolt_output_sink", new Component("bolt_output_sink", 10));
        schedule.addComponents("bolt_join", new Component("bolt_join", 10));
        schedule.addComponents("bolt_filter_2", new Component("bolt_filter_2", 10));
        schedule.addComponents("bolt_filter", new Component("bolt_filter", 10));


        schedule.getComponents().get("spout_head").setCapacity(0.001);
        schedule.getComponents().get("bolt_aggregate").setCapacity(0.1);
        schedule.getComponents().get("bolt_transform").setCapacity(0.2);
        schedule.getComponents().get("bolt_output_sink").setCapacity(0.3);
        schedule.getComponents().get("bolt_join").setCapacity(0.4);
        schedule.getComponents().get("bolt_filter_2").setCapacity(0.5);
        schedule.getComponents().get("bolt_filter").setCapacity(0.6);

        ArrayList<Component> uncongestedComponents = schedule.getCapacityWiseUncongestedOperators();
        assertEquals(uncongestedComponents.size(), 2);
//        assertEquals(uncongestedComponents.get(0).getId(), "spout_head");
        assertEquals(uncongestedComponents.get(0).getId(), "bolt_aggregate");
        assertEquals(uncongestedComponents.get(1).getId(), "bolt_transform");
    }

    @Test
    public void testTopologyGivenExecs() {
        Topology topology = new Topology("T1", 1.0, 80.0, 35.0, 4L);
        assertEquals((int) topology.getExecutorsForRebalancing(), 5);
        topology.setAverageLatency(120.0);
        assertEquals((int) topology.getExecutorsForRebalancing(), 7);
        topology.setMeasuredSLOs(0.8);
        assertEquals((int) topology.getExecutorsForRebalancing(), 3);
    }

    @Test
    public void testHostname() {
        String hostname = "Unknown";
        try {
            InetAddress addr;
            addr = InetAddress.getLocalHost();
            hostname = addr.getHostName();
            System.err.println(hostname);
            assertNotEquals(hostname, "Unknown");
        } catch (UnknownHostException ex) {
            System.out.println("Hostname can not be resolved");
        }
    }


    @Test
    public void howLongDoesBusyWorkTake () {
        System.out.println(System.currentTimeMillis());
        double calc = 0.0;
        for(double i = 0; i < 10000; i++) {
            calc+=i/3.0*4.5+1.3;
        }
        System.out.println(System.currentTimeMillis());
    }
*/
    @Test
    public void testingFileRead () {
        String file = "temp.txt";
        try {
            FileChannel channel = new RandomAccessFile(file , "rw").getChannel();
            FileLock lock = channel.tryLock();
            //FileLock lock = channel.tryLock(0L, Long.MAX_VALUE, true);
            while (lock == null) {
                //    channel.tryLock(0L, Long.MAX_VALUE, true);
                channel.tryLock();
            }
            //FileWriter fileWriter = new FileWriter(file, true);
            FileWriter fileWriter = new FileWriter(file, false);
            BufferedWriter bufferWriter = new BufferedWriter(fileWriter);
            bufferWriter.append("pooh");
            bufferWriter.close();
            fileWriter.close();
            lock.release();
            channel.close();


            channel = new RandomAccessFile(file , "rw").getChannel();
            lock = channel.tryLock();
            //FileLock lock = channel.tryLock(0L, Long.MAX_VALUE, true);
            while (lock == null) {
                //    channel.tryLock(0L, Long.MAX_VALUE, true);
                channel.tryLock();
            }
            //FileWriter fileWriter = new FileWriter(file, true);
            fileWriter = new FileWriter(file, true);
            bufferWriter = new BufferedWriter(fileWriter);
            bufferWriter.append(" hoop");
            bufferWriter.append(" green");

            MultithreadingDemo obj=new MultithreadingDemo();
            Thread tobj = new Thread(obj);
            tobj.run();

            bufferWriter.append(" this makes no difference");

            bufferWriter.close();
            fileWriter.close();
            lock.release();
            channel.close();

            tobj.run();

            File file1 = new File(file);
            file1.delete();
        } catch (IOException ex) {
            System.out.println(ex.toString());

        } catch (Exception ex) {
            System.out.println(ex.toString());
        }
    }

    class MultithreadingDemo implements Runnable{
        public void run(){
            String file = "temp.txt";
            BufferedReader br = null;
            String line = "";
            try {
                br = new BufferedReader(new FileReader(file));
                line = br.readLine();
                System.out.println(line);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
}
