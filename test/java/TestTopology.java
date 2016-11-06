

import backtype.storm.scheduler.advancedstela.BriefHistory;
import backtype.storm.scheduler.advancedstela.TopologyPicker;
import backtype.storm.scheduler.advancedstela.etp.SupervisorInfo;
import backtype.storm.scheduler.advancedstela.slo.Sensitivity;
import backtype.storm.scheduler.advancedstela.slo.Topology;
import com.google.gson.Gson;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by fariakalim on 10/31/16.
 */
public class TestTopology {

    Topology t1;
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
    public void testSupervisorOverUtilization (){
        SupervisorInfo supervisorInfo = new SupervisorInfo();
        HashMap<String, SupervisorInfo.Info> infos = new HashMap <>();

        String response = "{\"recentLoad\": \"4.0\"," +
                "         \"minLoad\": \"1.2\"," +
                "         \"fiveMinsLoad\": \"1.4\"," +
                "         \"freeMem\": \"4.0\"," +
                "         \"usedMemory\":\"4.0\",\n" +
                "         \"usedMemPercent\": \"4.0\"," +
                "         \"time\": \"12312434434\"}";
        Gson gson = new Gson();
        SupervisorInfo.Info info = gson.fromJson(response.toString(), SupervisorInfo.Info.class);
        String [] supervisors = {"pc427.emulab.net", "pc553.emulab.net", "pc538.emulab.net"};
        for (int i = 0; i < supervisorInfo.HISTORY_SIZE; i++) {
            int j = 0;
            for (String supervisor : supervisors){
                if (j == 0) info.recentLoad = supervisorInfo.MAXIMUM_LOAD_PER_MACHINE;
                else info.recentLoad = 0.0;
                infos.put(supervisor, info);
                info = gson.fromJson(response.toString(), SupervisorInfo.Info.class);
                j++;
            }
            supervisorInfo.insertInfo(infos);
        }
        /*for (HashMap<String, SupervisorInfo.Info> information : supervisorInfo.infoHistory) {
            for (String supervisor : supervisors){
                System.err.println("Supervisor: " + supervisor + " recent load: " + information.get(supervisor).recentLoad);
            }
        }*/
        assertEquals(true, supervisorInfo.areSupervisorsOverUtilized());
    }
}
