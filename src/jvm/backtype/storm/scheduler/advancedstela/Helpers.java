package backtype.storm.scheduler.advancedstela;

import backtype.storm.scheduler.ExecutorDetails;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by fariakalim on 1/13/17.
 */
public class Helpers {

    public Map<String, Integer> flipExecsMap(Map<ExecutorDetails, String> ExecutorsToComponents) {
        HashMap<String, Integer> flippedMap = new HashMap<>();
        for (Map.Entry<ExecutorDetails, String> entry : ExecutorsToComponents.entrySet()) {
            if (flippedMap.containsKey(entry.getValue())) {
                flippedMap.put(entry.getValue(), (flippedMap.get(entry.getValue()) + 1));
            } else {
                flippedMap.put(entry.getValue(), 1);
            }
        }
        return flippedMap;
    }

}
