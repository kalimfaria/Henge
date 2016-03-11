package backtype.storm.scheduler.advancedstela.etp;


import backtype.storm.generated.ExecutorSummary;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class ExecutorPair {
    private ExecutorSummary targetExecutorSummary;
    private ExecutorSummary victimExecutorSummary;
    private File flatline_log;

    public ExecutorPair(ExecutorSummary target, ExecutorSummary summary) {
        targetExecutorSummary = target;
        victimExecutorSummary = summary;
        flatline_log = new File("/tmp/flat_line.log");
    }

    public ExecutorSummary getTargetExecutorSummary() {
        return targetExecutorSummary;
    }

    public ExecutorSummary getVictimExecutorSummary() {
        return victimExecutorSummary;
    }

    public boolean bothPopulated() {

        writeToFile(flatline_log, "Is target executor summary not null? : " +  ( targetExecutorSummary != null ));
        writeToFile(flatline_log, "Is target victim summary not null? : " +  ( victimExecutorSummary != null ));
        return targetExecutorSummary != null && victimExecutorSummary != null;
    }

    public void writeToFile(File file, String data) {
        try {
            FileWriter fileWriter = new FileWriter(file, true);
            BufferedWriter bufferWriter = new BufferedWriter(fileWriter);
            bufferWriter.append(data);
            bufferWriter.close();
            fileWriter.close();

        } catch (IOException ex) {

        }
    }
}
