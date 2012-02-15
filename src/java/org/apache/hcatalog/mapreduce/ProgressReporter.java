package org.apache.hcatalog.mapreduce;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.Progressable;

class ProgressReporter implements  Reporter {

    private Progressable progressable;

    public ProgressReporter(TaskAttemptContext context) {
            this(context instanceof TaskInputOutputContext?
                    (TaskInputOutputContext)context:
                    Reporter.NULL);
    }

    public ProgressReporter(Progressable progressable) {
        this.progressable = progressable;
    }

    @Override
    public void setStatus(String status) {
    }

    @Override
    public Counters.Counter getCounter(Enum<?> name) {
        return Reporter.NULL.getCounter(name);
    }

    @Override
    public Counters.Counter getCounter(String group, String name) {
        return Reporter.NULL.getCounter(group,name);
    }

    @Override
    public void incrCounter(Enum<?> key, long amount) {
    }

    @Override
    public void incrCounter(String group, String counter, long amount) {
    }

    @Override
    public InputSplit getInputSplit() throws UnsupportedOperationException {
        return Reporter.NULL.getInputSplit();
    }

    @Override
    public void progress() {
        progressable.progress();
    }
}
