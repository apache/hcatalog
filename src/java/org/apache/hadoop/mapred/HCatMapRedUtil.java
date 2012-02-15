package org.apache.hadoop.mapred;

import org.apache.hadoop.util.Progressable;

public class HCatMapRedUtil {

    public static TaskAttemptContext createTaskAttemptContext(org.apache.hadoop.mapreduce.TaskAttemptContext context) {
        return  createTaskAttemptContext(new JobConf(context.getConfiguration()),
                                                            org.apache.hadoop.mapred.TaskAttemptID.forName(context.getTaskAttemptID().toString()),
                                                             Reporter.NULL);
    }

    public static TaskAttemptContext createTaskAttemptContext(JobConf conf, TaskAttemptID id, Progressable progressable) {
        return  new TaskAttemptContext(conf,id,progressable);
    }

    public static org.apache.hadoop.mapred.JobContext createJobContext(org.apache.hadoop.mapreduce.JobContext context) {
        return createJobContext(new JobConf(context.getConfiguration()),
                                            context.getJobID(),
                                            Reporter.NULL);
    }

    public static JobContext createJobContext(JobConf conf, org.apache.hadoop.mapreduce.JobID id, Progressable progressable) {
        return  new JobContext(conf,id,progressable);
    }
}
