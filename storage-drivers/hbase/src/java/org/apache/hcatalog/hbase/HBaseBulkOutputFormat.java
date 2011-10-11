package org.apache.hcatalog.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * Class which imports data into HBase via it's "bulk load" feature. Wherein regions
 * are created by the MR job using HFileOutputFormat and then later "moved" into
 * the appropriate region server.
 */
class HBaseBulkOutputFormat extends SequenceFileOutputFormat<ImmutableBytesWritable,Put> {

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
        return new HBaseBulkOutputCommitter(FileOutputFormat.getOutputPath(context),context,(FileOutputCommitter)super.getOutputCommitter(context));
    }

    private static class HBaseBulkOutputCommitter extends FileOutputCommitter {
        FileOutputCommitter baseOutputCommitter;

        public HBaseBulkOutputCommitter(Path outputPath, TaskAttemptContext taskAttemptContext,
                                                           FileOutputCommitter baseOutputCommitter) throws IOException {
            super(outputPath, taskAttemptContext);
            this.baseOutputCommitter = baseOutputCommitter;
        }

        @Override
        public void abortTask(TaskAttemptContext context) {
            baseOutputCommitter.abortTask(context);
        }

        @Override
        public void commitTask(TaskAttemptContext context) throws IOException {
            baseOutputCommitter.commitTask(context);
        }

        @Override
        public Path getWorkPath() throws IOException {
            return baseOutputCommitter.getWorkPath();
        }

        @Override
        public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
            return baseOutputCommitter.needsTaskCommit(context);
        }

        @Override
        public void setupJob(JobContext context) throws IOException {
            baseOutputCommitter.setupJob(context);
        }

        @Override
        public void setupTask(TaskAttemptContext context) throws IOException {
            baseOutputCommitter.setupTask(context);
        }

        @Override
        public void abortJob(JobContext jobContext, JobStatus.State state) throws IOException {
            try {
                baseOutputCommitter.abortJob(jobContext,state);
            } finally {
                cleanIntermediate(jobContext);
            }
        }

        @Override
        public void cleanupJob(JobContext context) throws IOException {
            try {
                baseOutputCommitter.cleanupJob(context);
            } finally {
                cleanIntermediate(context);
            }
        }

        @Override
        public void commitJob(JobContext jobContext) throws IOException {
            try {
                baseOutputCommitter.commitJob(jobContext);
                Configuration conf = jobContext.getConfiguration();
                Path srcPath = FileOutputFormat.getOutputPath(jobContext);
                Path destPath = new Path(srcPath.getParent(),srcPath.getName()+"_hfiles");
                ImportSequenceFile.runJob(conf,
                                                        conf.get(HBaseConstants.PROPERTY_OUTPUT_TABLE_NAME_KEY),
                                                        srcPath,
                                                        destPath);
            } finally {
                cleanIntermediate(jobContext);
            }
        }

        public void cleanIntermediate(JobContext jobContext) throws IOException {
            FileSystem fs = FileSystem.get(jobContext.getConfiguration());
            fs.delete(FileOutputFormat.getOutputPath(jobContext),true);
        }
    }
}
