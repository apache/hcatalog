/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hcatalog.hbase;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hcatalog.data.HCatRecord;

import java.io.IOException;
import java.util.Properties;


/**
 * Storage driver which works with {@link HBaseBulkOutputFormat} and makes use
 * of HBase's "bulk load" feature to get data into HBase. This should be
 * efficient for large batch writes in comparison to HBaseDirectOutputStorageDriver.
 */
public class HBaseBulkOutputStorageDriver extends HBaseBaseOutputStorageDriver {
    private OutputFormat outputFormat;
    private final static ImmutableBytesWritable EMPTY_KEY = new ImmutableBytesWritable(new byte[0]);

    @Override
    public void initialize(JobContext context, Properties hcatProperties) throws IOException {
        super.initialize(context, hcatProperties);
        Path outputDir = new Path(outputJobInfo.getLocation());
        context.getConfiguration().set("mapred.output.dir", outputDir.toString());
        outputFormat = new HBaseBulkOutputFormat();
    }

    @Override
    public OutputFormat<? extends WritableComparable<?>, ? extends Writable> getOutputFormat() throws IOException {
        return outputFormat;
    }

    @Override
    public WritableComparable<?> generateKey(HCatRecord value) throws IOException {
        return EMPTY_KEY;
    }

}
