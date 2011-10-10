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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.hbase.HBaseSerDe;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hcatalog.mapreduce.InputJobInfo;
import org.junit.Test;

public class TestHBaseInputStorageDriver extends SkeletonHBaseTest {
    
    private final byte[] FAMILY     = Bytes.toBytes("testFamily");
    private final byte[] QUALIFIER1 = Bytes.toBytes("testQualifier1");
    private final byte[] QUALIFIER2 = Bytes.toBytes("testQualifier2");
    private final String tableName  = "mytesttable";
    
    List<Put> generatePuts(int num) {
        List<Put> myPuts = new ArrayList<Put>();
        for (int i = 0; i < num; i++) {
            Put put = new Put(Bytes.toBytes("testRow" + i));
            put.add(FAMILY, QUALIFIER1, 0,
                    Bytes.toBytes("testQualifier1-" + "textValue-" + i));
            put.add(FAMILY, QUALIFIER2, 0,
                    Bytes.toBytes("testQualifier2-" + "textValue-" + i));
            myPuts.add(put);
        }
        return myPuts;
    }
    
    private void registerHBaseTable(String tableName) throws Exception {
        
        String databaseName = MetaStoreUtils.DEFAULT_DATABASE_NAME;
        HiveMetaStoreClient client = getCluster().getHiveMetaStoreClient();
        try {
            client.dropTable(databaseName, tableName);
        } catch (Exception e) {
        } // can fail with NoSuchObjectException
        
        Table tbl = new Table();
        tbl.setDbName(databaseName);
        tbl.setTableName(tableName);
        tbl.setTableType(TableType.EXTERNAL_TABLE.toString());
        tbl.setPartitionKeys(new ArrayList<FieldSchema>());
        Map<String, String> tableParams = new HashMap<String, String>();
        tableParams.put(HCatConstants.HCAT_ISD_CLASS,
                HBaseInputStorageDriver.class.getName());
        tableParams.put(HCatConstants.HCAT_OSD_CLASS, "NotRequired");
        tableParams.put(HBaseConstants.PROPERTY_COLUMN_MAPPING_KEY,
                ":key,testFamily:testQualifier1,testFamily:testQualifier2");
        tableParams.put(Constants.SERIALIZATION_FORMAT, "9");
        tableParams.put(Constants.SERIALIZATION_NULL_FORMAT, "NULL");
        tbl.setParameters(tableParams);
        
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(HCatUtil.getFieldSchemaList(getSchema().getFields()));
        sd.setBucketCols(new ArrayList<String>(3));
        sd.setSerdeInfo(new SerDeInfo());
        sd.getSerdeInfo().setName(tbl.getTableName());
        sd.getSerdeInfo().setParameters(new HashMap<String, String>());
        sd.getSerdeInfo().getParameters()
                .put(Constants.SERIALIZATION_FORMAT, "9");
        sd.getSerdeInfo().setSerializationLib(HBaseSerDe.class.getName());
        sd.setInputFormat(HBaseInputFormat.class.getName());
        sd.setOutputFormat("NotRequired");
        
        tbl.setSd(sd);
        client.createTable(tbl);
        
    }
    
    public void populateTable() throws IOException {
        List<Put> myPuts = generatePuts(10);
        HTable table = new HTable(getHbaseConf(), Bytes.toBytes(tableName));
        table.put(myPuts);
    }
    
    @Test
    public void TestHBaseTableReadMR() throws Exception {
        
        Configuration conf = new Configuration();
        // include hbase config in conf file
        for (Map.Entry<String, String> el : getHbaseConf()) {
            if (el.getKey().startsWith("hbase.")) {
                conf.set(el.getKey(), el.getValue());
            }
        }
        
        conf.set(HCatConstants.HCAT_KEY_HIVE_CONF,
                HCatUtil.serialize(getHiveConf().getAllProperties()));
        
        // create Hbase table using admin
        createTable(tableName, new String[] { "testFamily" });
        registerHBaseTable(tableName);
        populateTable();
        // output settings
        Path outputDir = new Path(getTestDir(), "mapred/testHbaseTableMRRead");
        FileSystem fs = getFileSystem();
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }
        // create job
        Job job = new Job(conf, "hbase-mr-read-test");
        job.setJarByClass(this.getClass());
        job.setMapperClass(MapReadHTable.class);
        
        job.getConfiguration().set(TableInputFormat.INPUT_TABLE, tableName);
        
        job.setInputFormatClass(HCatInputFormat.class);
        InputJobInfo inputJobInfo = InputJobInfo.create(
                MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName, null, null,
                null);
        HCatInputFormat.setOutputSchema(job, getSchema());
        HCatInputFormat.setInput(job, inputJobInfo);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, outputDir);
        job.setMapOutputKeyClass(BytesWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        assertTrue(job.waitForCompletion(true));
        assertTrue(MapReadHTable.error == false);
    }
    
    public static class MapReadHTable
            extends
            Mapper<ImmutableBytesWritable, HCatRecord, WritableComparable, Text> {
        
        static boolean error = false;
        
        @Override
        public void map(ImmutableBytesWritable key, HCatRecord value,
                Context context) throws IOException, InterruptedException {
            boolean correctValues = (value.size() == 3)
                    && (value.get(0).toString()).startsWith("testRow")
                    && (value.get(1).toString()).startsWith("testQualifier1")
                    && (value.get(2).toString()).startsWith("testQualifier2");
            
            if (correctValues == false) {
                error = true;
            }
        }
    }
    
    private HCatSchema getSchema() throws HCatException {
        
        HCatSchema schema = new HCatSchema(new ArrayList<HCatFieldSchema>());
        schema.append(new HCatFieldSchema("key", HCatFieldSchema.Type.STRING,
                ""));
        schema.append(new HCatFieldSchema("testqualifier1",
                HCatFieldSchema.Type.STRING, ""));
        schema.append(new HCatFieldSchema("testqualifier2",
                HCatFieldSchema.Type.STRING, ""));
        return schema;
    }
}
