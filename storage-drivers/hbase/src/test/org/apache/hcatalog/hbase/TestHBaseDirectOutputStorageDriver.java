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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.*;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.data.schema.HCatSchemaUtils;
import org.apache.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hcatalog.mapreduce.OutputJobInfo;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test HBaseDirectOuputStorageDriver and HBaseDirectOUtputFormat using a MiniCluster
 */
public class TestHBaseDirectOutputStorageDriver extends SkeletonHBaseTest {

    private void registerHBaseTable(String tableName) throws Exception {

        String databaseName = MetaStoreUtils.DEFAULT_DATABASE_NAME ;
        HiveMetaStoreClient client = new HiveMetaStoreClient(getHiveConf());

        try {
            client.dropTable(databaseName, tableName);
        } catch(Exception e) {
        } //can fail with NoSuchObjectException


        Table tbl = new Table();
        tbl.setDbName(databaseName);
        tbl.setTableName(tableName);
        tbl.setTableType(TableType.EXTERNAL_TABLE.toString());
        StorageDescriptor sd = new StorageDescriptor();

        sd.setCols(getTableColumns());
        tbl.setPartitionKeys(new ArrayList<FieldSchema>());

        tbl.setSd(sd);

        sd.setBucketCols(new ArrayList<String>(2));
        sd.setSerdeInfo(new SerDeInfo());
        sd.getSerdeInfo().setName(tbl.getTableName());
        sd.getSerdeInfo().setParameters(new HashMap<String, String>());
        sd.getSerdeInfo().getParameters().put(
                Constants.SERIALIZATION_FORMAT, "1");
        sd.getSerdeInfo().setSerializationLib(HBaseSerDe.class.getName());
        sd.setInputFormat("fillme");
        sd.setOutputFormat(HBaseDirectOutputFormat.class.getName());

        Map<String, String> tableParams = new HashMap<String, String>();
        tableParams.put(HCatConstants.HCAT_ISD_CLASS, "fillme");
        tableParams.put(HCatConstants.HCAT_OSD_CLASS, HBaseDirectOutputStorageDriver.class.getName());
        tableParams.put(HBaseConstants.PROPERTY_COLUMN_MAPPING_KEY,":key,my_family:english,my_family:spanish");
        tbl.setParameters(tableParams);

        client.createTable(tbl);
    }

    protected List<FieldSchema> getTableColumns() {
        List<FieldSchema> fields = new ArrayList<FieldSchema>();
        fields.add(new FieldSchema("key", Constants.INT_TYPE_NAME, ""));
        fields.add(new FieldSchema("english", Constants.STRING_TYPE_NAME, ""));
        fields.add(new FieldSchema("spanish", Constants.STRING_TYPE_NAME, ""));
        return fields;
    }

    private static  List<HCatFieldSchema> generateDataColumns() throws HCatException {
        List<HCatFieldSchema> dataColumns = new ArrayList<HCatFieldSchema>();
        dataColumns.add(HCatSchemaUtils.getHCatFieldSchema(new FieldSchema("key", Constants.INT_TYPE_NAME, "")));
        dataColumns.add(HCatSchemaUtils.getHCatFieldSchema(new FieldSchema("english", Constants.STRING_TYPE_NAME, "")));
        dataColumns.add(HCatSchemaUtils.getHCatFieldSchema(new FieldSchema("spanish", Constants.STRING_TYPE_NAME, "")));
        return dataColumns;
    }

    public void test() throws IOException {
        Configuration conf = getHbaseConf();
        String tableName = "my_table";
        byte[] tableNameBytes = Bytes.toBytes(tableName);
        String familyName = "my_family";
        byte[] familyNameBytes = Bytes.toBytes(familyName);
        createTable(tableName,new String[]{familyName});
        HTable table = new HTable(getHbaseConf(),tableNameBytes);
        byte[] key = Bytes.toBytes("foo");
        byte[] qualifier = Bytes.toBytes("qualifier");
        byte[] val = Bytes.toBytes("bar");
        Put put = new Put(key);
        put.add(familyNameBytes, qualifier, val);
        table.put(put);
        Result result = table.get(new Get(key));
        assertTrue(Bytes.equals(val, result.getValue(familyNameBytes, qualifier)));
    }

    @Test
    public void directOutputFormatTest() throws IOException, ClassNotFoundException, InterruptedException {
        String tableName = newTableName("mrTest");
        byte[] tableNameBytes = Bytes.toBytes(tableName);
        String familyName = "my_family";
        byte[] familyNameBytes = Bytes.toBytes(familyName);

        //include hbase config in conf file
        Configuration conf = new Configuration(getJobConf());
        for(Map.Entry<String,String> el: getHbaseConf()) {
            if(el.getKey().startsWith("hbase.")) {
                conf.set(el.getKey(),el.getValue());
            }
        }

        //create table
        createTable(tableName,new String[]{familyName});

        String data[] = {"1,english:ONE,spanish:UNO",
                "2,english:ONE,spanish:DOS",
                "3,english:ONE,spanish:TRES"};



        // input/output settings
        Path inputPath = new Path(getTestDir(), "mapred/testHCatMapReduceInput/");
        getFileSystem().mkdirs(inputPath);
        FSDataOutputStream os = getFileSystem().create(new Path(inputPath,"inputFile.txt"));
        for(String line: data)
            os.write(Bytes.toBytes(line + "\n"));
        os.close();

        //create job
        Job job = new Job(conf, "hcat mapreduce write test");
        job.setJarByClass(this.getClass());
        job.setMapperClass(MapWrite.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, inputPath);

        job.setOutputFormatClass(HBaseDirectOutputFormat.class);
        job.getConfiguration().set(HBaseConstants.PROPERTY_OUTPUT_TABLE_NAME_KEY, tableName);

        job.setMapOutputKeyClass(BytesWritable.class);
        job.setMapOutputValueClass(HCatRecord.class);

        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(HCatRecord.class);

        job.setNumReduceTasks(0);
        assertTrue(job.waitForCompletion(true));

        //verify
        HTable table = new HTable(conf, tableName);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("my_family"));
        ResultScanner scanner = table.getScanner(scan);
        int index=0;
        for(Result result: scanner) {
            String vals[] = data[index].toString().split(",");
            for(int i=1;i<vals.length;i++) {
                String pair[] = vals[i].split(":");
                assertTrue(result.containsColumn(familyNameBytes,Bytes.toBytes(pair[0])));
                assertEquals(pair[1],Bytes.toString(result.getValue(familyNameBytes,Bytes.toBytes(pair[0]))));
            }
            index++;
        }
        assertEquals(data.length,index);
    }

    public static class MapWrite extends Mapper<LongWritable, Text, BytesWritable, Put> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String vals[] = value.toString().split(",");
            Put put = new Put(Bytes.toBytes(vals[0]));
            for(int i=1;i<vals.length;i++) {
                String pair[] = vals[i].split(":");
                put.add(Bytes.toBytes("my_family"),
                        Bytes.toBytes(pair[0]),
                        Bytes.toBytes(pair[1]));
            }
            context.write(new BytesWritable(Bytes.toBytes(vals[0])),put);
        }
    }

    @Test
    public void directOutputStorageDriverTest() throws Exception {
        String tableName = newTableName("mrtest");
        byte[] tableNameBytes = Bytes.toBytes(tableName);
        String familyName = "my_family";
        byte[] familyNameBytes = Bytes.toBytes(familyName);


        //include hbase config in conf file
        Configuration conf = new Configuration(getJobConf());
        for(Map.Entry<String,String> el: getHbaseConf()) {
            if(el.getKey().startsWith("hbase.")) {
                conf.set(el.getKey(),el.getValue());
            }
        }

        conf.set(HCatConstants.HCAT_KEY_HIVE_CONF, HCatUtil.serialize(getHiveConf().getAllProperties()));

        //create table
        createTable(tableName,new String[]{familyName});
        registerHBaseTable(tableName);


        String data[] = {"1,english:ONE,spanish:UNO",
                "2,english:ONE,spanish:DOS",
                "3,english:ONE,spanish:TRES"};



        // input/output settings
        Path inputPath = new Path(getTestDir(), "mapred/testHCatMapReduceInput/");
        getFileSystem().mkdirs(inputPath);
        FSDataOutputStream os = getFileSystem().create(new Path(inputPath,"inputFile.txt"));
        for(String line: data)
            os.write(Bytes.toBytes(line + "\n"));
        os.close();

        //create job
        Job job = new Job(conf, "hcat mapreduce write test");
        job.setJarByClass(this.getClass());
        job.setMapperClass(MapHCatWrite.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, inputPath);


        job.setOutputFormatClass(HCatOutputFormat.class);
        OutputJobInfo outputJobInfo = OutputJobInfo.create(null,tableName,null,null,null);
        outputJobInfo.getProperties().put(HBaseConstants.PROPERTY_OUTPUT_VERSION_KEY, "1");
        HCatOutputFormat.setOutput(job,outputJobInfo);

        job.setMapOutputKeyClass(BytesWritable.class);
        job.setMapOutputValueClass(HCatRecord.class);

        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(HCatRecord.class);

        job.setNumReduceTasks(0);
        assertTrue(job.waitForCompletion(true));

        //verify
        HTable table = new HTable(conf, tableName);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("my_family"));
        ResultScanner scanner = table.getScanner(scan);
        int index=0;
        for(Result result: scanner) {
            String vals[] = data[index].toString().split(",");
            for(int i=1;i<vals.length;i++) {
                String pair[] = vals[i].split(":");
                assertTrue(result.containsColumn(familyNameBytes,Bytes.toBytes(pair[0])));
                assertEquals(pair[1],Bytes.toString(result.getValue(familyNameBytes,Bytes.toBytes(pair[0]))));
                assertEquals(1l,result.getColumn(familyNameBytes,Bytes.toBytes(pair[0])).get(0).getTimestamp());
            }
            index++;
        }
        assertEquals(data.length,index);
    }

    public static class MapHCatWrite extends Mapper<LongWritable, Text, BytesWritable, HCatRecord> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            HCatRecord record = new DefaultHCatRecord(3);
            HCatSchema schema = new HCatSchema(generateDataColumns());
            String vals[] = value.toString().split(",");
            record.setInteger("key",schema,Integer.parseInt(vals[0]));
            for(int i=1;i<vals.length;i++) {
                String pair[] = vals[i].split(":");
                record.set(pair[0],schema,pair[1]);
            }
            context.write(null,record);
        }
    }
}
