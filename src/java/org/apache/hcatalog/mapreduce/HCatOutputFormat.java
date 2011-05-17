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

package org.apache.hcatalog.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier;
import org.apache.hadoop.hive.thrift.DelegationTokenSelector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.hcatalog.common.ErrorType;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.thrift.TException;

/** The OutputFormat to use to write data to HCat. The key value is ignored and
 * and should be given as null. The value is the HCatRecord to write.*/
public class HCatOutputFormat extends HCatBaseOutputFormat {

    /** The directory under which data is initially written for a non partitioned table */
    protected static final String TEMP_DIR_NAME = "_TEMP";
    private static Map<String, Token<DelegationTokenIdentifier>> tokenMap = new HashMap<String, Token<DelegationTokenIdentifier>>();

    private static final PathFilter hiddenFileFilter = new PathFilter(){
      public boolean accept(Path p){
        String name = p.getName();
        return !name.startsWith("_") && !name.startsWith(".");
      }
    };

    /**
     * Set the info about the output to write for the Job. This queries the metadata server
     * to find the StorageDriver to use for the table.  Throws error if partition is already published.
     * @param job the job object
     * @param outputInfo the table output info
     * @throws IOException the exception in communicating with the metadata server
     */
    @SuppressWarnings("unchecked")
    public static void setOutput(Job job, HCatTableInfo outputInfo) throws IOException {
      HiveMetaStoreClient client = null;

      try {

	Configuration conf = job.getConfiguration();
        client = createHiveClient(outputInfo.getServerUri(), conf);
        Table table = client.getTable(outputInfo.getDatabaseName(), outputInfo.getTableName());

        if( outputInfo.getPartitionValues() == null ) {
          outputInfo.setPartitionValues(new HashMap<String, String>());
        } else {
          //Convert user specified map to have lower case key names
          Map<String, String> valueMap = new HashMap<String, String>();
          for(Map.Entry<String, String> entry : outputInfo.getPartitionValues().entrySet()) {
            valueMap.put(entry.getKey().toLowerCase(), entry.getValue());
          }

          outputInfo.setPartitionValues(valueMap);
        }

        //Handle duplicate publish
        handleDuplicatePublish(job, outputInfo, client, table);

        StorageDescriptor tblSD = table.getSd();
        HCatSchema tableSchema = HCatUtil.extractSchemaFromStorageDescriptor(tblSD);
        StorerInfo storerInfo = InitializeInput.extractStorerInfo(tblSD,table.getParameters());

        List<String> partitionCols = new ArrayList<String>();
        for(FieldSchema schema : table.getPartitionKeys()) {
          partitionCols.add(schema.getName());
        }

        Class<? extends HCatOutputStorageDriver> driverClass =
          (Class<? extends HCatOutputStorageDriver>) Class.forName(storerInfo.getOutputSDClass());
        HCatOutputStorageDriver driver = driverClass.newInstance();

        String tblLocation = tblSD.getLocation();
        String location = driver.getOutputLocation(job,
            tblLocation, partitionCols,
            outputInfo.getPartitionValues());

        //Serialize the output info into the configuration
        OutputJobInfo jobInfo = new OutputJobInfo(outputInfo,
                tableSchema, tableSchema, storerInfo, location, table);
        conf.set(HCatConstants.HCAT_KEY_OUTPUT_INFO, HCatUtil.serialize(jobInfo));

        Path tblPath = new Path(tblLocation);

        /*  Set the umask in conf such that files/dirs get created with table-dir
         * permissions. Following three assumptions are made:
         * 1. Actual files/dirs creation is done by RecordWriter of underlying
         * output format. It is assumed that they use default permissions while creation.
         * 2. Default Permissions = FsPermission.getDefault() = 777.
         * 3. UMask is honored by underlying filesystem.
         */

        FsPermission.setUMask(conf, FsPermission.getDefault().applyUMask(
            tblPath.getFileSystem(conf).getFileStatus(tblPath).getPermission()));

        if(UserGroupInformation.isSecurityEnabled()){
          UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
          // check if oozie has set up a hcat deleg. token - if so use it
          TokenSelector<? extends TokenIdentifier> tokenSelector = new DelegationTokenSelector();
          // TODO: will oozie use a "service" called "oozie" - then instead of
          // new Text() do new Text("oozie") below - if this change is made also
          // remember to do:
          //  job.getConfiguration().set(HCAT_KEY_TOKEN_SIGNATURE, "oozie");
          // Also change code in HCatOutputCommitter.cleanupJob() to cancel the
          // token only if token.service is not "oozie" - remove the condition of
          // HCAT_KEY_TOKEN_SIGNATURE != null in that code.
          Token<? extends TokenIdentifier> token = tokenSelector.selectToken(
              new Text(), ugi.getTokens());
          if(token != null) {

            job.getCredentials().addToken(new Text(ugi.getUserName()),token);

          } else {

            // we did not get token set up by oozie, let's get them ourselves here.
            // we essentially get a token per unique Output HCatTableInfo - this is
            // done because through Pig, setOutput() method is called multiple times
            // We want to only get the token once per unique output HCatTableInfo -
            // we cannot just get one token since in multi-query case (> 1 store in 1 job)
            // or the case when a single pig script results in > 1 jobs, the single
            // token will get cancelled by the output committer and the subsequent
            // stores will fail - by tying the token with the concatenation of
            // dbname, tablename and partition keyvalues of the output
            // TableInfo, we can have as many tokens as there are stores and the TokenSelector
            // will correctly pick the right tokens which the committer will use and
            // cancel.
            String tokenSignature = getTokenSignature(outputInfo);
            if(tokenMap.get(tokenSignature) == null) {
              // get delegation tokens from hcat server and store them into the "job"
              // These will be used in the HCatOutputCommitter to publish partitions to
              // hcat
              // when the JobTracker in Hadoop MapReduce starts supporting renewal of 
              // arbitrary tokens, the renewer should be the principal of the JobTracker
              String tokenStrForm = client.getDelegationToken(ugi.getUserName());
              Token<DelegationTokenIdentifier> t = new Token<DelegationTokenIdentifier>();
              t.decodeFromUrlString(tokenStrForm);
              t.setService(new Text(tokenSignature));
              tokenMap.put(tokenSignature, t);
            }
            job.getCredentials().addToken(new Text(ugi.getUserName() + tokenSignature),
                tokenMap.get(tokenSignature));
            // this will be used by the outputcommitter to pass on to the metastore client
            // which in turn will pass on to the TokenSelector so that it can select
            // the right token.
            job.getConfiguration().set(HCatConstants.HCAT_KEY_TOKEN_SIGNATURE, tokenSignature);
        }
       }
      } catch(Exception e) {
        if( e instanceof HCatException ) {
          throw (HCatException) e;
        } else {
          throw new HCatException(ErrorType.ERROR_SET_OUTPUT, e);
        }
      } finally {
        if( client != null ) {
          client.close();
        }
      }
    }


    // a signature string to associate with a HCatTableInfo - essentially
    // a concatenation of dbname, tablename and partition keyvalues.
    private static String getTokenSignature(HCatTableInfo outputInfo) {
      StringBuilder result = new StringBuilder("");
      String dbName = outputInfo.getDatabaseName();
      if(dbName != null) {
        result.append(dbName);
      }
      String tableName = outputInfo.getTableName();
      if(tableName != null) {
        result.append("+" + tableName);
      }
      Map<String, String> partValues = outputInfo.getPartitionValues();
      if(partValues != null) {
        for(Entry<String, String> entry: partValues.entrySet()) {
          result.append("+" + entry.getKey() + "=" + entry.getValue());
        }
      }
      return result.toString();
    }



    /**
     * Handles duplicate publish of partition. Fails if partition already exists.
     * For non partitioned tables, fails if files are present in table directory.
     * @param job the job
     * @param outputInfo the output info
     * @param client the metastore client
     * @param table the table being written to
     * @throws IOException
     * @throws MetaException
     * @throws TException
     */
    private static void handleDuplicatePublish(Job job, HCatTableInfo outputInfo,
        HiveMetaStoreClient client, Table table) throws IOException, MetaException, TException {
      List<String> partitionValues = HCatOutputCommitter.getPartitionValueList(
                  table, outputInfo.getPartitionValues());

      if( table.getPartitionKeys().size() > 0 ) {
        //For partitioned table, fail if partition is already present
        List<String> currentParts = client.listPartitionNames(outputInfo.getDatabaseName(),
            outputInfo.getTableName(), partitionValues, (short) 1);

        if( currentParts.size() > 0 ) {
          throw new HCatException(ErrorType.ERROR_DUPLICATE_PARTITION);
        }
      } else {
        Path tablePath = new Path(table.getSd().getLocation());
        FileSystem fs = tablePath.getFileSystem(job.getConfiguration());

        if ( fs.exists(tablePath) ) {
          FileStatus[] status = fs.globStatus(new Path(tablePath, "*"), hiddenFileFilter);

          if( status.length > 0 ) {
            throw new HCatException(ErrorType.ERROR_NON_EMPTY_TABLE,
                      table.getDbName() + "." + table.getTableName());
          }
        }
      }
    }

    /**
     * Set the schema for the data being written out to the partition. The
     * table schema is used by default for the partition if this is not called.
     * @param job the job object
     * @param schema the schema for the data
     */
    public static void setSchema(final Job job, final HCatSchema schema) throws IOException {

        OutputJobInfo jobInfo = getJobInfo(job);
        Map<String,String> partMap = jobInfo.getTableInfo().getPartitionValues();
        setPartDetails(jobInfo, schema, partMap);
        job.getConfiguration().set(HCatConstants.HCAT_KEY_OUTPUT_INFO, HCatUtil.serialize(jobInfo));
    }

    /**
     * Get the record writer for the job. Uses the Table's default OutputStorageDriver
     * to get the record writer.
     * @param context the information about the current task.
     * @return a RecordWriter to write the output for the job.
     * @throws IOException
     */
    @Override
    public RecordWriter<WritableComparable<?>, HCatRecord>
      getRecordWriter(TaskAttemptContext context
                      ) throws IOException, InterruptedException {

      // First create the RW.
      HCatRecordWriter rw = new HCatRecordWriter(context);

      // Now set permissions and group on freshly created files.
      OutputJobInfo info =  getJobInfo(context);
      Path workFile = rw.getStorageDriver().getWorkFilePath(context,info.getLocation());
      Path tblPath = new Path(info.getTable().getSd().getLocation());
      FileSystem fs = tblPath.getFileSystem(context.getConfiguration());
      FileStatus tblPathStat = fs.getFileStatus(tblPath);
      fs.setPermission(workFile, tblPathStat.getPermission());
      try{
        fs.setOwner(workFile, null, tblPathStat.getGroup());
      } catch(AccessControlException ace){
        // log the messages before ignoring. Currently, logging is not built in HCat.
      }
      return rw;
    }

    /**
     * Get the output committer for this output format. This is responsible
     * for ensuring the output is committed correctly.
     * @param context the task context
     * @return an output committer
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context
                                       ) throws IOException, InterruptedException {
        OutputFormat<? super WritableComparable<?>, ? super Writable> outputFormat = getOutputFormat(context);
        return new HCatOutputCommitter(outputFormat.getOutputCommitter(context));
    }

    static HiveMetaStoreClient createHiveClient(String url, Configuration conf) throws IOException, MetaException {
      HiveConf hiveConf = new HiveConf(HCatOutputFormat.class);

      if( url != null ) {
        //User specified a thrift url

        hiveConf.setBoolean(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname, true);
        hiveConf.set(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname, conf.get(HCatConstants.HCAT_METASTORE_PRINCIPAL));

        hiveConf.set("hive.metastore.local", "false");
        hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, url);
        if(conf.get(HCatConstants.HCAT_KEY_TOKEN_SIGNATURE) != null) {
          hiveConf.set("hive.metastore.token.signature", conf.get(HCatConstants.HCAT_KEY_TOKEN_SIGNATURE));
        }
      } else {
        //Thrift url is null, copy the hive conf into the job conf and restore it
        //in the backend context

        if( conf.get(HCatConstants.HCAT_KEY_HIVE_CONF) == null ) {
          conf.set(HCatConstants.HCAT_KEY_HIVE_CONF, HCatUtil.serialize(hiveConf.getAllProperties()));
        } else {
          //Copy configuration properties into the hive conf
          Properties properties = (Properties) HCatUtil.deserialize(conf.get(HCatConstants.HCAT_KEY_HIVE_CONF));

          for(Map.Entry<Object, Object> prop : properties.entrySet() ) {
            if( prop.getValue() instanceof String ) {
              hiveConf.set((String) prop.getKey(), (String) prop.getValue());
            } else if( prop.getValue() instanceof Integer ) {
              hiveConf.setInt((String) prop.getKey(), (Integer) prop.getValue());
            } else if( prop.getValue() instanceof Boolean ) {
              hiveConf.setBoolean((String) prop.getKey(), (Boolean) prop.getValue());
            } else if( prop.getValue() instanceof Long ) {
              hiveConf.setLong((String) prop.getKey(), (Long) prop.getValue());
            } else if( prop.getValue() instanceof Float ) {
              hiveConf.setFloat((String) prop.getKey(), (Float) prop.getValue());
            }
          }
        }

      }

      return new HiveMetaStoreClient(hiveConf);
    }


}
