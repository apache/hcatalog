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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hcatalog.common.ErrorType;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatSchema;

public abstract class HCatBaseOutputFormat extends OutputFormat<WritableComparable<?>, HCatRecord> {

  /**
   * Gets the table schema for the table specified in the HowlOutputFormat.setOutput call
   * on the specified job context.
   * @param context the context
   * @return the table schema
   * @throws IOException if HowlOutputFromat.setOutput has not been called for the passed context
   */
  public static HCatSchema getTableSchema(JobContext context) throws IOException {
      OutputJobInfo jobInfo = getJobInfo(context);
      return jobInfo.getTableSchema();
  }

  /**
   * Check for validity of the output-specification for the job.
   * @param context information about the job
   * @throws IOException when output should not be attempted
   */
  @Override
  public void checkOutputSpecs(JobContext context
                                        ) throws IOException, InterruptedException {
      OutputFormat<? super WritableComparable<?>, ? super Writable> outputFormat = getOutputFormat(context);
      outputFormat.checkOutputSpecs(context);
  }

  /**
   * Gets the output format instance.
   * @param context the job context
   * @return the output format instance
   * @throws IOException
   */
  protected OutputFormat<? super WritableComparable<?>, ? super Writable> getOutputFormat(JobContext context) throws IOException {
      OutputJobInfo jobInfo = getJobInfo(context);
      HCatOutputStorageDriver  driver = getOutputDriverInstance(context, jobInfo);

      OutputFormat<? super WritableComparable<?>, ? super Writable> outputFormat =
            driver.getOutputFormat();
      return outputFormat;
  }

  /**
   * Gets the HowlOuputJobInfo object by reading the Configuration and deserializing
   * the string. If JobInfo is not present in the configuration, throws an
   * exception since that means HowlOutputFormat.setOutput has not been called.
   * @param jobContext the job context
   * @return the OutputJobInfo object
   * @throws IOException the IO exception
   */
  static OutputJobInfo getJobInfo(JobContext jobContext) throws IOException {
      String jobString = jobContext.getConfiguration().get(HCatConstants.HCAT_KEY_OUTPUT_INFO);
      if( jobString == null ) {
          throw new HCatException(ErrorType.ERROR_NOT_INITIALIZED);
      }

      return (OutputJobInfo) HCatUtil.deserialize(jobString);
  }

  /**
   * Gets the output storage driver instance.
   * @param jobContext the job context
   * @param jobInfo the output job info
   * @return the output driver instance
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  static HCatOutputStorageDriver getOutputDriverInstance(
          JobContext jobContext, OutputJobInfo jobInfo) throws IOException {
      try {
          Class<? extends HCatOutputStorageDriver> driverClass =
              (Class<? extends HCatOutputStorageDriver>)
              Class.forName(jobInfo.getStorerInfo().getOutputSDClass());
          HCatOutputStorageDriver driver = driverClass.newInstance();

          //Initialize the storage driver
          driver.setSchema(jobContext, jobInfo.getOutputSchema());
          driver.setPartitionValues(jobContext, jobInfo.getTableInfo().getPartitionValues());
          driver.setOutputPath(jobContext, jobInfo.getLocation());

          driver.initialize(jobContext, jobInfo.getStorerInfo().getProperties());

          return driver;
      } catch(Exception e) {
          throw new HCatException(ErrorType.ERROR_INIT_STORAGE_DRIVER, e);
      }
  }

  protected static void setPartDetails(OutputJobInfo jobInfo, final HCatSchema schema,
      Map<String, String> partMap) throws HCatException, IOException {
    List<Integer> posOfPartCols = new ArrayList<Integer>();

    // If partition columns occur in data, we want to remove them.
    // So, find out positions of partition columns in schema provided by user.
    // We also need to update the output Schema with these deletions.

    // Note that, output storage drivers never sees partition columns in data
    // or schema.

    HCatSchema schemaWithoutParts = new HCatSchema(schema.getFields());
    for(String partKey : partMap.keySet()){
      Integer idx;
      if((idx = schema.getPosition(partKey)) != null){
        posOfPartCols.add(idx);
        schemaWithoutParts.remove(schema.get(partKey));
      }
    }
    HCatUtil.validatePartitionSchema(jobInfo.getTable(), schemaWithoutParts);
    jobInfo.setPosOfPartCols(posOfPartCols);
    jobInfo.setOutputSchema(schemaWithoutParts);
  }
}
