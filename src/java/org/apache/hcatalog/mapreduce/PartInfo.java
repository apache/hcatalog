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

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.plan.TableDesc;

import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.mapreduce.HCatStorageHandler;

/** The Class used to serialize the partition information read from the metadata server that maps to a partition */
public class PartInfo implements Serializable {

  /** The serialization version */
  private static final long serialVersionUID = 1L;

  /** The partition schema. */
  private final HCatSchema partitionSchema;

  /** The information about which input storage handler to use */
  private final HCatStorageHandler storageHandler;

  /** HCat-specific properties set at the partition */
  private final Properties hcatProperties;

  /** The data location. */
  private final String location;

  /** The map of partition key names and their values. */
  private Map<String,String> partitionValues;

  /** Job properties associated with this parition */
  Map<String,String> jobProperties;

  /** the table info associated with this partition */
  HCatTableInfo tableInfo;

  /**
   * Instantiates a new hcat partition info.
   * @param partitionSchema the partition schema
   * @param storageHandler the storage handler
   * @param location the location
   * @param hcatProperties hcat-specific properties at the partition
   */
  public PartInfo(HCatSchema partitionSchema, HCatStorageHandler storageHandler,
                  String location, Properties hcatProperties, 
                  Map<String,String> jobProperties, HCatTableInfo tableInfo){
    this.partitionSchema = partitionSchema;
    this.storageHandler = storageHandler;
    this.location = location;
    this.hcatProperties = hcatProperties;
    this.jobProperties = jobProperties;
    this.tableInfo = tableInfo;
  }

  /**
   * Gets the value of partitionSchema.
   * @return the partitionSchema
   */
  public HCatSchema getPartitionSchema() {
    return partitionSchema;
  }


  /**
   * Gets the value of input storage driver class name.
   * @return the input storage driver class name
   */
  public HCatStorageHandler getStorageHandler() {
    return storageHandler;
  }


  /**
   * Gets the value of hcatProperties.
   * @return the hcatProperties
   */
  public Properties getInputStorageHandlerProperties() {
    return hcatProperties;
  }

  /**
   * Gets the value of location.
   * @return the location
   */
  public String getLocation() {
    return location;
  }

  /**
   * Sets the partition values.
   * @param partitionValues the new partition values
   */
  public void setPartitionValues(Map<String,String> partitionValues) {
    this.partitionValues = partitionValues;
  }

  /**
   * Gets the partition values.
   * @return the partition values
   */
  public Map<String,String> getPartitionValues() {
    return partitionValues;
  }

  public Map<String,String> getJobProperties() {
    return jobProperties;
  }

  public HCatTableInfo getTableInfo() {
    return tableInfo;
  }
}
