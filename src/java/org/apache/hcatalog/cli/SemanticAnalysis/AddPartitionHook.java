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
package org.apache.hcatalog.cli.SemanticAnalysis;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.security.authorization.Privilege;
import org.apache.hcatalog.common.HCatConstants;

public class AddPartitionHook extends HCatSemanticAnalyzerBase {

  private String tblName, inDriver, outDriver;

  @Override
  public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast)
      throws SemanticException {
    Map<String, String> tblProps;
    tblName = ast.getChild(0).getText();
    try {
      tblProps = context.getHive().getTable(tblName).getParameters();
    } catch (HiveException he) {
      throw new SemanticException(he);
    }

    inDriver = tblProps.get(HCatConstants.HCAT_ISD_CLASS);
    outDriver = tblProps.get(HCatConstants.HCAT_OSD_CLASS);

    if(inDriver == null  || outDriver == null){
      throw new SemanticException("Operation not supported. Partitions can be added only in a table created through HCatalog. " +
      		"It seems table "+tblName+" was not created through HCatalog.");
    }
    return ast;
  }

//  @Override
//  public void postAnalyze(HiveSemanticAnalyzerHookContext context,
//      List<Task<? extends Serializable>> rootTasks) throws SemanticException {
//    authorizeDDL(context, rootTasks);
//    try {
//      Hive db = context.getHive();
//      Table tbl = db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tblName);
//      for(Task<? extends Serializable> task : rootTasks){
//        System.err.println("PArt spec: "+((DDLWork)task.getWork()).getAddPartitionDesc().getPartSpec());
//        Partition part = db.getPartition(tbl,((DDLWork)task.getWork()).getAddPartitionDesc().getPartSpec(),false);
//        Map<String,String> partParams = part.getParameters();
//        if(partParams == null){
//          System.err.println("Part map null ");
//          partParams = new HashMap<String, String>();
//        }
//        partParams.put(InitializeInput.HOWL_ISD_CLASS, inDriver);
//        partParams.put(InitializeInput.HOWL_OSD_CLASS, outDriver);
//        part.getTPartition().setParameters(partParams);
//        db.alterPartition(tblName, part);
//      }
//    } catch (HiveException he) {
//      throw new SemanticException(he);
//    } catch (InvalidOperationException e) {
//      throw new SemanticException(e);
//    }
//  }
  
  @Override
  protected void authorizeDDLWork(HiveSemanticAnalyzerHookContext context,
      Hive hive, DDLWork work) throws HiveException {
    AddPartitionDesc addPartitionDesc = work.getAddPartitionDesc();
    if (addPartitionDesc != null) {
      String dbName = getDbName(hive, addPartitionDesc.getDbName());
      Table table = hive.getTable(dbName, addPartitionDesc.getTableName());
      Path partPath = null;
      if (addPartitionDesc.getLocation() != null) {
        partPath = new Path(table.getPath(), addPartitionDesc.getLocation());
      }
      
      Partition part = newPartition(
          table, addPartitionDesc.getPartSpec(), partPath,
          addPartitionDesc.getPartParams(),
          addPartitionDesc.getInputFormat(),
          addPartitionDesc.getOutputFormat(),
          addPartitionDesc.getNumBuckets(),
          addPartitionDesc.getCols(),
          addPartitionDesc.getSerializationLib(),
          addPartitionDesc.getSerdeParams(),
          addPartitionDesc.getBucketCols(),
          addPartitionDesc.getSortCols());
      
      authorize(part, Privilege.CREATE);
    }
  }
  
  protected Partition newPartition(Table tbl, Map<String, String> partSpec,
      Path location, Map<String, String> partParams, String inputFormat, String outputFormat,
      int numBuckets, List<FieldSchema> cols,
      String serializationLib, Map<String, String> serdeParams,
      List<String> bucketCols, List<Order> sortCols) throws HiveException {

    try {
      Partition tmpPart = new Partition(tbl, partSpec, location);
      org.apache.hadoop.hive.metastore.api.Partition inPart
        = tmpPart.getTPartition();
      if (partParams != null) {
        inPart.setParameters(partParams);
      }
      if (inputFormat != null) {
        inPart.getSd().setInputFormat(inputFormat);
      }
      if (outputFormat != null) {
        inPart.getSd().setOutputFormat(outputFormat);
      }
      if (numBuckets != -1) {
        inPart.getSd().setNumBuckets(numBuckets);
      }
      if (cols != null) {
        inPart.getSd().setCols(cols);
      }
      if (serializationLib != null) {
          inPart.getSd().getSerdeInfo().setSerializationLib(serializationLib);
      }
      if (serdeParams != null) {
        inPart.getSd().getSerdeInfo().setParameters(serdeParams);
      }
      if (bucketCols != null) {
        inPart.getSd().setBucketCols(bucketCols);
      }
      if (sortCols != null) {
        inPart.getSd().setSortCols(sortCols);
      }
      
      return new Partition(tbl, inPart);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }
}