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

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.exec.DDLTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.CreateTableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.Privilege;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.mapreduce.HCatStorageHandler;
import org.apache.hcatalog.rcfile.RCFileInputDriver;
import org.apache.hcatalog.rcfile.RCFileOutputDriver;

final class CreateTableHook extends HCatSemanticAnalyzerBase {

    private String inStorageDriver, outStorageDriver, tableName, loader, storer;

    @Override
    public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context,
            ASTNode ast) throws SemanticException {

        Hive db;
        try {
            db = context.getHive();
        } catch (HiveException e) {
            throw new SemanticException(
                    "Couldn't get Hive DB instance in semantic analysis phase.",
                    e);
        }

        // Analyze and create tbl properties object
        int numCh = ast.getChildCount();

        String inputFormat = null, outputFormat = null;
        tableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast
                .getChild(0));

        for (int num = 1; num < numCh; num++) {
            ASTNode child = (ASTNode) ast.getChild(num);

            switch (child.getToken().getType()) {

                case HiveParser.TOK_QUERY: // CTAS
                    throw new SemanticException(
                            "Operation not supported. Create table as " +
                            "Select is not a valid operation.");

                case HiveParser.TOK_TABLEBUCKETS:
                    throw new SemanticException(
                            "Operation not supported. HCatalog doesn't " +
                            "allow Clustered By in create table.");

                case HiveParser.TOK_TBLSEQUENCEFILE:
                    throw new SemanticException(
                            "Operation not supported. HCatalog doesn't support " +
                            "Sequence File by default yet. "
                             + "You may specify it through INPUT/OUTPUT storage drivers.");

                case HiveParser.TOK_TBLTEXTFILE:
                    inputFormat      = org.apache.hadoop.mapred.TextInputFormat.class.getName();
                    outputFormat     = org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat.class.getName();
                    inStorageDriver  = org.apache.hcatalog.pig.drivers.LoadFuncBasedInputDriver.class.getName();
                    outStorageDriver = org.apache.hcatalog.pig.drivers.StoreFuncBasedOutputDriver.class.getName();
                    loader = HCatConstants.HCAT_PIG_STORAGE_CLASS;
                    storer = HCatConstants.HCAT_PIG_STORAGE_CLASS;

                    break;

                case HiveParser.TOK_LIKETABLE:

                    String likeTableName;
                    if (child.getChildCount() > 0
                            && (likeTableName = BaseSemanticAnalyzer
                                    .getUnescapedName((ASTNode) ast.getChild(0))) != null) {

                        throw new SemanticException(
                                "Operation not supported. CREATE TABLE LIKE is not supported.");
                        // Map<String, String> tblProps;
                        // try {
                        // tblProps =
                        // db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME,
                        // likeTableName).getParameters();
                        // } catch (HiveException he) {
                        // throw new SemanticException(he);
                        // }
                        // if(!(tblProps.containsKey(InitializeInput.HOWL_ISD_CLASS)
                        // &&
                        // tblProps.containsKey(InitializeInput.HOWL_OSD_CLASS))){
                        // throw new
                        // SemanticException("Operation not supported. Table "+likeTableName+" should have been created through HCat. Seems like its not.");
                        // }
                        // return ast;
                    }
                    break;

                case HiveParser.TOK_IFNOTEXISTS:
                    try {
                        List<String> tables = db.getTablesByPattern(tableName);
                        if (tables != null && tables.size() > 0) { // table
                                                                   // exists
                            return ast;
                        }
                    } catch (HiveException e) {
                        throw new SemanticException(e);
                    }
                    break;

                case HiveParser.TOK_TABLEPARTCOLS:
                    List<FieldSchema> partCols = BaseSemanticAnalyzer
                            .getColumns((ASTNode) child.getChild(0), false);
                    for (FieldSchema fs : partCols) {
                        if (!fs.getType().equalsIgnoreCase("string")) {
                            throw new SemanticException(
                                    "Operation not supported. HCatalog only " +
                                    "supports partition columns of type string. "
                                            + "For column: "
                                            + fs.getName()
                                            + " Found type: " + fs.getType());
                        }
                    }
                    break;

                case HiveParser.TOK_STORAGEHANDLER:
                    String storageHandler = BaseSemanticAnalyzer
                            .unescapeSQLString(child.getChild(0).getText());
                    if (org.apache.commons.lang.StringUtils
                            .isNotEmpty(storageHandler)) {
                        return ast;
                    }

                    break;

                case HiveParser.TOK_TABLEFILEFORMAT:
                    if (child.getChildCount() < 4) {
                        throw new SemanticException(
                                "Incomplete specification of File Format. " +
                                "You must provide InputFormat, OutputFormat, " +
                                "InputDriver, OutputDriver.");
                    }
                    inputFormat = BaseSemanticAnalyzer.unescapeSQLString(child
                            .getChild(0).getText());
                    outputFormat = BaseSemanticAnalyzer.unescapeSQLString(child
                            .getChild(1).getText());
                    inStorageDriver = BaseSemanticAnalyzer
                            .unescapeSQLString(child.getChild(2).getText());
                    outStorageDriver = BaseSemanticAnalyzer
                            .unescapeSQLString(child.getChild(3).getText());
                    break;

                case HiveParser.TOK_TBLRCFILE:
                    inputFormat = RCFileInputFormat.class.getName();
                    outputFormat = RCFileOutputFormat.class.getName();
                    inStorageDriver = RCFileInputDriver.class.getName();
                    outStorageDriver = RCFileOutputDriver.class.getName();
                    break;

            }
        }

        if (inputFormat == null || outputFormat == null
                || inStorageDriver == null || outStorageDriver == null) {
            throw new SemanticException(
                    "STORED AS specification is either incomplete or incorrect.");
        }

        return ast;
    }

    @Override
    public void postAnalyze(HiveSemanticAnalyzerHookContext context,
            List<Task<? extends Serializable>> rootTasks)
            throws SemanticException {

        if (rootTasks.size() == 0) {
            // There will be no DDL task created in case if its CREATE TABLE IF
            // NOT EXISTS
            return;
        }
        CreateTableDesc desc = ((DDLTask) rootTasks.get(rootTasks.size() - 1))
                .getWork().getCreateTblDesc();
        if (desc == null) {
          // Desc will be null if its CREATE TABLE LIKE. Desc will be
          // contained in CreateTableLikeDesc. Currently, HCat disallows CTLT in
          // pre-hook. So, desc can never be null.
          return;
        }
        Map<String, String> tblProps = desc.getTblProps();
        if (tblProps == null) {
            // tblProps will be null if user didnt use tblprops in his CREATE
            // TABLE cmd.
            tblProps = new HashMap<String, String>();

        }

        // first check if we will allow the user to create table.
        String storageHandler = desc.getStorageHandler();
        if (StringUtils.isEmpty(storageHandler)) {
            tblProps.put(HCatConstants.HCAT_ISD_CLASS, inStorageDriver);
            tblProps.put(HCatConstants.HCAT_OSD_CLASS, outStorageDriver);

        } else {
            try {
                HCatStorageHandler storageHandlerInst = HCatUtil
                        .getStorageHandler(context.getConf(),
                                                     desc.getStorageHandler(),
                                                     desc.getSerName(),
                                                     desc.getInputFormat(),
                                                     desc.getOutputFormat());
                //Authorization checks are performed by the storageHandler.getAuthorizationProvider(), if  
                //StorageDelegationAuthorizationProvider is used.
            } catch (IOException e) {
                throw new SemanticException(e);
            }
        }
        if (loader!=null) {
            tblProps.put(HCatConstants.HCAT_PIG_LOADER, loader);
        }
        if (storer!=null) {
            tblProps.put(HCatConstants.HCAT_PIG_STORER, storer);
        }

        if (desc != null) {
          try {
            Table table = context.getHive().newTable(desc.getTableName());
            if (desc.getLocation() != null) {
              table.setDataLocation(new Path(desc.getLocation()).toUri());
            }
            if (desc.getStorageHandler() != null) {
              table.setProperty(
                org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_STORAGE,
                desc.getStorageHandler());
            }
            for (Map.Entry<String, String> prop : tblProps.entrySet()) {
              table.setProperty(prop.getKey(), prop.getValue());
            }
            for (Map.Entry<String, String> prop : desc.getSerdeProps().entrySet()) {
              table.setSerdeParam(prop.getKey(), prop.getValue());
            }
            //TODO: set other Table properties as needed
  
            //authorize against the table operation so that location permissions can be checked if any
            
            if (HiveConf.getBoolVar(context.getConf(),
                HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED)) {
              authorize(table, Privilege.CREATE);
            }
          } catch (HiveException ex) {
            throw new SemanticException(ex);
          }
        }

        desc.setTblProps(tblProps);
        context.getConf().set(HCatConstants.HCAT_CREATE_TBL_NAME, tableName);
    }
}
