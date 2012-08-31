/**
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
package org.apache.hcatalog.api;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hcatalog.common.HCatException;

/**
 * The abstract class HCatClient containing APIs for HCatalog DDL commands.
 */
public abstract class HCatClient {

    public enum DROP_DB_MODE { RESTRICT, CASCADE };
    public static final String HCAT_CLIENT_IMPL_CLASS = "hcat.client.impl.class";
    /**
     * Creates an instance of HCatClient.
     *
     * @param conf An instance of configuration.
     * @return An instance of HCatClient.
     * @throws HCatException,ConnectionFailureException
     */
    public static HCatClient create(Configuration conf) throws HCatException,
            ConnectionFailureException {
        HCatClient client = null;
        String className = conf.get(HCAT_CLIENT_IMPL_CLASS,
                HCatClientHMSImpl.class.getName());
        try {
            Class<? extends HCatClient> clientClass = Class.forName(className,
                    true, JavaUtils.getClassLoader()).asSubclass(
                    HCatClient.class);
            client = (HCatClient) clientClass.newInstance();
        } catch (ClassNotFoundException e) {
            throw new HCatException(
                    "ClassNotFoundException while creating client class.", e);
        } catch (InstantiationException e) {
            throw new HCatException(
                    "InstantiationException while creating client class.", e);
        } catch (IllegalAccessException e) {
            throw new HCatException(
                    "IllegalAccessException while creating client class.", e);
        }
        if(client != null){
            client.initialize(conf);
        }
        return client;
    }

    abstract void initialize(Configuration conf) throws HCatException,ConnectionFailureException;

    /**
     * Get all existing databases that match the given
     * pattern. The matching occurs as per Java regular expressions
     *
     * @param pattern  java re pattern
     * @return list of database names
     * @throws HCatException,ConnectionFailureException
     */
    public abstract List<String> listDatabaseNamesByPattern(String pattern)
            throws HCatException, ConnectionFailureException;

    /**
     * Gets the database.
     *
     * @param dbName The name of the database.
     * @return An instance of HCatDatabaseInfo.
     * @throws HCatException,ConnectionFailureException
     */
    public abstract HCatDatabase getDatabase(String dbName) throws HCatException,ConnectionFailureException;

    /**
     * Creates the database.
     *
     * @param dbInfo An instance of HCatCreateDBDesc.
     * @throws HCatException,ConnectionFailureException
     */
    public abstract void createDatabase(HCatCreateDBDesc dbInfo)
            throws HCatException,ConnectionFailureException;

    /**
     * Drops a database.
     *
     * @param dbName The name of the database to delete.
     * @param ifExists Hive returns an error if the database specified does not exist,
     *                 unless ifExists is set to true.
     * @param mode This is set to either "restrict" or "cascade". Restrict will
     *             remove the schema if all the tables are empty. Cascade removes
     *             everything including data and definitions.
     * @throws HCatException,ConnectionFailureException
     */
    public abstract void dropDatabase(String dbName, boolean ifExists,
            DROP_DB_MODE mode) throws HCatException, ConnectionFailureException;

    /**
     * Returns all existing tables from the specified database which match the given
     * pattern. The matching occurs as per Java regular expressions.
     * @param dbName
     * @param tablePattern
     * @return list of table names
     * @throws HCatException,ConnectionFailureException
     */
    public abstract List<String> listTableNamesByPattern(String dbName, String tablePattern)
            throws HCatException,ConnectionFailureException;

    /**
     * Gets the table.
     *
     * @param dbName The name of the database.
     * @param tableName The name of the table.
     * @return An instance of HCatTableInfo.
     * @throws HCatException,ConnectionFailureException
     */
    public abstract HCatTable getTable(String dbName, String tableName)
            throws HCatException,ConnectionFailureException;

    /**
     * Creates the table.
     *
     * @param createTableDesc An instance of HCatCreateTableDesc class.
     * @throws HCatException,ConnectionFailureException
     */
    public abstract void createTable(HCatCreateTableDesc createTableDesc)
            throws HCatException,ConnectionFailureException;

    /**
     * Creates the table like an existing table.
     *
     * @param dbName The name of the database.
     * @param existingTblName The name of the existing table.
     * @param newTableName The name of the new table.
     * @param ifNotExists If true, then error related to already table existing is skipped.
     * @param isExternal Set to "true", if table has be created at a different
     *                   location other than default.
     * @param location The location for the table.
     * @throws HCatException,ConnectionFailureException
     */
    public abstract void createTableLike(String dbName, String existingTblName,
            String newTableName, boolean ifNotExists, boolean isExternal,
            String location) throws HCatException,ConnectionFailureException;

    /**
     * Drop table.
     *
     * @param dbName The name of the database.
     * @param tableName The name of the table.
     * @param ifExists Hive returns an error if the database specified does not exist,
     *                 unless ifExists is set to true.
     * @throws HCatException,ConnectionFailureException
     */
    public abstract void dropTable(String dbName, String tableName,
            boolean ifExists) throws HCatException,ConnectionFailureException;

    /**
     * Renames a table.
     *
     * @param dbName The name of the database.
     * @param oldName The name of the table to be renamed.
     * @param newName The new name of the table.
     * @throws HCatException,ConnectionFailureException
     */
    public abstract void renameTable(String dbName, String oldName,
            String newName) throws HCatException, ConnectionFailureException;

    /**
     * Gets all the partitions.
     *
     * @param dbName The name of the database.
     * @param tblName The name of the table.
     * @return A list of partitions.
     * @throws HCatException,ConnectionFailureException
     */
    public abstract List<HCatPartition> getPartitions(String dbName, String tblName)
            throws HCatException,ConnectionFailureException;

    /**
     * Gets the partition.
     *
     * @param dbName The database name.
     * @param tableName The table name.
     * @param partitionSpec The partition specification, {[col_name,value],[col_name2,value2]}.
     * @return An instance of HCatPartitionInfo.
     * @throws HCatException,ConnectionFailureException
     */
    public abstract HCatPartition getPartition(String dbName, String tableName,
            Map<String,String> partitionSpec) throws HCatException,ConnectionFailureException;

    /**
     * Adds the partition.
     *
     * @param partInfo An instance of HCatAddPartitionDesc.
     * @throws HCatException,ConnectionFailureException
     */
    public abstract void addPartition(HCatAddPartitionDesc partInfo)
            throws HCatException, ConnectionFailureException;

    /**
     * Adds a list of partitions.
     *
     * @param partInfoList A list of HCatAddPartitionDesc.
     * @return
     * @throws HCatException,ConnectionFailureException
     */
    public abstract int addPartitions(List<HCatAddPartitionDesc> partInfoList)
            throws HCatException, ConnectionFailureException;

    /**
     * Drops partition.
     *
     * @param dbName The database name.
     * @param tableName The table name.
     * @param partitionSpec The partition specification, {[col_name,value],[col_name2,value2]}.
     * @param ifExists Hive returns an error if the partition specified does not exist, unless ifExists is set to true.
     * @throws HCatException,ConnectionFailureException
     */
    public abstract void dropPartition(String dbName, String tableName,
            Map<String, String> partitionSpec, boolean ifExists)
            throws HCatException, ConnectionFailureException;

    /**
     * List partitions by filter.
     *
     * @param dbName The database name.
     * @param tblName The table name.
     * @param filter The filter string,
     *    for example "part1 = \"p1_abc\" and part2 <= "\p2_test\"". Filtering can
     *    be done only on string partition keys.
     * @return list of partitions
     * @throws HCatException,ConnectionFailureException
     */
    public abstract List<HCatPartition> listPartitionsByFilter(String dbName, String tblName,
            String filter) throws HCatException,ConnectionFailureException;

    /**
     * Mark partition for event.
     *
     * @param dbName The database name.
     * @param tblName The table name.
     * @param partKVs the key-values associated with the partition.
     * @param eventType the event type
     * @throws HCatException,ConnectionFailureException
     */
    public abstract void markPartitionForEvent(String dbName, String tblName,
            Map<String, String> partKVs, PartitionEventType eventType)
            throws HCatException,ConnectionFailureException;

    /**
     * Checks if a partition is marked for event.
     *
     * @param dbName the db name
     * @param tblName the table name
     * @param partKVs the key-values associated with the partition.
     * @param eventType the event type
     * @return true, if is partition marked for event
     * @throws HCatException,ConnectionFailureException
     */
    public abstract boolean isPartitionMarkedForEvent(String dbName, String tblName,
            Map<String, String> partKVs, PartitionEventType eventType)
            throws HCatException,ConnectionFailureException;

    /**
     * Gets the delegation token.
     *
     * @param owner the owner
     * @param renewerKerberosPrincipalName the renewer kerberos principal name
     * @return the delegation token
     * @throws HCatException,ConnectionFailureException
     */
    public abstract String getDelegationToken(String owner,
            String renewerKerberosPrincipalName) throws HCatException,
            ConnectionFailureException;

    /**
     * Renew delegation token.
     *
     * @param tokenStrForm the token string
     * @return the new expiration time
     * @throws HCatException,ConnectionFailureException
     */
    public abstract long renewDelegationToken(String tokenStrForm)
            throws HCatException, ConnectionFailureException;

    /**
     * Cancel delegation token.
     *
     * @param tokenStrForm the token string
     * @throws HCatException,ConnectionFailureException
     */
    public abstract void cancelDelegationToken(String tokenStrForm)
            throws HCatException, ConnectionFailureException;

    /**
     * Close the hcatalog client.
     *
     * @throws HCatException
     */
    public abstract void close() throws HCatException;
}
