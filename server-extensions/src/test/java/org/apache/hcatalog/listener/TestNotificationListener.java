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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hcatalog.listener;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.mapreduce.HCatBaseTest;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestNotificationListener extends HCatBaseTest implements MessageListener {

    private List<String> actualMessages = new ArrayList<String>();

    @Before
    public void setUp() throws Exception {
        System.setProperty("java.naming.factory.initial",
                "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
        System.setProperty("java.naming.provider.url",
                "vm://localhost?broker.persistent=false");
        ConnectionFactory connFac = new ActiveMQConnectionFactory(
                "vm://localhost?broker.persistent=false");
        Connection conn = connFac.createConnection();
        conn.start();
        // We want message to be sent when session commits, thus we run in
        // transacted mode.
        Session session = conn.createSession(true, Session.SESSION_TRANSACTED);
        Destination hcatTopic = session
                .createTopic(HCatConstants.HCAT_DEFAULT_TOPIC_PREFIX);
        MessageConsumer consumer1 = session.createConsumer(hcatTopic);
        consumer1.setMessageListener(this);
        Destination tblTopic = session
                .createTopic(HCatConstants.HCAT_DEFAULT_TOPIC_PREFIX + ".mydb.mytbl");
        MessageConsumer consumer2 = session.createConsumer(tblTopic);
        consumer2.setMessageListener(this);
        Destination dbTopic = session
                .createTopic(HCatConstants.HCAT_DEFAULT_TOPIC_PREFIX + ".mydb");
        MessageConsumer consumer3 = session.createConsumer(dbTopic);
        consumer3.setMessageListener(this);

        setUpHiveConf();
        hiveConf.set(ConfVars.METASTORE_EVENT_LISTENERS.varname,
                NotificationListener.class.getName());
        SessionState.start(new CliSessionState(hiveConf));
        driver = new Driver(hiveConf);
        client = new HiveMetaStoreClient(hiveConf);
    }

    @After
    public void tearDown() throws Exception {
        List<String> expectedMessages = Arrays.asList(
                HCatConstants.HCAT_ADD_DATABASE_EVENT,
                HCatConstants.HCAT_ADD_TABLE_EVENT,
                HCatConstants.HCAT_ADD_PARTITION_EVENT,
                HCatConstants.HCAT_PARTITION_DONE_EVENT,
                HCatConstants.HCAT_DROP_PARTITION_EVENT,
                HCatConstants.HCAT_DROP_TABLE_EVENT,
                HCatConstants.HCAT_DROP_DATABASE_EVENT);
        Assert.assertEquals(expectedMessages, actualMessages);
    }

    @Test
    public void testAMQListener() throws Exception {
        driver.run("create database mydb");
        driver.run("use mydb");
        driver.run("create table mytbl (a string) partitioned by (b string)");
        driver.run("alter table mytbl add partition(b='2011')");
        Map<String, String> kvs = new HashMap<String, String>(1);
        kvs.put("b", "2011");
        client.markPartitionForEvent("mydb", "mytbl", kvs,
                PartitionEventType.LOAD_DONE);
        driver.run("alter table mytbl drop partition(b='2011')");
        driver.run("drop table mytbl");
        driver.run("drop database mydb");
    }

    @Override
    public void onMessage(Message msg) {
        String event;
        try {
            event = msg.getStringProperty(HCatConstants.HCAT_EVENT);
            actualMessages.add(event);

            if (event.equals(HCatConstants.HCAT_ADD_DATABASE_EVENT)) {

                Assert.assertEquals("topic://" + HCatConstants.HCAT_DEFAULT_TOPIC_PREFIX, msg
                        .getJMSDestination().toString());
                Assert.assertEquals("mydb",
                        ((Database) ((ObjectMessage) msg).getObject()).getName());
            } else if (event.equals(HCatConstants.HCAT_ADD_TABLE_EVENT)) {

                Assert.assertEquals("topic://hcat.mydb", msg.getJMSDestination().toString());
                Table tbl = (Table) (((ObjectMessage) msg).getObject());
                Assert.assertEquals("mytbl", tbl.getTableName());
                Assert.assertEquals("mydb", tbl.getDbName());
                Assert.assertEquals(1, tbl.getPartitionKeysSize());
            } else if (event.equals(HCatConstants.HCAT_ADD_PARTITION_EVENT)) {

                Assert.assertEquals("topic://hcat.mydb.mytbl", msg.getJMSDestination()
                        .toString());
                Partition part = (Partition) (((ObjectMessage) msg).getObject());
                Assert.assertEquals("mytbl", part.getTableName());
                Assert.assertEquals("mydb", part.getDbName());
                List<String> vals = new ArrayList<String>(1);
                vals.add("2011");
                Assert.assertEquals(vals, part.getValues());
            } else if (event.equals(HCatConstants.HCAT_DROP_PARTITION_EVENT)) {

                Assert.assertEquals("topic://hcat.mydb.mytbl", msg.getJMSDestination()
                        .toString());
                Partition part = (Partition) (((ObjectMessage) msg).getObject());
                Assert.assertEquals("mytbl", part.getTableName());
                Assert.assertEquals("mydb", part.getDbName());
                List<String> vals = new ArrayList<String>(1);
                vals.add("2011");
                Assert.assertEquals(vals, part.getValues());
            } else if (event.equals(HCatConstants.HCAT_DROP_TABLE_EVENT)) {

                Assert.assertEquals("topic://hcat.mydb", msg.getJMSDestination().toString());
                Table tbl = (Table) (((ObjectMessage) msg).getObject());
                Assert.assertEquals("mytbl", tbl.getTableName());
                Assert.assertEquals("mydb", tbl.getDbName());
                Assert.assertEquals(1, tbl.getPartitionKeysSize());
            } else if (event.equals(HCatConstants.HCAT_DROP_DATABASE_EVENT)) {

                Assert.assertEquals("topic://" + HCatConstants.HCAT_DEFAULT_TOPIC_PREFIX, msg
                        .getJMSDestination().toString());
                Assert.assertEquals("mydb",
                        ((Database) ((ObjectMessage) msg).getObject()).getName());
            } else if (event.equals(HCatConstants.HCAT_PARTITION_DONE_EVENT)) {
                Assert.assertEquals("topic://hcat.mydb.mytbl", msg.getJMSDestination()
                        .toString());
                MapMessage mapMsg = (MapMessage) msg;
                assert mapMsg.getString("b").equals("2011");
            } else
                assert false;
        } catch (JMSException e) {
            e.printStackTrace(System.err);
            assert false;
        }
    }
}
