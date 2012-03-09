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

import java.io.IOException;

import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.mapred.JobConf;

public class HBaseMapredUtil {

    private HBaseMapredUtil() {
    }

    /**
     * Get delegation token from hbase and add it to JobConf
     * @param job
     * @throws IOException
     */
    public static void addHBaseDelegationToken(JobConf job) throws IOException {
        if (User.isHBaseSecurityEnabled(job)) {
            try {
                User.getCurrent().obtainAuthTokenForJob(job);
            } catch (InterruptedException e) {
                throw new IOException("Error while obtaining hbase delegation token", e);
            }
        }
    }

}
