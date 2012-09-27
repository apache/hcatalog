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

package org.apache.hcatalog.common;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

/**
 * HCatContext provides global access to configuration data. It uses a reference to the
 * job configuration so that settings are automatically passed to the backend by the
 * MR framework.
 */
public class HCatContext {

    private static final HCatContext hCatContext = new HCatContext();

    private Configuration conf = null;

    private HCatContext() {
    }

    /**
     * Setup the HCatContext as a reference to the given configuration. Keys
     * exclusive to an existing config are set in the new conf.
     */
    public static synchronized HCatContext setupHCatContext(Configuration newConf) {
        Preconditions.checkNotNull(newConf, "HCatContext must not have a null conf.");

        if (hCatContext.conf == null) {
            hCatContext.conf = newConf;
            return hCatContext;
        }

        if (hCatContext.conf != newConf) {
            for (Map.Entry<String, String> entry : hCatContext.conf) {
                if (newConf.get(entry.getKey()) == null) {
                    newConf.set(entry.getKey(), entry.getValue());
                }
            }
            hCatContext.conf = newConf;
        }
        return hCatContext;
    }

    public static HCatContext getInstance() {
        return hCatContext;
    }

    public Optional<Configuration> getConf() {
        return Optional.fromNullable(conf);
    }
}
