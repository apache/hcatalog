package org.apache.hcatalog.mapreduce;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.pig.PigServer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Simplify writing HCatalog tests that require a HiveMetaStore.
 */
public class HCatBaseTest {
  protected static final Logger LOG = LoggerFactory.getLogger(HCatBaseTest.class);
  protected static final String TEST_DATA_DIR = System.getProperty("user.dir") +
      "/build/test/data/" + HCatBaseTest.class.getCanonicalName();
  protected static final String TEST_WAREHOUSE_DIR = TEST_DATA_DIR + "/warehouse";

  protected HiveConf hiveConf = null;
  protected Driver driver = null;
  protected HiveMetaStoreClient client = null;

  @BeforeClass
  public static void setUpTestDataDir() throws Exception {
    LOG.info("Using warehouse directory " + TEST_WAREHOUSE_DIR);
    File f = new File(TEST_WAREHOUSE_DIR);
    if (f.exists()) {
      FileUtil.fullyDelete(f);
    }
    Assert.assertTrue(new File(TEST_WAREHOUSE_DIR).mkdirs());
  }

  @Before
  public void setUp() throws Exception {
    if (driver == null) {
      hiveConf = new HiveConf(this.getClass());
      hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
      hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
      hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
      hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, TEST_WAREHOUSE_DIR);
      driver = new Driver(hiveConf);
      client = new HiveMetaStoreClient(hiveConf);
      SessionState.start(new CliSessionState(hiveConf));
    }
  }

  protected void logAndRegister(PigServer server, String query) throws IOException {
    LOG.info("Registering pig query: " + query);
    server.registerQuery(query);
  }
}
