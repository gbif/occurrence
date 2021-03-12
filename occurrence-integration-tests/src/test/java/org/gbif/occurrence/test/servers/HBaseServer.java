package org.gbif.occurrence.test.servers;


import java.io.IOException;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

public class HBaseServer implements DisposableBean, InitializingBean {


  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Override
  public void destroy() throws Exception {
    stop();
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    start();
  }

  public void stop() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  public void start() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.master.port", HBaseTestingUtility.randomFreePort());
    TEST_UTIL
      .getConfiguration()
      .setInt("hbase.master.info.port", HBaseTestingUtility.randomFreePort());
    TEST_UTIL
      .getConfiguration()
      .setInt("hbase.regionserver.port", HBaseTestingUtility.randomFreePort());
    TEST_UTIL
      .getConfiguration()
      .setInt("hbase.regionserver.info.port", HBaseTestingUtility.randomFreePort());
    TEST_UTIL.startMiniCluster(1);
  }

  public HBaseTestingUtility getHBaseTestingUtility() {
    return TEST_UTIL;
  }

  public Connection getConnection() throws IOException  {
    return ConnectionFactory.createConnection(TEST_UTIL.getMiniHBaseCluster().getConfiguration());
  }

  public String getZKClusterKey() {
    return String.format("127.0.0.1:%d:%s", TEST_UTIL.getZkCluster().getClientPort(),
                         TEST_UTIL.getConfiguration().get(HConstants.ZOOKEEPER_ZNODE_PARENT));
  }


}
