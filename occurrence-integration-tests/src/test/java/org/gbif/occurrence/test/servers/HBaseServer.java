/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

    TEST_UTIL.getConfiguration().set("hadoop.home.dir",
      "/tmp/hadoop-" + System.getProperty("user.name")
    );
    TEST_UTIL.getConfiguration().set("fs.hdfs.impl",
      org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
    );
    TEST_UTIL.getConfiguration().set("fs.file.impl",
      org.apache.hadoop.fs.LocalFileSystem.class.getName()
    );
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
