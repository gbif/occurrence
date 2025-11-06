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
package org.gbif.occurrence.persistence.meta;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.EnsurePath;

import lombok.extern.slf4j.Slf4j;

/**
 * An implementation of the Metastore backed by Zookeeper.
 *
 * This uses a Zookeeper Path Cache pattern to watch for changes of the ZK node.
 */
@Slf4j
public class ZKMetastore implements Closeable {

  private final CuratorFramework client;
  private final NodeCache zkNodeCache;

  public ZKMetastore(String zkEnsemble, int retryIntervalMs, String zkNodePath, Consumer<NodeCache> onChange) throws Exception {
    client = CuratorFrameworkFactory.newClient(zkEnsemble, new RetryNTimes(Integer.MAX_VALUE, retryIntervalMs));
    client.start();

    // Ensure node exists so data can be set
    EnsurePath path = new EnsurePath(zkNodePath);
    path.ensure(client.getZookeeperClient());

    // start a node cache, watching for changes and initialised with the current state
    zkNodeCache = new NodeCache(client, zkNodePath, false);

    zkNodeCache.getListenable().addListener(() -> onChange.accept(zkNodeCache));

    log.info("Starting watcher for {}", zkNodePath);
    zkNodeCache.start(true);

    // Force the same onChange action to run immediately after start (ensures update on startup)
    onChange.accept(zkNodeCache);
  }


  @Override
  public void close() throws IOException {
    zkNodeCache.close();
    client.close();
  }
}
