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
package org.gbif.occurrence.ws.download;

import io.kubernetes.client.util.KubeConfig;
import org.gbif.stackable.StackableSparkWatcher;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

/**
 * Simple background thread that listens to status change in downloads.
 */
@Component
public class DownloadsStackableListener implements DisposableBean, InitializingBean {

  private final StackableSparkWatcher watcher;

  public DownloadsStackableListener(KubeConfig kubeConfig, StackableDownloadStatusListener listener, WatcherConfiguration watcherConfiguration) {
    watcher = new StackableSparkWatcher(kubeConfig, listener, watcherConfiguration.getNameSelector());
  }

  @Override
  public void afterPropertiesSet() {
    watcher.start();
  }

  @Override
  public void destroy() {
    watcher.stop();
  }
}
