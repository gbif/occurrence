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
package org.gbif.occurrence.cli.registry.sync;

import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncCommon {

  private static final Logger LOG = LoggerFactory.getLogger(SyncCommon.class);

  public static final String PROPS_FILE = "registry-sync.properties";

  public static Properties loadProperties() {
    Properties props = new Properties();
    try (InputStream in = SyncCommon.class.getClassLoader().getResourceAsStream(PROPS_FILE)) {
      props.load(in);
    } catch (Exception e) {
      LOG.error("Unable to open registry-sync.properties file - RegistrySync is not initialized", e);
    }

    return props;
  }
}
