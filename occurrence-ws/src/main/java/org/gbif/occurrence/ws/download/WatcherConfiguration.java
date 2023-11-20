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

import java.util.Collections;
import java.util.Map;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Simple configuration class for the K8 Stackable watcher.
 */
@Data
@NoArgsConstructor
public class WatcherConfiguration {
  private Map<String,String> labelSelectors = Collections.emptyMap();
  private Map<String,String> fieldSelectors = Collections.emptyMap();
  private String nameSelector;
}
