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
package org.gbif.occurrence.download.action;

import org.gbif.occurrence.download.file.DownloadAggregator;
import org.gbif.occurrence.download.file.Result;

import java.util.List;

/**
 * Combine the parts created by actor and combine them into single file.
 */
public class NotSupportedDownloadAggregator implements DownloadAggregator {

  /**
   * Collects the results of each job.
   * Iterates over the list of futures to collect individual results.
   */
  @Override
  public void aggregate(List<Result> results) {
    throw new IllegalStateException("Downloads of this format not supported as small downloads.");
  }
}
