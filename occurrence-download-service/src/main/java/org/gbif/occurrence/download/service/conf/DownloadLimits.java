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
package org.gbif.occurrence.download.service.conf;

import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.occurrence.query.PredicateCounter;
import org.gbif.occurrence.query.PredicateGeometryPointCounter;

import java.util.Iterator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.base.Splitter;

@Component
public class DownloadLimits {

  /**
   * Amount of downloads that an user can execute simultaneously under certain amount of global downloads.
   */
  public static class Limit {

    private final int maxUserDownloads;

    private final int globalExecutingDownloads;

    public Limit(int maxUserDownloads, int globalExecutingDownloads) {
      this.globalExecutingDownloads = globalExecutingDownloads;
      this.maxUserDownloads = maxUserDownloads;
    }

    public int getMaxUserDownloads() {
      return maxUserDownloads;
    }

    public int getGlobalExecutingDownloads() {
      return globalExecutingDownloads;
    }

    public boolean violatesLimit(int userDownloads, int globalDownloads) {
      return globalDownloads >= globalExecutingDownloads && userDownloads >= maxUserDownloads;
    }
  }

  private static final Splitter COMMA_SPLITTER = Splitter.on(',');
  private final PredicateCounter predicateCounter = new PredicateCounter();
  private final PredicateGeometryPointCounter predicateGeometryPointCounter = new PredicateGeometryPointCounter();

  private final int maxUserDownloads;
  private final Limit softLimit;
  private final Limit hardLimit;

  private final int maxPoints;
  private final int maxPredicates;

  public DownloadLimits(int maxUserDownloads, Limit softLimit, Limit hardLimit, int maxPoints, int maxPredicates) {
    this.maxUserDownloads = maxUserDownloads;
    this.softLimit = softLimit;
    this.hardLimit = hardLimit;
    this.maxPoints = maxPoints;
    this.maxPredicates = maxPredicates;
  }

  @Autowired
  public DownloadLimits(@Value("${occurrence.download.max_user_downloads}") int maxUserDownloads,
                        @Value("${occurrence.download.downloads_soft_limit}") String softLimit,
                        @Value("${occurrence.download.downloads_hard_limit}") String hardLimit,
                        @Value("${occurrence.download.downloads_max_points}") int maxPoints,
                        @Value("${occurrence.download.downloads_max_predicates}") int maxPredicates) {

    Iterator<String> softLimits = COMMA_SPLITTER.split(softLimit).iterator();
    Iterator<String> hardLimits = COMMA_SPLITTER.split(hardLimit).iterator();
    this.maxUserDownloads = maxUserDownloads;
    this.softLimit = new Limit(Integer.parseInt(softLimits.next()), Integer.parseInt(softLimits.next()));
    this.hardLimit = new Limit(Integer.parseInt(hardLimits.next()), Integer.parseInt(hardLimits.next()));
    this.maxPoints = maxPoints;
    this.maxPredicates = maxPredicates;
  }

  public int getMaxUserDownloads() {
    return maxUserDownloads;
  }

  public Limit getSoftLimit() {
    return softLimit;
  }

  public Limit getHardLimit() {
    return hardLimit;
  }

  public boolean violatesLimits(int userDownloads, int globalDownloads) {
    return getSoftLimit().violatesLimit(userDownloads, globalDownloads)
           && getHardLimit().violatesLimit(userDownloads, globalDownloads);
  }

  public String violatesFilterRules(Predicate p) {
    // Find any WithinPredicates, and check the geometry is not too large.
    Integer points = predicateGeometryPointCounter.count(p);
    if (points > maxPoints) {
      return "The geometry (or geometries) contains "+points+" points.  The maximum is "+maxPoints+".";
    }

    // Count the total expanded number of predicates.
    Integer count = predicateCounter.count(p);
    if (count > maxPredicates) {
      return "The request contains "+count+" predicate items.  The maximum is "+maxPredicates+".";
    }

    return null;
  }
}
