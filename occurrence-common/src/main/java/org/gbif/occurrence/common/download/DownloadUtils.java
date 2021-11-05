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
package org.gbif.occurrence.common.download;

import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

/**
 * Shared download utilities.
 */
public class DownloadUtils {

  private static final String OOZIE_SUFFIX = "-oozie-oozi-W";
  public static final String DOWNLOAD_ID_PLACEHOLDER = "_DOWNLOAD_ID_";

  public static final String DELIMETERS_MATCH =
    "\\t|\\n|\\r|(?:(?>\\u000D\\u000A)|[\\u000A\\u000B\\u000C\\u000D\\u0085\\u2028\\u2029\\u0000])";

  public static final Pattern DELIMETERS_MATCH_PATTERN = Pattern.compile(DELIMETERS_MATCH);

  public static final DateTimeFormatter ISO_8601_ZONED = DateTimeFormatter.ISO_INSTANT;
  public static final DateTimeFormatter ISO_8601_LOCAL = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

  public static final String DOWNLOAD_LINK_FMT = "occurrence/download/request/%s%s";

  /**
   * Private default constructor.
   */
  private DownloadUtils() {
    throw new UnsupportedOperationException("Can't initialize class");
  }

  /**
   * Adds a path to a base url and makes sure the path / separator only exists once.
   *
   * @param base the base url to add to with or without trailing slash
   * @param path the path to be added without a prefix /
   * @return a string with the form base/path
   */
  public static String concatUrlPaths(String base, String path) {
    StringBuilder sb = new StringBuilder();
    sb.append(base);
    if (!base.endsWith("/")) {
      sb.append('/');
    }
    sb.append(path);
    return sb.toString();
  }

  /**
   * Creates a URL pointing to the download file.
   */
  public static String downloadLink(String baseUrl, String downloadId, String extension) {
    return concatUrlPaths(baseUrl, String.format(DOWNLOAD_LINK_FMT, downloadId, extension));
  }

  public static String downloadToWorkflowId(String downloadId) {
    return downloadId + OOZIE_SUFFIX;
  }

  public static String workflowToDownloadId(String workflowId) {
    if (workflowId.contains(OOZIE_SUFFIX)) {
      return workflowId.replace(OOZIE_SUFFIX, "");
    }
    throw new IllegalArgumentException("WorkflowId given in unknown format: " + workflowId);
  }

}
