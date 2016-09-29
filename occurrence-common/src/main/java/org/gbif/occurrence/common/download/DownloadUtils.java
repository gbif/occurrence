package org.gbif.occurrence.common.download;

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

  public static final String ISO_8601_FORMAT = "yyyy-MM-dd'T'HH:mm'Z'";

  public static final String DOWNLOAD_LINK_FMT = "occurrence/download/request/%s.zip";

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
  public static String downloadLink(String baseUrl, String downloadId) {
    return concatUrlPaths(baseUrl, String.format(DOWNLOAD_LINK_FMT, downloadId));
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
