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
package org.gbif.occurrence.downloads.launcher.services.launcher.oozie;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** This class contains static fields with the expected parameters by the download workflows. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DownloadWorkflowParameters {

  // String pattern to the shared lib of oozie workflow of specific environment.
  public static final String WORKFLOWS_PATH_FMT = "/%s-download-workflows-%s/";

  public static final String DOWNLOAD_WORKFLOW_PATH_FMT = WORKFLOWS_PATH_FMT + "download-workflow";

  public static final String WORKFLOWS_LIB_PATH_FMT = WORKFLOWS_PATH_FMT + "lib/";
  public static final String DOWNLOAD_FORMAT = "download_format";

  public static final String CORE_TERM_NAME = "core_term_name";
  public static final String TABLE_NAME = "table_name";
  public static final String GBIF_FILTER = "gbif_filter"; // Filter/Predicate/SQL
  public static final String NOTIFICATION_PROPERTY = "gbif_notification_addresses";
  public static final String USER_PROPERTY = "gbif_user";
  public static final String DOWNLOAD_KEY = "download_key";
}
