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
package org.gbif.occurrence.download.file.common;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.api.vocabulary.License;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.download.action.DownloadWorkflowModule;
import org.gbif.occurrence.download.util.RegistryClientUtil;
import org.gbif.utils.file.properties.PropertiesUtil;

import java.io.IOException;
import java.util.Properties;

import com.google.common.base.Preconditions;

/**
 *
 * Oozie action persists occurrence downloads count information and license to registry.
 *
 */
public class DownloadCountAndLicensePersistence {

  public static void main(String[] args) throws IOException{
    String countPath = Preconditions.checkNotNull(args[0]);
    String downloadKey = Preconditions.checkNotNull(args[1]);
    String license = Preconditions.checkNotNull(args[2]);
    DwcTerm coreTerm = DwcTerm.valueOf(Preconditions.checkNotNull(args[3]));

    Properties properties = PropertiesUtil.loadProperties(DownloadWorkflowModule.CONF_FILE);
    String nameNode = properties.getProperty(DownloadWorkflowModule.DefaultSettings.NAME_NODE_KEY);
    String registryWsURL = properties.getProperty(DownloadWorkflowModule.DefaultSettings.REGISTRY_URL_KEY);
    String registryUser = properties.getProperty(DownloadWorkflowModule.DefaultSettings.DOWNLOAD_USER_KEY);
    String registryPassword = properties.getProperty(DownloadWorkflowModule.DefaultSettings.DOWNLOAD_PASSWORD_KEY);

    RegistryClientUtil registryClientUtil = new RegistryClientUtil(registryUser, registryPassword, registryWsURL);
    OccurrenceDownloadService occurrenceDownloadService = registryClientUtil.occurrenceDownloadService(coreTerm);
    // persists download count information.
    DownloadCount.persist(downloadKey, DownloadFileUtils.readCount(nameNode, countPath), occurrenceDownloadService);
    // persist license information.
    Download download = occurrenceDownloadService.get(downloadKey);
    download.setLicense(License.valueOf(license));
    occurrenceDownloadService.update(download);
  }
}
