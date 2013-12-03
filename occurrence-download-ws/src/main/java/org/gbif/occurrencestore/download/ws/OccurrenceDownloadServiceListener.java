/*
 * Copyright 2012 Global Biodiversity Information Facility (GBIF)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrencestore.download.ws;

import org.gbif.checklistbank.ws.client.guice.ChecklistBankWsClientModule;
import org.gbif.occurrencestore.download.service.OccurrenceDownloadServiceModule;
import org.gbif.registry.ws.client.guice.RegistryWsClientModule;
import org.gbif.user.guice.DrupalMyBatisModule;
import org.gbif.ws.client.guice.SingleUserAuthModule;
import org.gbif.ws.server.guice.GbifServletListener;
import org.gbif.ws.server.guice.WsAuthModule;

import java.util.List;
import java.util.Properties;

import com.google.common.collect.Lists;
import com.google.inject.Module;
import org.apache.bval.guice.ValidationModule;

public class OccurrenceDownloadServiceListener extends GbifServletListener {

  private static final String APPLICATION_PROPERTIES = "occurrence-download-ws.properties";

  private static final String RESOURCE_PACKAGE = "org.gbif.occurrencestore.download.ws.resources";

  private static final String DOWNLOAD_USER_KEY = "occurrence-download.ws.username";
  private static final String DOWNLOAD_PASSWORD_KEY = "occurrence-download.ws.password";

  public OccurrenceDownloadServiceListener() {
    super(APPLICATION_PROPERTIES, RESOURCE_PACKAGE, true);
  }

  @Override
  protected List<Module> getModules(Properties properties) {
    List<Module> modules = Lists.newArrayList();
    modules.add(new DrupalMyBatisModule(properties));
    modules.add(new WsAuthModule(properties));
    modules.add(new SingleUserAuthModule(properties.getProperty(DOWNLOAD_USER_KEY),
      properties.getProperty(DOWNLOAD_PASSWORD_KEY)));
    modules.add(new OccurrenceDownloadServiceModule(properties));
    modules.add(new ValidationModule());
    modules.add(new RegistryWsClientModule(properties));
    modules.add(new ChecklistBankWsClientModule(properties, false, true, false));
    return modules;
  }

}
