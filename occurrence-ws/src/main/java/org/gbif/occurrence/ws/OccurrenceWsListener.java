package org.gbif.occurrence.ws;

import org.gbif.checklistbank.ws.client.guice.ChecklistBankWsClientModule;
import org.gbif.identity.inject.IdentityAccessModule;
import org.gbif.occurrence.download.service.OccurrenceDownloadServiceModule;
import org.gbif.occurrence.persistence.guice.OccurrencePersistenceModule;
import org.gbif.occurrence.query.TitleLookupModule;
import org.gbif.occurrence.search.guice.OccurrenceSearchModule;
import org.gbif.registry.ws.client.guice.RegistryWsClientModule;
import org.gbif.utils.file.properties.PropertiesUtil;
import org.gbif.ws.app.ConfUtils;
import org.gbif.ws.client.guice.SingleUserAuthModule;
import org.gbif.ws.mixin.Mixins;
import org.gbif.ws.server.filter.IdentityFilter;
import org.gbif.ws.server.guice.GbifServletListener;
import org.gbif.ws.server.guice.WsAuthModule;
import org.gbif.ws.server.guice.WsJerseyModuleConfiguration;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.bval.guice.ValidationModule;

public class OccurrenceWsListener extends GbifServletListener {

  private static final String DOWNLOAD_USER_KEY = "occurrence.download.ws.username";
  private static final String DOWNLOAD_PASSWORD_KEY = "occurrence.download.ws.password";
  private static final String APP_CONF_FILE = "occurrence.properties";

  private static final String PACKAGES = "org.gbif.occurrence.ws";

  public OccurrenceWsListener() throws IOException {
    super(PropertiesUtil.readFromFile(ConfUtils.getAppConfFile(APP_CONF_FILE)),
            new WsJerseyModuleConfiguration()
                    .resourcePackages(PACKAGES)
                    .useAuthenticationFilter(IdentityFilter.class));
  }

  @Override
  @VisibleForTesting
  protected Injector getInjector() {
    return super.getInjector();
  }

  @Override
  protected List<Module> getModules(Properties properties) {
    List<Module> modules = Lists.newArrayList();
    // client stuff
    modules.add(new SingleUserAuthModule(properties.getProperty(DOWNLOAD_USER_KEY),
                                         properties.getProperty(DOWNLOAD_PASSWORD_KEY)));
    modules.add(new RegistryWsClientModule(properties));
    modules.add(new ChecklistBankWsClientModule(properties));
    // others
    modules.add(new WsAuthModule(properties));
    modules.add(new IdentityAccessModule(properties));
    modules.add(new ValidationModule());
    modules.add(new OccurrencePersistenceModule(properties));
    modules.add(new OccurrenceSearchModule(properties));
    modules.add(new OccurrenceDownloadServiceModule(properties));
    modules.add(new TitleLookupModule(true, properties.getProperty("api.url")));
    return modules;
  }

  @Override
  protected Map<Class<?>, Class<?>> getMixIns() {
    return Mixins.getPredefinedMixins();
  }
}
