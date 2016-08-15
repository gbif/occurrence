package org.gbif.occurrence.ws;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.checklistbank.ws.client.guice.ChecklistBankWsClientModule;
import org.gbif.drupal.guice.DrupalMyBatisModule;
import org.gbif.occurrence.download.service.OccurrenceDownloadServiceModule;
import org.gbif.occurrence.persistence.guice.OccurrencePersistenceModule;
import org.gbif.occurrence.query.TitleLookupModule;
import org.gbif.occurrence.search.guice.OccurrenceSearchModule;
import org.gbif.occurrence.ws.resources.FeaturedOccurrenceReader;
import org.gbif.registry.ws.client.guice.RegistryWsClientModule;
import org.gbif.service.guice.PrivateServiceModule;
import org.gbif.utils.file.properties.PropertiesUtil;
import org.gbif.ws.app.ConfUtils;
import org.gbif.ws.client.guice.SingleUserAuthModule;
import org.gbif.ws.mixin.LicenseMixin;
import org.gbif.ws.server.guice.GbifServletListener;
import org.gbif.ws.server.guice.WsAuthModule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.apache.bval.guice.ValidationModule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class OccurrenceWsListener extends GbifServletListener {

  private static final String DOWNLOAD_USER_KEY = "occurrence.download.ws.username";
  private static final String DOWNLOAD_PASSWORD_KEY = "occurrence.download.ws.password";
  private static final String APP_CONF_FILE = "occurrence.properties";

  /**
   * Wires up the featured module to be able to access the HBase table.
   */
  private static class FeaturedModule extends PrivateServiceModule {

    private static final String PREFIX = "occurrence.db.";

    public FeaturedModule(Properties properties) {
      super(PREFIX, properties);
    }

    @SuppressWarnings("unused")
    @Provides
    @Named("featured_table_pool")
    @Singleton
    public Connection provideHTablePool(@Named("max_connection_pool") Integer maxConnectionPool) throws IOException {
      return ConnectionFactory.createConnection(HBaseConfiguration.create());
    }

    @Override
    protected void configureService() {
      bind(FeaturedOccurrenceReader.class);
      expose(FeaturedOccurrenceReader.class);
    }
  }

  public OccurrenceWsListener() throws IOException {
    super(PropertiesUtil.readFromFile(ConfUtils.getAppConfFile(APP_CONF_FILE)), "org.gbif.occurrence.ws", true);
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
    modules.add(new ValidationModule());
    modules.add(new DrupalMyBatisModule(properties));
    modules.add(new OccurrencePersistenceModule(properties));
    modules.add(new OccurrenceSearchModule(properties));
    modules.add(new OccurrenceDownloadServiceModule(properties));
    modules.add(new TitleLookupModule(true, properties.getProperty("api.url")));
    modules.add(new FeaturedModule(properties));
    return modules;
  }

  @Override
  protected Map<Class<?>, Class<?>> getMixIns() {
    return ImmutableMap.<Class<?>, Class<?>>of(Occurrence.class, LicenseMixin.class);
  }
}
