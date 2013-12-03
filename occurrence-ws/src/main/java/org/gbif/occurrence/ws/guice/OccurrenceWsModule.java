package org.gbif.occurrence.ws.guice;


import org.gbif.checklistbank.ws.client.guice.ChecklistBankWsClientModule;
import org.gbif.occurrence.ws.resources.FeaturedOccurrenceReader;
import org.gbif.occurrencestore.persistence.guice.OccurrencePersistenceModule;
import org.gbif.occurrencestore.search.guice.OccurrenceSearchModule;
import org.gbif.registry.ws.client.guice.RegistryWsClientModule;
import org.gbif.service.guice.PrivateServiceModule;
import org.gbif.ws.client.guice.AnonymousAuthModule;
import org.gbif.ws.server.guice.GbifServletListener;

import java.util.List;
import java.util.Properties;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;

public class OccurrenceWsModule extends GbifServletListener {

  /**
   * Wires up the featured module to be able to access the HBase table.
   */
  private static class FeaturedModule extends PrivateServiceModule {

    private static final String PREFIX = "occurrencestore.db.";

    public FeaturedModule(Properties properties) {
      super(PREFIX, properties);
    }

    @SuppressWarnings("unused")
    @Provides
    @Named("featured_table_pool")
    public HTablePool provideHTablePool(@Named("max_connection_pool") Integer maxConnectionPool) {
      return new HTablePool(HBaseConfiguration.create(), maxConnectionPool);
    }

    @Override
    protected void configureService() {
      install(new RegistryWsClientModule(this.getVerbatimProperties()));
      install(new AnonymousAuthModule());
      bind(FeaturedOccurrenceReader.class);
      expose(FeaturedOccurrenceReader.class);
    }
  }

  public OccurrenceWsModule() {
    super("occurrence.properties", "org.gbif.occurrence.ws", false);
  }

  @Override
  @VisibleForTesting
  protected Injector getInjector() {
    return super.getInjector();
  }

  @Override
  protected List<Module> getModules(Properties properties) {
    List<Module> modules = Lists.newArrayList();
    modules.add(new ChecklistBankWsClientModule(properties, false, false, true));
    modules.add(new OccurrencePersistenceModule(properties));
    modules.add(new OccurrenceSearchModule(properties));
    modules.add(new FeaturedModule(properties));
    return modules;
  }


}
