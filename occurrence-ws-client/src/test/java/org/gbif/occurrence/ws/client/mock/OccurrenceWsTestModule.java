package org.gbif.occurrence.ws.client.mock;


import org.gbif.ws.server.guice.GbifServletListener;

import java.util.List;
import java.util.Properties;

import com.google.common.collect.Lists;
import com.google.inject.Module;

public class OccurrenceWsTestModule extends GbifServletListener {

  public OccurrenceWsTestModule() {
    super("occurrence-test.properties", "org.gbif.occurrence.ws", false);
  }

  @Override
  protected List<Module> getModules(Properties properties) {
    List<Module> modules = Lists.newArrayList();
    modules.add(new OccurrencePersistenceMockModule());
    return modules;
  }

}
