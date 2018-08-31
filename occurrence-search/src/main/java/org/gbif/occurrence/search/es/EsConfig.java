package org.gbif.occurrence.search.es;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.gbif.utils.file.properties.PropertiesUtil;

import javax.annotation.Nullable;
import java.util.Properties;

public class EsConfig {

  private static final String HOSTS_PROP = "hosts";
  private static final String INDEX_PROP = "index";
  private static final Splitter SPLITTER = Splitter.on(",");

  private final String[] hosts;
  private final String index;

  private EsConfig(String[] hosts, String index) {
    this.hosts = hosts;
    this.index = index;
  }

  public static EsConfig fromProperties(Properties properties, @Nullable String prefix) {
    Properties props;
    if (prefix != null) {
      props = PropertiesUtil.filterProperties(properties, prefix);
    } else {
      props = properties;
    }

    String[] hosts =
        Iterables.toArray(
            SPLITTER.omitEmptyStrings().split(props.getProperty(HOSTS_PROP)), String.class);

    String index =  props.getProperty(INDEX_PROP);

    return new EsConfig(hosts, index);
  }

  public String[] getHosts() {
    return hosts;
  }

  public String getIndex() {
    return index;
  }
}
