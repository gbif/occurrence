package org.gbif.occurrence.search.es;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.gbif.utils.file.properties.PropertiesUtil;

import javax.annotation.Nullable;
import java.util.Properties;

public class EsConfig {

  private static final String HOSTS_PROP = "hosts";
  private static final String INDEX_PROP = "index";
  private static final String CONNECT_TIMEOUT_PROP = "connect_timeout";
  private static final String SOCKET_TIMEOUT_PROP = "socket_timeout";
  private static final Splitter SPLITTER = Splitter.on(",");

  private final String[] hosts;
  private final String index;
  private final int connectTimeout;
  private final int socketTimeout;

  private EsConfig(String[] hosts, String index, int connectTimeout, int socketTimeout) {
    this.hosts = hosts;
    this.index = index;
    this.connectTimeout = connectTimeout;
    this.socketTimeout = socketTimeout;
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
    int connectTimeout = Integer.parseInt(props.getProperty(CONNECT_TIMEOUT_PROP));
    int socketTimeout = Integer.parseInt(props.getProperty(SOCKET_TIMEOUT_PROP));

    return new EsConfig(hosts, index, connectTimeout, socketTimeout);
  }

  public String[] getHosts() {
    return hosts;
  }

  public String getIndex() {
    return index;
  }

  public int getConnectTimeout() {
    return connectTimeout;
  }

  public int getSocketTimeout() {
    return socketTimeout;
  }
}
