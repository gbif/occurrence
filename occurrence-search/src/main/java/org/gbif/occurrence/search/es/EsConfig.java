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
  private static final String SNIFF_INTERVAL_PROP = "sniff_interval";
  private static final String SNIFF_AFTER_FAILURE_DELAY_PROP = "sniff_after_failure_delay";
  private static final Splitter SPLITTER = Splitter.on(",");

  private final String[] hosts;
  private final String index;
  private final int connectTimeout;
  private final int socketTimeout;
  private final int sniffInterval;
  private final int sniffAfterFailureDelay;

  private EsConfig(
      String[] hosts,
      String index,
      int connectTimeout,
      int socketTimeout,
      int sniffInterval,
      int sniffAfterFailureDelay) {
    this.hosts = hosts;
    this.index = index;
    this.connectTimeout = connectTimeout;
    this.socketTimeout = socketTimeout;
    this.sniffInterval = sniffInterval;
    this.sniffAfterFailureDelay = sniffAfterFailureDelay;
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
    int sniffInterval = Integer.parseInt(props.getProperty(SNIFF_INTERVAL_PROP));
    int sniffAfterFailureDelay = Integer.parseInt(props.getProperty(SNIFF_AFTER_FAILURE_DELAY_PROP));

    return new EsConfig(hosts, index, connectTimeout, socketTimeout, sniffInterval, sniffAfterFailureDelay);
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

  public int getSniffInterval() {
    return sniffInterval;
  }

  public int getSniffAfterFailureDelay() {
    return sniffAfterFailureDelay;
  }
}
