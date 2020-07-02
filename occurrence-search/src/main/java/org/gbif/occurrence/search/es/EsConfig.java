package org.gbif.occurrence.search.es;

import org.gbif.utils.file.properties.PropertiesUtil;

import java.util.Optional;
import java.util.Properties;
import java.util.function.BiFunction;
import javax.annotation.Nullable;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

public class EsConfig {

  private static final String HOSTS_PROP = "hosts";
  private static final String INDEX_PROP = "index";
  private static final String CONNECT_TIMEOUT_PROP = "connect_timeout";
  private static final String SOCKET_TIMEOUT_PROP = "socket_timeout";
  private static final String SNIFF_INTERVAL_PROP = "sniff_interval";
  private static final String SNIFF_AFTER_FAILURE_DELAY_PROP = "sniff_after_failure_delay";
  private static final Splitter SPLITTER = Splitter.on(",");

  // defaults
  private static final int CONNECT_TIMEOUT_DEFAULT = 6000;
  private static final int SOCKET_TIMEOUT_DEFAULT = 100000;
  private static final int SNIFF_INTERVAL_DEFAULT = 600000;
  private static final int SNIFF_AFTER_FAILURE_DELAY_DEFAULT = 60000;

  private String[] hosts;
  private String index;
  private int connectTimeout = CONNECT_TIMEOUT_DEFAULT;
  private int socketTimeout = SOCKET_TIMEOUT_DEFAULT;
  private int sniffInterval = SNIFF_INTERVAL_DEFAULT;
  private int sniffAfterFailureDelay = SNIFF_AFTER_FAILURE_DELAY_DEFAULT;

  public  EsConfig(){
    super();
  }

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

    BiFunction<String, Integer, Integer> getOrDefault =
        (prop, defaultValue) ->
            Optional.ofNullable(props.getProperty(prop))
                .filter(v -> !v.isEmpty())
                .map(Integer::parseInt)
                .orElse(defaultValue);

    int connectTimeout = getOrDefault.apply(CONNECT_TIMEOUT_PROP, CONNECT_TIMEOUT_DEFAULT);
    int socketTimeout = getOrDefault.apply(SOCKET_TIMEOUT_PROP, SOCKET_TIMEOUT_DEFAULT);
    int sniffInterval = getOrDefault.apply(SNIFF_INTERVAL_PROP, SNIFF_INTERVAL_DEFAULT);
    int sniffAfterFailureDelay = getOrDefault.apply(SNIFF_AFTER_FAILURE_DELAY_PROP, SNIFF_AFTER_FAILURE_DELAY_DEFAULT);

    return new EsConfig(hosts, index, connectTimeout, socketTimeout, sniffInterval, sniffAfterFailureDelay);
  }

  public String[] getHosts() {
    return hosts;
  }

  public void setHosts(String[] hosts) {
    this.hosts = hosts;
  }

  public String getIndex() {
    return index;
  }

  public void setIndex(String index) {
    this.index = index;
  }

  public int getConnectTimeout() {
    return connectTimeout;
  }

  public void setConnectTimeout(int connectTimeout) {
    this.connectTimeout = connectTimeout;
  }

  public int getSocketTimeout() {
    return socketTimeout;
  }

  public void setSocketTimeout(int socketTimeout) {
    this.socketTimeout = socketTimeout;
  }

  public int getSniffInterval() {
    return sniffInterval;
  }

  public void setSniffInterval(int sniffInterval) {
    this.sniffInterval = sniffInterval;
  }

  public int getSniffAfterFailureDelay() {
    return sniffAfterFailureDelay;
  }

  public void setSniffAfterFailureDelay(int sniffAfterFailureDelay) {
    this.sniffAfterFailureDelay = sniffAfterFailureDelay;
  }
}
