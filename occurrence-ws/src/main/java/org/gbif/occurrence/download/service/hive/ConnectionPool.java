package org.gbif.occurrence.download.service.hive;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.hive.HiveConnectionPool;
import org.apache.nifi.mock.MockControllerServiceInitializationContext;
import org.apache.nifi.reporting.InitializationException;
import org.gbif.utils.file.properties.PropertiesUtil;
import org.gbif.ws.app.ConfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * ConnectionPool for Hive JDBC connection.
 *
 */
public class ConnectionPool {

  private static final String JDBC_URL = "occurrence.hive.jdbc.url";
  private static final String JDBC_USER = "occurrence.hive.jdbc.username";
  private static final String JDBC_PASS = "occurrence.hive.jdbc.password";
  private static final String APP_CONF_FILE = "occurrence.properties";
  private static final String JDBC_POOL_SIZE = "occurrence.hive.jdbc.poolsize";
  private static final String JDBC_WAIT_TIME = "occurrence.hive.jdbc.maxWaitTime";
  private static final Logger LOG = LoggerFactory.getLogger(ConnectionPool.class);
  
  private ConnectionPool() {}
  
  private static HiveConnectionPool cp;
  
  /**
   * Creates {@linkplain org.apache.nifi.dbcp.hive.HiveConnectionPool} from provided default properties.
   * @return HiveConnectionPool from default properties.
   * @throws IOException when default occurrence.properties file not available.
   * @throws InitializationException error initializing connection pool.
   */
  public static synchronized HiveConnectionPool fromDefaultProperties() throws IOException, InitializationException {
    if (cp != null) {
      LOG.info("Cached connection pool for Hive JDBC connections, {}", cp);
      return cp;
    }
    
    cp =  new HiveConnectionPool();
    Properties jdbcProperties = PropertiesUtil.readFromFile(ConfUtils.getAppConfFile(APP_CONF_FILE));
    Function<String,String> messageTemplate = config -> String.format("%s is missing in configuration.", config);
    
    String jdbcURL = Objects.requireNonNull(jdbcProperties.getProperty(JDBC_URL), messageTemplate.apply(JDBC_URL) );
    String username = Objects.requireNonNull(jdbcProperties.getProperty(JDBC_USER), messageTemplate.apply(JDBC_USER));
    String password = Objects.requireNonNull(jdbcProperties.getProperty(JDBC_PASS), messageTemplate.apply(JDBC_PASS));
    String maxWaitTime = Objects.requireNonNull(jdbcProperties.getProperty(JDBC_WAIT_TIME), messageTemplate.apply(JDBC_WAIT_TIME));
    int poolSize = Integer.parseInt(Objects.requireNonNull(jdbcProperties.getProperty(JDBC_POOL_SIZE , messageTemplate.apply(JDBC_POOL_SIZE))));

    NifiConfigurationContext context = NifiConfigurationContext.from(jdbcURL).withUsername(username)
      .withPassword(password).withMaxConnections(poolSize).withMaxWaitTime(maxWaitTime);
    cp.initialize(new MockControllerServiceInitializationContext());
    cp.onConfigured(context);
    LOG.info("Creating connection pool for Hive JDBC connections, using jdbc properties {}, {}",jdbcProperties, cp);
    return cp;
  }

  /**
   * 
   * Nifi Configuration Context containing jdbc related properties.
   *
   */
  private static class NifiConfigurationContext implements ConfigurationContext {

    private final Map<PropertyDescriptor,String> properties = new HashMap<>(); 
    
    private NifiConfigurationContext(){}
    
    public static NifiConfigurationContext from(String jdbcURL) {
      NifiConfigurationContext context = new NifiConfigurationContext();
      context.properties.put(HiveConnectionPool.DATABASE_URL, jdbcURL);
      return context;
    }
    
    public NifiConfigurationContext withUsername(String username) {
      properties.put(HiveConnectionPool.DB_USER, username);
      return this;
    }
    
    public NifiConfigurationContext withPassword(String password) {
      properties.put(HiveConnectionPool.DB_PASSWORD, password);
      return this;
    }
    
    public NifiConfigurationContext withMaxConnections(int connections) {
      properties.put(HiveConnectionPool.MAX_TOTAL_CONNECTIONS, connections+"");
      return this;
    }
    
    public NifiConfigurationContext withMaxWaitTime(String timeInMillis) {
      properties.put(HiveConnectionPool.MAX_WAIT_TIME, timeInMillis);
      return this;
    }
    
    @Override
    public PropertyValue getProperty(PropertyDescriptor descriptor) {
      return new StandardPropertyValue(properties.get(descriptor), null);
    }

    @Override
    public Map<String, String> getAllProperties() {
      return properties.entrySet().stream().collect(Collectors.toMap(k -> k.getKey().toString(), Map.Entry::getValue));
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
      return properties;
    }

    @Override
    public String getSchedulingPeriod() {
      return null;
    }

    @Override
    public Long getSchedulingPeriod(TimeUnit timeUnit) {
      return null;
    }

    @Override
    public String getName() {
      return "hive connection pool";
    }
    
  }
  
}
