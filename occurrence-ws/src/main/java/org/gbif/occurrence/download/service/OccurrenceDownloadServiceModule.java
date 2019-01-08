package org.gbif.occurrence.download.service;

import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.service.hive.SqlDownloadService;
import org.gbif.occurrence.download.service.workflow.DownloadWorkflowParameters;
import org.gbif.service.guice.PrivateServiceModule;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.mail.Session;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.hive.HiveConnectionPool;
import org.apache.nifi.mock.MockControllerServiceInitializationContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.oozie.client.OozieClient;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

public class OccurrenceDownloadServiceModule extends PrivateServiceModule {

  private static final String PREFIX = "occurrence.download.";

  public OccurrenceDownloadServiceModule(Properties properties) {
    super(PREFIX, properties);
  }

  @Override
  protected void configureService() {
    bind(DownloadEmailUtils.class);
    bind(DownloadRequestService.class).to(DownloadRequestServiceImpl.class);
    bind(SqlDownloadService.class);
    
    expose(DownloadRequestService.class);
    expose(SqlDownloadService.class);
    
    bind(CallbackService.class).to(DownloadRequestServiceImpl.class);
    expose(CallbackService.class);
  }

  @Provides
  @Singleton
  Session providesMailSession(@Named("mail.smtp") String smtpServer, @Named("mail.from") String fromAddress) {
    Properties props = new Properties();
    props.setProperty("mail.smtp.host", smtpServer);
    props.setProperty("mail.from", fromAddress);
    return Session.getInstance(props, null);
  }

  @Provides
  @Singleton
  OozieClient providesOozieClient(@Named("oozie.url") String url) {
    return new OozieClient(url);
  }

  @Provides
  @Singleton
  @Named("oozie.default_properties")
  Map<String,String> providesDefaultParameters(@Named("environment") String environment,
                                               @Named("ws.url") String wsUrl,
                                               @Named("hdfs.namenode") String nameNode,
                                               @Named("user.name") String userName) {
    return new ImmutableMap.Builder<String, String>()
      .put(OozieClient.LIBPATH, String.format(DownloadWorkflowParameters.WORKFLOWS_LIB_PATH_FMT, environment))
      .put(OozieClient.APP_PATH, nameNode + String.format(DownloadWorkflowParameters.DOWNLOAD_WORKFLOW_PATH_FMT,
                                                          environment))
      .put(OozieClient.WORKFLOW_NOTIFICATION_URL,
           DownloadUtils.concatUrlPaths(wsUrl, "occurrence/download/request/callback?job_id=$jobId&status=$status"))
      .put(OozieClient.USER_NAME, userName)
      .putAll(DownloadWorkflowParameters.CONSTANT_PARAMETERS).build();
  }

  /**
   * Creates {@linkplain org.apache.nifi.dbcp.hive.HiveConnectionPool} from provided default
   * properties.
   *
   * @return HiveConnectionPool from default properties.
   * @throws InitializationException error initializing connection pool.
   */
  @Provides
  @Singleton
  HiveConnectionPool providesHiveConnectionPool(@Named("hive.jdbc.url") String jdbcURL,
                                               @Named("hive.jdbc.username") String username,
                                               @Named("hive.jdbc.password") String password,
                                               @Named("hive.jdbc.poolsize") int poolSize,
                                                @Named("hive.jdbc.maxWaitTime") String maxWaitTime,
                                                @Named("hive.jdbc.validationQuery") String validationQuery)
    throws InitializationException {
    HiveConnectionPool cp = new HiveConnectionPool();
    NifiConfigurationContext
       context = NifiConfigurationContext.from(jdbcURL).withUsername(username).withPassword(password)
      .withMaxConnections(poolSize).withMaxWaitTime(maxWaitTime).withProperty(HiveConnectionPool.VALIDATION_QUERY, validationQuery);
    cp.initialize(new MockControllerServiceInitializationContext());
    cp.onConfigured(context);
    return cp;
  }

  /**
   *
   * Nifi Configuration Context containing jdbc related properties.
   *
   */
  private static class NifiConfigurationContext implements ConfigurationContext {

    private final Map<PropertyDescriptor, String> properties = new HashMap<>();

    private NifiConfigurationContext() {}

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
      properties.put(HiveConnectionPool.MAX_TOTAL_CONNECTIONS, connections + "");
      return this;
    }

    public NifiConfigurationContext withMaxWaitTime(String timeInMillis) {
      properties.put(HiveConnectionPool.MAX_WAIT_TIME, timeInMillis);
      return this;
    }

    public NifiConfigurationContext withProperty(PropertyDescriptor descriptor, String value) {
      properties.put(descriptor, value);
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
