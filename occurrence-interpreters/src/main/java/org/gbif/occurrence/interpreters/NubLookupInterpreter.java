package org.gbif.occurrence.interpreters;

import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.vocabulary.OccurrenceValidationRule;
import org.gbif.occurrence.interpreters.result.InterpretationResult;
import org.gbif.occurrence.interpreters.util.RetryingWebserviceClient;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.core.MultivaluedMap;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.apache.ApacheHttpClient;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Attempts to lookup the nub key and nub classification for a given classification.
 */
public class NubLookupInterpreter {

  private static final Logger LOG = LoggerFactory.getLogger(NubLookupInterpreter.class);

  private static final String WEB_SERVICE_URL;
  private static final String WEB_SERVICE_URL_PROPERTY = "occurrence.nub.ws.url";

  // The repetitive nature of our data encourages use of a light cache to reduce WS load
  private static final LoadingCache<WebResource, NameUsageMatch> CACHE =
    CacheBuilder.newBuilder().maximumSize(10000).expireAfterAccess(10, TimeUnit.MINUTES)
      .build(RetryingWebserviceClient.newInstance(NameUsageMatch.class, 10, 2000));

  private static final WebResource RESOURCE;

  static {
    try {
      InputStream is =
        NubLookupInterpreter.class.getClassLoader().getResourceAsStream("occurrence-interpreter.properties");
      if (is == null) {
        throw new RuntimeException("Can't load properties file [occurrence-interpreter.properties]");
      }
      try {
        Properties props = new Properties();
        props.load(is);
        WEB_SERVICE_URL = props.getProperty(WEB_SERVICE_URL_PROPERTY);
      } finally {
        is.close();
      }
    } catch (IOException e) {
      throw new RuntimeException("Can't load properties file [occurrence-interpreter.properties]", e);
    }
    ClientConfig cc = new DefaultClientConfig();
    cc.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, true);
    cc.getClasses().add(JacksonJsonProvider.class);
    Client client = ApacheHttpClient.create(cc);
    RESOURCE = client.resource(WEB_SERVICE_URL);
  }

  // if the WS is not responding, we drop into a retry count
  private static final int NUM_RETRIES = 3;
  private static final int RETRY_PERIOD_MSEC = 2000;

  private NubLookupInterpreter() {
  }

  public static InterpretationResult<NameUsageMatch> nubLookup(String kingdom, String phylum, String clazz, String order,
    String family, String genus, String scientificName, String author) {

    if (kingdom == null && phylum == null && clazz == null && order == null && family == null && genus == null
        && scientificName == null) {
      return new InterpretationResult<NameUsageMatch>(null);
    }

    InterpretationResult<NameUsageMatch> result = null;
    MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
    queryParams.add("kingdom", kingdom);
    queryParams.add("phylum", phylum);
    queryParams.add("class", clazz);
    queryParams.add("order", order);
    queryParams.add("family", family);
    queryParams.add("genus", genus);
    queryParams.add("name", scientificName);
    // TODO: include author in query

    if (scientificName != null) {
      for (int i = 0; i < NUM_RETRIES; i++) {
        LOG.debug("Attempt [{}] to lookup sci name [{}]", i, scientificName);
        try {
          NameUsageMatch lookup = CACHE.get(RESOURCE.queryParams(queryParams));
          if (lookup != null) {
            result = new InterpretationResult<NameUsageMatch>(lookup);
            switch (lookup.getMatchType()) {
              case NONE:
                result.setValidationRule(OccurrenceValidationRule.TAXON_MATCH_NONE, true);
                LOG.info("Nub lookup for [{}] returned no match. Lookup note: [{}]", scientificName, lookup.getNote());
                break;
              case FUZZY:
                result.setValidationRule(OccurrenceValidationRule.TAXON_MATCH_FUZZY, true);
                LOG.debug("Nub lookup for [{}] was fuzzy. Match note: [{}]", scientificName, lookup.getNote());
                break;
              case HIGHERRANK:
                result.setValidationRule(OccurrenceValidationRule.TAXON_MATCH_HIGHERRANK, true);
                LOG.debug("Nub lookup for [{}] was to higher rank only. Match note: [{}]",
                          scientificName,
                          lookup.getNote());
                break;
            }
          }

          break; // from retry loop
        } catch (ExecutionException e) {
          // Log the error
          LOG.error("Failed WS call with: {}", recreateQueryString(queryParams));

          // have we exhausted our attempts?
          if (i >= NUM_RETRIES) {
            throw new RuntimeException(e);
          }

          try {
            Thread.sleep(RETRY_PERIOD_MSEC);
          } catch (InterruptedException e1) {
          }
        } catch (UniformInterfaceException e) {
          LOG.info("Got unexpected result for scientific name '{}', Response: {}", scientificName, e.getResponse());
          // have we exhausted our attempts?
          if (i >= NUM_RETRIES) {
            throw e;
          }

          try {
            Thread.sleep(RETRY_PERIOD_MSEC);
          } catch (InterruptedException e1) {
          }
        }
      } // retry loop
    } // scientific name exists

    return result;
  }

  private static String recreateQueryString(MultivaluedMap<String, String> queryParams) {
    StringBuilder sb = new StringBuilder("?");
    for (Map.Entry<String, List<String>> en : queryParams.entrySet()) {
      sb.append(en.getKey()).append('=');
      for (String s : en.getValue()) {
        sb.append(s);
      }
      sb.append('&');
    }

    return sb.toString();
  }
}
