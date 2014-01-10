package org.gbif.occurrence.interpreters.util;

import java.net.URI;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;


/**
 * A cache loader that will call a webservice, falling back into a retry mechanism should any
 * errors occur in the web service call.  Developers are urged to consider carefully their use
 * before adopting this approach.  This is particularly suitable for operations such as batch
 * processing, where one knows the client will be making a large number of identical web service
 * calls in a short space of time, and changes during that time are not of high importance.
 * Note that {@link WebResource} is suitable as the key
 * since it uses {@link URI} and not {@link URL} which the JVM does not try to resolve at runtime.
 * Construction is forced through static factories to ensure that generics are handled correctly,
 * removing the need for casting at use.
 * The following illustrates the intended use of this class:
 * <pre>
 *   LoadingCache<WebResource, NameUsageMatch> cache = CacheBuilder.newBuilder()
 *     .maximumSize(1000)
 *     .expireAfterAccess(1, TimeUnit.MINUTES)
 *     .build(RetryingWebserviceClient.newInstance(NameUsageMatch.class, NUM_RETRIES, RETRY_PERIOD_MSEC));
 *   NameUsageMatch lookup = cache.get(RESOURCE.queryParams(queryParams));
 * </pre>
 *
 * TODO: this is a copy from occurrence-store - move to somewhere common
 */
public class RetryingWebserviceClient<T> extends CacheLoader<WebResource, T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RetryingWebserviceClient.class);
  private final Class<T> type;
  private final GenericType<T> genericType;
  private final int numberOfAttempts;
  private final int retryDelayMsec;
  public static final int DEFAULT_NUMBER_OF_ATTEMPTS = 1;
  public static final int DEFAULT_DELAY_MSECS = 0;

  /**
   * Creates a new instance using the default configuration {@link #DEFAULT_NUMBER_OF_ATTEMPTS}
   * and {@link #DEFAULT_DELAY_MSECS}.
   *
   * @param type Of resource to be returned by the web service
   *
   * @return A cacheloader suitable for using in a Google {@link CacheBuilder}
   */
  public static <T> RetryingWebserviceClient<T> newInstance(Class<T> type) {
    return new RetryingWebserviceClient<T>(type, DEFAULT_NUMBER_OF_ATTEMPTS, DEFAULT_DELAY_MSECS);
  }

  /**
   * Creates a new instance using the provided configuration.
   *
   * @param type             Of resource to be returned by the web service
   * @param numberOfAttempts Which must be 1 or more
   * @param retryDelayMsec   Which must be 0 or more
   *
   * @return A cacheloader suitable for using in a Google {@link CacheBuilder}
   */
  public static <T> RetryingWebserviceClient<T> newInstance(Class<T> type, int numberOfAttempts, int retryDelayMsec) {
    return new RetryingWebserviceClient<T>(type, numberOfAttempts, retryDelayMsec);
  }

  /**
   * Creates a new instance using the default configuration {@link #DEFAULT_NUMBER_OF_ATTEMPTS}
   * and {@link #DEFAULT_DELAY_MSECS}.
   * This allows implementation to use the likes of:
   * <pre>
   *   new GenericType<PagingResponse<Dataset>>() {};
   * </pre>
   *
   * @param genericType Of the response object
   *
   * @return A cacheloader suitable for using in a Google {@link CacheBuilder}
   */
  public static <T> RetryingWebserviceClient<T> newInstance(GenericType<T> genericType) {
    return new RetryingWebserviceClient<T>(genericType, DEFAULT_NUMBER_OF_ATTEMPTS, DEFAULT_DELAY_MSECS);
  }

  /**
   * Creates a new instance using the provided configuratio, which supports the generic typing of the response object.
   * This allows implementation to use the likes of:
   * <pre>
   *   new GenericType<PagingResponse<Dataset>>() {};
   * </pre>
   *
   * @param genericType      Of the response object
   * @param numberOfAttempts Which must be 1 or more
   * @param retryDelayMsec   Which must be 0 or more
   *
   * @return A cacheloader suitable for using in a Google {@link CacheBuilder}
   */
  public static <T> RetryingWebserviceClient<T> newInstance(GenericType<T> genericType, int numberOfAttempts, int retryDelayMsec) {
    return new RetryingWebserviceClient<T>(genericType, numberOfAttempts, retryDelayMsec);
  }

  /**
   * Force the static factory, to ensure generics are respected.
   */
  private RetryingWebserviceClient(Class<T> type, int numberOfAttempts, int retryDelayMsec) {
    Preconditions.checkArgument(numberOfAttempts > 0, "Number of attempts must be a positive number");
    Preconditions.checkArgument(retryDelayMsec >= 0, "Retry delay must be 0 or more milliseconds");
    this.type = Preconditions.checkNotNull(type);
    this.genericType = null;
    this.numberOfAttempts = numberOfAttempts;
    this.retryDelayMsec = retryDelayMsec;
  }

  /**
   * Force the static factory, to ensure generics are respected.
   */
  private RetryingWebserviceClient(GenericType<T> genericType, int numberOfAttempts, int retryDelayMsec) {
    Preconditions.checkArgument(numberOfAttempts > 0, "Number of attempts must be a positive number");
    Preconditions.checkArgument(retryDelayMsec >= 0, "Retry delay must be 0 or more milliseconds");
    this.type = null;
    this.genericType = Preconditions.checkNotNull(genericType);
    this.numberOfAttempts = numberOfAttempts;
    this.retryDelayMsec = retryDelayMsec;
  }

  @Override
  public T load(WebResource resource) throws Exception {
    for (int attempt = 1; attempt <= numberOfAttempts; attempt++) {
      try {
        if (type != null) {
          return resource.get(type);
        } else {
          return resource.get(genericType);
        }

      } catch (Exception e) {
        LOGGER.error("Error looking up resource in attempt[{}] of max[{}]", new Object[] {attempt, numberOfAttempts, e});
        if (attempt >= numberOfAttempts) {
          throw e;
        }
        if (retryDelayMsec > 0) {
          TimeUnit.MILLISECONDS.sleep(retryDelayMsec);
        }
      }
    }
    // Should not ever happen as we propagate the underlying exception above
    throw new IllegalStateException("Retry count exhausted");
  }
}
