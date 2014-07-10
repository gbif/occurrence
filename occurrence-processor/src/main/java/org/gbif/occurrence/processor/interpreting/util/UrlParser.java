package org.gbif.occurrence.processor.interpreting.util;

import com.google.common.base.CharMatcher;
import com.google.common.base.Strings;

import java.net.URI;

/**
 * Greedy URL parser assuming http URIs in case no schema was given.
 * Modified version of the registry-metadata GreedyUriConverter.
 * @deprecated please use the version from common parsers
 */
@Deprecated
public class UrlParser {
  private static final String HTTP_SCHEME = "http://";

  private UrlParser() {
  }

  /**
   * Convert a String into a java.net.URI.
   * In case its missing the protocol prefix, it is prefixed with the default protocol.
   *
   * @param value The input value to be converted
   *
   * @return The converted value, or null if not parsable or exception occurred
   */
  public static URI parse(String value) {
    value = CharMatcher.WHITESPACE.trimFrom(Strings.nullToEmpty(value));
    if (Strings.isNullOrEmpty(value)) {
      return null;
    }

    URI uri = null;
    try {
      uri = URI.create(value);
      if (!uri.isAbsolute() && value.startsWith("www")) {
        // make www an http address
        try {
          uri = URI.create(HTTP_SCHEME + value);
        } catch (IllegalArgumentException e) {
          // keep the previous scheme-less result
        }
      }

      // verify that we have a domain
      if (Strings.isNullOrEmpty(uri.getHost())) {
        return null;
      }

    } catch (IllegalArgumentException e) {
    }

    return uri;
  }

}
