package org.gbif.occurrence.persistence.keygen;

import java.util.Set;

import com.google.common.collect.Sets;

/**
 * An implementation of KeyBuilder to provide keys for use as secondary indexes to occurrences in HBase.
 */
public class OccurrenceKeyBuilder implements KeyBuilder {

  private static final String DELIM = "|";

  @Override
  public Set<String> buildKeys(Set<String> uniqueStrings, String scope) {
    Set<String> results = Sets.newHashSet();
    for (String uniqueString : uniqueStrings) {
      results.add(buildKey(uniqueString, scope));
    }

    return results;
  }

  @Override
  public String buildKey(String uniqueString, String scope) {
    return scope + DELIM + uniqueString;
  }
}
