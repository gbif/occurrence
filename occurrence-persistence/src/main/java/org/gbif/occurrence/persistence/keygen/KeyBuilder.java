package org.gbif.occurrence.persistence.keygen;

import java.util.Set;

/**
 * This interface describes methods that can be used to build unique keys given a string and its "scope". It is
 * expected that implementations of this class would incorporate the scope in the resulting "key" (e.g. by
 * concatenating it with the input uniqueString). It's expected that this class will move outside occurrence
 * persistence at some point.
 */
public interface KeyBuilder {

  /**
   * Combine the given uniqueStrings with the given scope to produce a set of strings that are now (hopefully)
   * globally unique.
   *
   * @param uniqueStrings the strings unique within the scope
   * @param scope the scope of the given strings' uniqueness
   *
   * @return the uniqueStrings combined with the scope
   */
  Set<String> buildKeys(Set<String> uniqueStrings, String scope);

  /**
   * Combine the given uniqueString with the given scope to produce a string that is now (hopefully) globally unique.
   *
   * @param uniqueString the string unique within the scope
   * @param scope the scope of the given string's uniqueness
   *
   * @return the uniqueString combined with the scope
   */
  String buildKey(String uniqueString, String scope);
}
