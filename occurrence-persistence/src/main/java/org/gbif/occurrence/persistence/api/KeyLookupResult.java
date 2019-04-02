package org.gbif.occurrence.persistence.api;

import com.google.common.base.Objects;

/**
 * Wraps the result of looking up an Occurrence key in order to provide information on whether the key was created for
 * this request or not.
 */
public class KeyLookupResult {

  private final long key;
  private final boolean created;

  public KeyLookupResult(long key, boolean created) {
    this.key = key;
    this.created = created;
  }

  public long getKey() {
    return key;
  }

  public boolean isCreated() {
    return created;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(key, created);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final KeyLookupResult other = (KeyLookupResult) obj;
    return Objects.equal(this.key, other.key) && Objects.equal(this.created, other.created);
  }
}
