package org.gbif.occurrencestore.persistence.api;

import com.google.common.base.Objects;

/**
 * The result of attempting to insert a new Fragment. The usual case will be that the key is created as expected. It is
 * possible that a race could mean that another snippet with the same unique identifiers creates the key first, making
 * this an update.
 */
public class FragmentCreationResult {

  private final Fragment fragment;
  private final boolean keyCreated;

  public FragmentCreationResult(Fragment fragment, boolean keyCreated) {
    this.fragment = fragment;
    this.keyCreated = keyCreated;
  }

  public Fragment getFragment() {
    return fragment;
  }

  public boolean isKeyCreated() {
    return keyCreated;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(fragment, keyCreated);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final FragmentCreationResult other = (FragmentCreationResult) obj;
    return Objects.equal(this.fragment, other.fragment) && Objects.equal(this.keyCreated, other.keyCreated);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("fragment", fragment).add("keyCreated", keyCreated).toString();
  }
}
