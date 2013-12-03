package org.gbif.occurrence.ws.resources;

import java.util.Date;
import java.util.UUID;


/**
 * This is an HTTP API only object which encapsulates only the concise information needed to push featured occurrences
 * for the homepage maps. This exists specifically to reduce the amount of data sent for the high traffic homepage of
 * GBIF and also to provide extra fields not present in the occurrence object.
 * The visibility of this class is public <em>only</em> to allow Jackson access.
 */
public class FeaturedOccurrence {

  private final Integer key;
  private final String scientificName;
  private final String publisher;
  private final Double latitude;
  private final Double longitude;
  private final UUID publisherKey;
  private final Date modified;

  public FeaturedOccurrence(Integer key, Double latitude, Double longitude, String scientificName, String publisher,
    UUID publisherKey, Date modified) {
    this.key = key;
    this.scientificName = scientificName;
    this.publisher = publisher;
    this.latitude = latitude;
    this.longitude = longitude;
    this.publisherKey = publisherKey;
    this.modified = modified;
  }

  public Integer getKey() {
    return key;
  }

  public Double getLatitude() {
    return latitude;
  }

  public Double getLongitude() {
    return longitude;
  }

  public Date getModified() {
    return modified;
  }

  public String getPublisher() {
    return publisher;
  }

  public UUID getPublisherKey() {
    return publisherKey;
  }

  public String getScientificName() {
    return scientificName;
  }

}
