package org.gbif.occurrence.persistence.api;

import org.gbif.api.vocabulary.EndpointType;

import java.util.UUID;

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represents the "verbatim" view of an occurrence. These are the same fields we were able to parse into
 * RawOccurrenceRecords in our old processing, and are understood by our current interpretation process.  This class
 * is meant to be temporary until a replacement that handles more fields is created (presumably based on maps rather
 * than explicit fields). In theory all fields except datasetKey and at least one uniqueIdentifier are optional, though
 * this would obviously mean a VerbatimOccurrence without much use.
 */
public class VerbatimOccurrence {

  private Integer key;
  private final UUID datasetKey;
  private String institutionCode;
  private String collectionCode;
  private String catalogNumber;
  private String unitQualifier;
  private Integer dataProviderId;
  private Integer dataResourceId;
  private Integer resourceAccessPointId;
  private String scientificName;
  private String author;
  private String rank;
  private String kingdom;
  private String phylum;
  private String klass;
  private String order;
  private String family;
  private String genus;
  private String species;
  private String subspecies;
  private String latitude;
  private String longitude;
  private String latLongPrecision;
  private String minAltitude;
  private String maxAltitude;
  private String altitudePrecision;
  private String minDepth;
  private String maxDepth;
  private String depthPrecision;
  private String continentOrOcean;
  private String country;
  private String stateOrProvince;
  private String county;
  private String collectorName;
  private String locality;
  private String year;
  private String month;
  private String day;
  private String occurrenceDate;
  private String basisOfRecord;
  private String identifierName;
  private String yearIdentified;
  private String monthIdentified;
  private String dayIdentified;
  private String dateIdentified;
  private Long modified;
  private UUID owningOrgKey;
  private EndpointType protocol;

  // TODO: identifiers including dwcOccurrenceId

  public Integer getKey() {
    return key;
  }

  public void setKey(Integer key) {
    this.key = key;
  }

  public Integer getDataProviderId() {
    return dataProviderId;
  }

  public void setDataProviderId(Integer dataProviderId) {
    this.dataProviderId = dataProviderId;
  }

  public Integer getDataResourceId() {
    return dataResourceId;
  }

  public void setDataResourceId(Integer dataResourceId) {
    this.dataResourceId = dataResourceId;
  }

  public Integer getResourceAccessPointId() {
    return resourceAccessPointId;
  }

  public void setResourceAccessPointId(Integer resourceAccessPointId) {
    this.resourceAccessPointId = resourceAccessPointId;
  }

  public String getScientificName() {
    return scientificName;
  }

  public void setScientificName(String scientificName) {
    this.scientificName = scientificName;
  }

  public String getAuthor() {
    return author;
  }

  public void setAuthor(String author) {
    this.author = author;
  }

  public String getRank() {
    return rank;
  }

  public void setRank(String rank) {
    this.rank = rank;
  }

  public String getKingdom() {
    return kingdom;
  }

  public void setKingdom(String kingdom) {
    this.kingdom = kingdom;
  }

  public String getPhylum() {
    return phylum;
  }

  public void setPhylum(String phylum) {
    this.phylum = phylum;
  }

  public String getKlass() {
    return klass;
  }

  public void setKlass(String klass) {
    this.klass = klass;
  }

  public String getOrder() {
    return order;
  }

  public void setOrder(String order) {
    this.order = order;
  }

  public String getFamily() {
    return family;
  }

  public void setFamily(String family) {
    this.family = family;
  }

  public String getGenus() {
    return genus;
  }

  public void setGenus(String genus) {
    this.genus = genus;
  }

  public String getSpecies() {
    return species;
  }

  public void setSpecies(String species) {
    this.species = species;
  }

  public String getSubspecies() {
    return subspecies;
  }

  public void setSubspecies(String subspecies) {
    this.subspecies = subspecies;
  }

  public String getLatitude() {
    return latitude;
  }

  public void setLatitude(String latitude) {
    this.latitude = latitude;
  }

  public String getLongitude() {
    return longitude;
  }

  public void setLongitude(String longitude) {
    this.longitude = longitude;
  }

  public String getLatLongPrecision() {
    return latLongPrecision;
  }

  public void setLatLongPrecision(String latLongPrecision) {
    this.latLongPrecision = latLongPrecision;
  }

  public String getMinAltitude() {
    return minAltitude;
  }

  public void setMinAltitude(String minAltitude) {
    this.minAltitude = minAltitude;
  }

  public String getMaxAltitude() {
    return maxAltitude;
  }

  public void setMaxAltitude(String maxAltitude) {
    this.maxAltitude = maxAltitude;
  }

  public String getAltitudePrecision() {
    return altitudePrecision;
  }

  public void setAltitudePrecision(String altitudePrecision) {
    this.altitudePrecision = altitudePrecision;
  }

  public String getMinDepth() {
    return minDepth;
  }

  public void setMinDepth(String minDepth) {
    this.minDepth = minDepth;
  }

  public String getMaxDepth() {
    return maxDepth;
  }

  public void setMaxDepth(String maxDepth) {
    this.maxDepth = maxDepth;
  }

  public String getDepthPrecision() {
    return depthPrecision;
  }

  public void setDepthPrecision(String depthPrecision) {
    this.depthPrecision = depthPrecision;
  }

  public String getContinentOrOcean() {
    return continentOrOcean;
  }

  public void setContinentOrOcean(String continentOrOcean) {
    this.continentOrOcean = continentOrOcean;
  }

  public String getCountry() {
    return country;
  }

  public void setCountry(String country) {
    this.country = country;
  }

  public String getStateOrProvince() {
    return stateOrProvince;
  }

  public void setStateOrProvince(String stateOrProvince) {
    this.stateOrProvince = stateOrProvince;
  }

  public String getCounty() {
    return county;
  }

  public void setCounty(String county) {
    this.county = county;
  }

  public String getCollectorName() {
    return collectorName;
  }

  public void setCollectorName(String collectorName) {
    this.collectorName = collectorName;
  }

  public String getLocality() {
    return locality;
  }

  public void setLocality(String locality) {
    this.locality = locality;
  }

  public String getYear() {
    return year;
  }

  public void setYear(String year) {
    this.year = year;
  }

  public String getMonth() {
    return month;
  }

  public void setMonth(String month) {
    this.month = month;
  }

  public String getDay() {
    return day;
  }

  public void setDay(String day) {
    this.day = day;
  }

  public String getOccurrenceDate() {
    return occurrenceDate;
  }

  public void setOccurrenceDate(String occurrenceDate) {
    this.occurrenceDate = occurrenceDate;
  }

  public String getBasisOfRecord() {
    return basisOfRecord;
  }

  public void setBasisOfRecord(String basisOfRecord) {
    this.basisOfRecord = basisOfRecord;
  }

  public String getIdentifierName() {
    return identifierName;
  }

  public void setIdentifierName(String identifierName) {
    this.identifierName = identifierName;
  }

  public String getYearIdentified() {
    return yearIdentified;
  }

  public void setYearIdentified(String yearIdentified) {
    this.yearIdentified = yearIdentified;
  }

  public String getMonthIdentified() {
    return monthIdentified;
  }

  public void setMonthIdentified(String monthIdentified) {
    this.monthIdentified = monthIdentified;
  }

  public String getDayIdentified() {
    return dayIdentified;
  }

  public void setDayIdentified(String dayIdentified) {
    this.dayIdentified = dayIdentified;
  }

  public String getDateIdentified() {
    return dateIdentified;
  }

  public void setDateIdentified(String dateIdentified) {
    this.dateIdentified = dateIdentified;
  }

  public Long getModified() {
    return modified;
  }

  public void setModified(Long modified) {
    this.modified = modified;
  }

  public UUID getOwningOrgKey() {
    return owningOrgKey;
  }

  public void setOwningOrgKey(UUID owningOrgKey) {
    this.owningOrgKey = owningOrgKey;
  }

  public UUID getDatasetKey() {
    return datasetKey;
  }

  public String getInstitutionCode() {
    return institutionCode;
  }

  public void setInstitutionCode(String institutionCode) {
    this.institutionCode = institutionCode;
  }

  public String getCollectionCode() {
    return collectionCode;
  }

  public void setCollectionCode(String collectionCode) {
    this.collectionCode = collectionCode;
  }

  public String getCatalogNumber() {
    return catalogNumber;
  }

  public void setCatalogNumber(String catalogNumber) {
    this.catalogNumber = catalogNumber;
  }

  public String getUnitQualifier() {
    return unitQualifier;
  }

  public void setUnitQualifier(String unitQualifier) {
    this.unitQualifier = unitQualifier;
  }

  public EndpointType getProtocol() {
    return protocol;
  }

  public void setProtocol(EndpointType protocol) {
    this.protocol = protocol;
  }

  @Override
  public int hashCode() {
    return Objects
      .hashCode(key, datasetKey, institutionCode, collectionCode, catalogNumber, unitQualifier, dataProviderId,
        dataResourceId, resourceAccessPointId, scientificName, author, rank, kingdom, phylum, klass, order, family,
        genus, species, subspecies, latitude, longitude, latLongPrecision, minAltitude, maxAltitude, altitudePrecision,
        minDepth, maxDepth, depthPrecision, continentOrOcean, country, stateOrProvince, county, collectorName, locality,
        year, month, day, occurrenceDate, basisOfRecord, identifierName, yearIdentified, monthIdentified, dayIdentified,
        dateIdentified, modified, owningOrgKey, protocol);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final VerbatimOccurrence other = (VerbatimOccurrence) obj;
    return Objects.equal(this.key, other.key) && Objects.equal(this.datasetKey, other.datasetKey) && Objects
      .equal(this.institutionCode, other.institutionCode) && Objects.equal(this.collectionCode, other.collectionCode)
           && Objects.equal(this.catalogNumber, other.catalogNumber) && Objects
      .equal(this.unitQualifier, other.unitQualifier) && Objects.equal(this.dataProviderId, other.dataProviderId)
           && Objects.equal(this.dataResourceId, other.dataResourceId) && Objects
      .equal(this.resourceAccessPointId, other.resourceAccessPointId) && Objects
      .equal(this.scientificName, other.scientificName) && Objects.equal(this.author, other.author) && Objects
      .equal(this.rank, other.rank) && Objects.equal(this.kingdom, other.kingdom) && Objects
      .equal(this.phylum, other.phylum) && Objects.equal(this.klass, other.klass) && Objects
      .equal(this.order, other.order) && Objects.equal(this.family, other.family) && Objects
      .equal(this.genus, other.genus) && Objects.equal(this.species, other.species) && Objects
      .equal(this.subspecies, other.subspecies) && Objects.equal(this.latitude, other.latitude) && Objects
      .equal(this.longitude, other.longitude) && Objects.equal(this.latLongPrecision, other.latLongPrecision) && Objects
      .equal(this.minAltitude, other.minAltitude) && Objects.equal(this.maxAltitude, other.maxAltitude) && Objects
      .equal(this.altitudePrecision, other.altitudePrecision) && Objects.equal(this.minDepth, other.minDepth) && Objects
      .equal(this.maxDepth, other.maxDepth) && Objects.equal(this.depthPrecision, other.depthPrecision) && Objects
      .equal(this.continentOrOcean, other.continentOrOcean) && Objects.equal(this.country, other.country) && Objects
      .equal(this.stateOrProvince, other.stateOrProvince) && Objects.equal(this.county, other.county) && Objects
      .equal(this.collectorName, other.collectorName) && Objects.equal(this.locality, other.locality) && Objects
      .equal(this.year, other.year) && Objects.equal(this.month, other.month) && Objects.equal(this.day, other.day)
           && Objects.equal(this.occurrenceDate, other.occurrenceDate) && Objects
      .equal(this.basisOfRecord, other.basisOfRecord) && Objects.equal(this.identifierName, other.identifierName)
           && Objects.equal(this.yearIdentified, other.yearIdentified) && Objects
      .equal(this.monthIdentified, other.monthIdentified) && Objects.equal(this.dayIdentified, other.dayIdentified)
           && Objects.equal(this.dateIdentified, other.dateIdentified)
           && Objects.equal(this.modified, other.modified) && Objects.equal(this.owningOrgKey, other.owningOrgKey)
           && Objects.equal(this.protocol, other.protocol);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("key", key).add("datasetKey", datasetKey)
      .add("institutionCode", institutionCode).add("collectionCode", collectionCode).add("catalogNumber", catalogNumber)
      .add("unitQualifier", unitQualifier).add("dataProviderId", dataProviderId).add("dataResourceId", dataResourceId)
      .add("resourceAccessPointId", resourceAccessPointId).add("scientificName", scientificName).add("author", author)
      .add("rank", rank).add("kingdom", kingdom).add("phylum", phylum).add("klass", klass).add("order", order)
      .add("family", family).add("genus", genus).add("species", species).add("subspecies", subspecies)
      .add("latitude", latitude).add("longitude", longitude).add("latLongPrecision", latLongPrecision)
      .add("minAltitude", minAltitude).add("maxAltitude", maxAltitude).add("altitudePrecision", altitudePrecision)
      .add("minDepth", minDepth).add("maxDepth", maxDepth).add("depthPrecision", depthPrecision)
      .add("continentOrOcean", continentOrOcean).add("country", country).add("stateOrProvince", stateOrProvince)
      .add("county", county).add("collectorName", collectorName).add("locality", locality).add("year", year)
      .add("month", month).add("day", day).add("occurrenceDate", occurrenceDate).add("basisOfRecord", basisOfRecord)
      .add("identifierName", identifierName).add("yearIdentified", yearIdentified)
      .add("monthIdentified", monthIdentified).add("dayIdentified", dayIdentified).add("dateIdentified", dateIdentified)
      .add("modified", modified).add("owningOrgKey", owningOrgKey).add("protocol", protocol)
      .toString();
  }

  // builder methods

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private Integer key;
    private UUID datasetKey;
    private String institutionCode;
    private String collectionCode;
    private String catalogNumber;
    private String unitQualifier;
    private Integer dataProviderId;
    private Integer dataResourceId;
    private Integer resourceAccessPointId;
    private String scientificName;
    private String author;
    private String rank;
    private String kingdom;
    private String phylum;
    private String klass;
    private String order;
    private String family;
    private String genus;
    private String species;
    private String subspecies;
    private String latitude;
    private String longitude;
    private String latLongPrecision;
    private String minAltitude;
    private String maxAltitude;
    private String altitudePrecision;
    private String minDepth;
    private String maxDepth;
    private String depthPrecision;
    private String continentOrOcean;
    private String country;
    private String stateOrProvince;
    private String county;
    private String collectorName;
    private String locality;
    private String year;
    private String month;
    private String day;
    private String occurrenceDate;
    private String basisOfRecord;
    private String identifierName;
    private String yearIdentified;
    private String monthIdentified;
    private String dayIdentified;
    private String dateIdentified;
    private Long modified;
    private UUID owningOrgKey;
    private EndpointType protocol;

    public Builder key(Integer key) {
      this.key = key;
      return this;
    }

    public Builder datasetKey(UUID datasetKey) {
      this.datasetKey = datasetKey;
      return this;
    }

    public Builder institutionCode(String institutionCode) {
      this.institutionCode = institutionCode;
      return this;
    }

    public Builder collectionCode(String collectionCode) {
      this.collectionCode = collectionCode;
      return this;
    }

    public Builder catalogNumber(String catalogNumber) {
      this.catalogNumber = catalogNumber;
      return this;
    }

    public Builder unitQualifier(String unitQualifier) {
      this.unitQualifier = unitQualifier;
      return this;
    }

    public Builder dataProviderId(Integer dataProviderId) {
      this.dataProviderId = dataProviderId;
      return this;
    }

    public Builder dataResourceId(Integer dataResourceId) {
      this.dataResourceId = dataResourceId;
      return this;
    }

    public Builder resourceAccessPointId(Integer resourceAccessPointId) {
      this.resourceAccessPointId = resourceAccessPointId;
      return this;
    }

    public Builder scientificName(String scientificName) {
      this.scientificName = scientificName;
      return this;
    }

    public Builder author(String author) {
      this.author = author;
      return this;
    }

    public Builder rank(String rank) {
      this.rank = rank;
      return this;
    }

    public Builder kingdom(String kingdom) {
      this.kingdom = kingdom;
      return this;
    }

    public Builder phylum(String phylum) {
      this.phylum = phylum;
      return this;
    }

    public Builder klass(String klass) {
      this.klass = klass;
      return this;
    }

    public Builder order(String order) {
      this.order = order;
      return this;
    }

    public Builder family(String family) {
      this.family = family;
      return this;
    }

    public Builder genus(String genus) {
      this.genus = genus;
      return this;
    }

    public Builder species(String species) {
      this.species = species;
      return this;
    }

    public Builder subspecies(String subspecies) {
      this.subspecies = subspecies;
      return this;
    }

    public Builder latitude(String latitude) {
      this.latitude = latitude;
      return this;
    }

    public Builder longitude(String longitude) {
      this.longitude = longitude;
      return this;
    }

    public Builder latLongPrecision(String latLongPrecision) {
      this.latLongPrecision = latLongPrecision;
      return this;
    }

    public Builder minAltitude(String minAltitude) {
      this.minAltitude = minAltitude;
      return this;
    }

    public Builder maxAltitude(String maxAltitude) {
      this.maxAltitude = maxAltitude;
      return this;
    }

    public Builder altitudePrecision(String altitudePrecision) {
      this.altitudePrecision = altitudePrecision;
      return this;
    }

    public Builder minDepth(String minDepth) {
      this.minDepth = minDepth;
      return this;
    }

    public Builder maxDepth(String maxDepth) {
      this.maxDepth = maxDepth;
      return this;
    }

    public Builder depthPrecision(String depthPrecision) {
      this.depthPrecision = depthPrecision;
      return this;
    }

    public Builder continentOrOcean(String continentOrOcean) {
      this.continentOrOcean = continentOrOcean;
      return this;
    }

    public Builder country(String country) {
      this.country = country;
      return this;
    }

    public Builder stateOrProvince(String stateOrProvince) {
      this.stateOrProvince = stateOrProvince;
      return this;
    }

    public Builder county(String county) {
      this.county = county;
      return this;
    }

    public Builder collectorName(String collectorName) {
      this.collectorName = collectorName;
      return this;
    }

    public Builder locality(String locality) {
      this.locality = locality;
      return this;
    }

    public Builder year(String year) {
      this.year = year;
      return this;
    }

    public Builder month(String month) {
      this.month = month;
      return this;
    }

    public Builder day(String day) {
      this.day = day;
      return this;
    }

    public Builder occurrenceDate(String occurrenceDate) {
      this.occurrenceDate = occurrenceDate;
      return this;
    }

    public Builder basisOfRecord(String basisOfRecord) {
      this.basisOfRecord = basisOfRecord;
      return this;
    }

    public Builder identifierName(String identifierName) {
      this.identifierName = identifierName;
      return this;
    }

    public Builder yearIdentified(String yearIdentified) {
      this.yearIdentified = yearIdentified;
      return this;
    }

    public Builder monthIdentified(String monthIdentified) {
      this.monthIdentified = monthIdentified;
      return this;
    }

    public Builder dayIdentified(String dayIdentified) {
      this.dayIdentified = dayIdentified;
      return this;
    }

    public Builder dateIdentified(String dateIdentified) {
      this.dateIdentified = dateIdentified;
      return this;
    }

    public Builder modified(Long modified) {
      this.modified = modified;
      return this;
    }

    public Builder owningOrgKey(UUID owningOrgKey) {
      this.owningOrgKey = owningOrgKey;
      return this;
    }

    public Builder protocol(EndpointType protocol) {
      this.protocol = protocol;
      return this;
    }

    public VerbatimOccurrence build() {
      return new VerbatimOccurrence(this);
    }
  }

  // private so that it can only be used by Builder.build()
  private VerbatimOccurrence(Builder builder) {
    this.key = builder.key;
    this.datasetKey = checkNotNull(builder.datasetKey, "datasetKey can't be null");
    this.institutionCode = builder.institutionCode;
    this.collectionCode = builder.collectionCode;
    this.catalogNumber = builder.catalogNumber;
    this.unitQualifier = builder.unitQualifier;
    this.dataProviderId = builder.dataProviderId;
    this.dataResourceId = builder.dataResourceId;
    this.resourceAccessPointId = builder.resourceAccessPointId;
    this.scientificName = builder.scientificName;
    this.author = builder.author;
    this.rank = builder.rank;
    this.kingdom = builder.kingdom;
    this.phylum = builder.phylum;
    this.klass = builder.klass;
    this.order = builder.order;
    this.family = builder.family;
    this.genus = builder.genus;
    this.species = builder.species;
    this.subspecies = builder.subspecies;
    this.latitude = builder.latitude;
    this.longitude = builder.longitude;
    this.latLongPrecision = builder.latLongPrecision;
    this.minAltitude = builder.minAltitude;
    this.maxAltitude = builder.maxAltitude;
    this.altitudePrecision = builder.altitudePrecision;
    this.minDepth = builder.minDepth;
    this.maxDepth = builder.maxDepth;
    this.depthPrecision = builder.depthPrecision;
    this.continentOrOcean = builder.continentOrOcean;
    this.country = builder.country;
    this.stateOrProvince = builder.stateOrProvince;
    this.county = builder.county;
    this.collectorName = builder.collectorName;
    this.locality = builder.locality;
    this.year = builder.year;
    this.month = builder.month;
    this.day = builder.day;
    this.occurrenceDate = builder.occurrenceDate;
    this.basisOfRecord = builder.basisOfRecord;
    this.identifierName = builder.identifierName;
    this.yearIdentified = builder.yearIdentified;
    this.monthIdentified = builder.monthIdentified;
    this.dayIdentified = builder.dayIdentified;
    this.dateIdentified = builder.dateIdentified;
    this.modified = builder.modified;
    this.owningOrgKey = builder.owningOrgKey;
    this.protocol = builder.protocol;
  }
}
