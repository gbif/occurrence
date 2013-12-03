package org.gbif.occurrence.index.solr;

import com.google.common.base.Objects;
import org.apache.solr.client.solrj.beans.Field;


/**
 * Occurrence object used to create Solr documents.
 */
public class OccurrenceIndexDocument {

  private Integer key;

  private String catalogNumber;

  private String collectorName;

  private String isoCountryCode;

  private String hostCountry;

  private Double longitude;

  private Double latitude;

  private Integer year;

  private Integer month;

  private Integer basisOfRecord;

  private String day;

  private Integer nubKey;

  private String datasetKey;

  private Integer kingdomKey;

  private Integer phylumKey;

  private Integer classKey;

  private Integer orderKey;

  private Integer familyKey;

  private Integer genusKey;

  private Integer speciesKey;

  @Override
  public boolean equals(Object object) {
    if (object instanceof OccurrenceIndexDocument) {
      if (!super.equals(object)) {
        return false;
      }
      OccurrenceIndexDocument that = (OccurrenceIndexDocument) object;
      return Objects.equal(this.key, that.key)
        && Objects.equal(this.latitude, that.latitude)
        && Objects.equal(this.longitude, that.longitude)
        && Objects.equal(this.year, that.year)
        && Objects.equal(this.month, that.month)
        && Objects.equal(this.catalogNumber, that.catalogNumber)
        && Objects.equal(this.nubKey, that.nubKey)
        && Objects.equal(this.kingdomKey, that.kingdomKey)
        && Objects.equal(this.phylumKey, that.phylumKey)
        && Objects.equal(this.classKey, that.classKey)
        && Objects.equal(this.orderKey, that.orderKey)
        && Objects.equal(this.familyKey, that.familyKey)
        && Objects.equal(this.genusKey, that.genusKey)
        && Objects.equal(this.speciesKey, that.speciesKey)
        && Objects.equal(this.datasetKey, that.datasetKey)
        && Objects.equal(this.day, that.day)
        && Objects.equal(this.basisOfRecord, that.basisOfRecord)
        && Objects.equal(this.collectorName, that.collectorName)
        && Objects.equal(this.isoCountryCode, that.isoCountryCode);
    }
    return false;
  }

  /**
   * @return the basisOfRecord
   */
  public Integer getBasisOfRecord() {
    return basisOfRecord;
  }

  /**
   * @return the catalogNumber
   */
  public String getCatalogNumber() {
    return catalogNumber;
  }

  /**
   * @return the classKey
   */
  public Integer getClassKey() {
    return classKey;
  }

  /**
   * @return the collectorName
   */
  public String getCollectorName() {
    return collectorName;
  }

  /**
   * @return the datasetKey
   */
  public String getDatasetKey() {
    return datasetKey;
  }

  /**
   * @return the day
   */
  public String getDay() {
    return day;
  }

  /**
   * @return the familyKey
   */
  public Integer getFamilyKey() {
    return familyKey;
  }

  /**
   * @return the genusKey
   */
  public Integer getGenusKey() {
    return genusKey;
  }

  public String getHostCountry() {
    return hostCountry;
  }

  /**
   * @return the isoCountryCode
   */
  public String getIsoCountryCode() {
    return isoCountryCode;
  }

  /**
   * @return the key
   */
  public Integer getKey() {
    return key;
  }

  /**
   * @return the kingdomKey
   */
  public Integer getKingdomKey() {
    return kingdomKey;
  }

  /**
   * @return the latitude
   */
  public Double getLatitude() {
    return latitude;
  }


  /**
   * @return the longitude
   */
  public Double getLongitude() {
    return longitude;
  }


  /**
   * @return the month
   */
  public Integer getMonth() {
    return month;
  }


  /**
   * @return the nubKey
   */
  public Integer getNubKey() {
    return nubKey;
  }


  /**
   * @return the orderKey
   */
  public Integer getOrderKey() {
    return orderKey;
  }


  /**
   * @return the phylumKey
   */
  public Integer getPhylumKey() {
    return phylumKey;
  }


  /**
   * @return the speciesKey
   */
  public Integer getSpeciesKey() {
    return speciesKey;
  }


  /**
   * @return the year
   */
  public Integer getYear() {
    return year;
  }


  @Override
  public int hashCode() {
    return Objects
      .hashCode(super.hashCode(), key, latitude, longitude, year, month, catalogNumber, nubKey);
  }


  /**
   * @param basisOfRecord the basisOfRecord to set
   */
  public void setBasisOfRecord(Integer basisOfRecord) {
    this.basisOfRecord = basisOfRecord;
  }


  /**
   * @param catalogNumber the catalogNumber to set
   */
  @Field("catalog_number")
  public void setCatalogNumber(String catalogNumber) {
    this.catalogNumber = catalogNumber;
  }


  /**
   * @param classKey the classKey to set
   */
  public void setClassKey(Integer classKey) {
    this.classKey = classKey;
  }


  /**
   * @param collectorName the collectorName to set
   */
  public void setCollectorName(String collectorName) {
    this.collectorName = collectorName;
  }


  /**
   * @param datasetKey the datasetKey to set
   */
  public void setDatasetKey(String datasetKey) {
    this.datasetKey = datasetKey;
  }


  /**
   * @param day the day to set
   */
  public void setDay(String day) {
    this.day = day;
  }


  /**
   * @param familyKey the familyKey to set
   */
  public void setFamilyKey(Integer familyKey) {
    this.familyKey = familyKey;
  }


  /**
   * @param genusKey the genusKey to set
   */
  public void setGenusKey(Integer genusKey) {
    this.genusKey = genusKey;
  }


  public void setHostCountry(String hostCountry) {
    this.hostCountry = hostCountry;
  }


  /**
   * @param isoCountryCode the isoCountryCode to set
   */
  public void setIsoCountryCode(String isoCountryCode) {
    this.isoCountryCode = isoCountryCode;
  }


  /**
   * @param key the key to set
   */
  @Field
  public void setKey(Integer key) {
    this.key = key;
  }


  /**
   * @param kingdomKey the kingdomKey to set
   */
  public void setKingdomKey(Integer kingdomKey) {
    this.kingdomKey = kingdomKey;
  }


  /**
   * @param latitude the latitude to set
   */
  @Field
  public void setLatitude(Double latitude) {
    this.latitude = latitude;
  }


  /**
   * @param longitude the longitude to set
   */
  @Field
  public void setLongitude(Double longitude) {
    this.longitude = longitude;
  }


  /**
   * @param month the month to set
   */
  @Field
  public void setMonth(Integer month) {
    this.month = month;
  }


  /**
   * @param nubKey the nubKey to set
   */
  @Field("nub_key")
  public void setNubKey(Integer nubKey) {
    this.nubKey = nubKey;
  }


  /**
   * @param orderKey the orderKey to set
   */
  public void setOrderKey(Integer orderKey) {
    this.orderKey = orderKey;
  }


  /**
   * @param phylumKey the phylumKey to set
   */
  public void setPhylumKey(Integer phylumKey) {
    this.phylumKey = phylumKey;
  }


  /**
   * @param speciesKey the speciesKey to set
   */
  public void setSpeciesKey(Integer speciesKey) {
    this.speciesKey = speciesKey;
  }


  /**
   * @param year the year to set
   */
  @Field
  public void setYear(Integer year) {
    this.year = year;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("super", super.toString()).add("key", key)
      .add("latitude", latitude).add("longitude", longitude).add("isoCountryCode", isoCountryCode)
      .add("year", year).add("month", month).add("day", day)
      .add("catalogNumber", catalogNumber).add("nubKey", nubKey)
      .add("kingdomKey", kingdomKey).add("phylumKey", phylumKey)
      .add("classKey", classKey).add("orderKey", orderKey)
      .add("familyKey", familyKey).add("genusKey", genusKey)
      .add("speciesKey", speciesKey).add("datasetKey", datasetKey)
      .add("collectorName", collectorName).add("basisOfRecord", basisOfRecord)
      .toString();
  }
}
