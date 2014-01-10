package org.gbif.occurrence.persistence.api;

import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.OccurrenceSchemaType;

import java.util.Arrays;
import java.util.Date;
import java.util.UUID;
import javax.annotation.Nullable;

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represents an occurrence "fragment" from one of our publishers, either XML from any of our supported schemas (listed
 * in OccurrenceSchemaType) or JSON from a DarwinCore Archive. In most cases it represents one occurrence record - the
 * exception is ABCD 2, in which multiple Identification elements may exist (within the xml data) which we interpret as
 * distinct, multiple occurrences within this one fragment. In the ABCD 2 multiples case the resulting Fragments can
 * only be differentiated by the scientific name within each Identification, which is stored here and in subsequent
 * occurrence objects (VerbatimOccurrence and Occurrence) as the UnitQualifier.
 *
 * @see OccurrenceSchemaType
 */
public class Fragment {

  private Integer key;
  private final UUID datasetKey;
  private byte[] data;
  private byte[] dataHash;
  private String unitQualifier;
  private FragmentType fragmentType;
  private Date harvestedDate;
  private Integer crawlId;
  private OccurrenceSchemaType xmlSchema;
  private EndpointType protocol;
  private Long created;

  public Fragment(
    UUID datasetKey,
    @Nullable byte[] data,
    @Nullable byte[] dataHash,
    @Nullable FragmentType fragmentType,
    @Nullable EndpointType protocol,
    @Nullable Date harvestedDate,
    @Nullable Integer crawlId,
    @Nullable OccurrenceSchemaType xmlSchema,
    @Nullable String unitQualifier,
    @Nullable Long created)
  {
    this.datasetKey = checkNotNull(datasetKey, "datasetKey can't be null");
    if (data != null) {
      this.data = Arrays.copyOf(data, data.length);
    }
    if (dataHash != null) {
      this.dataHash = Arrays.copyOf(dataHash, dataHash.length);
    }
    this.fragmentType = fragmentType;
    this.harvestedDate = harvestedDate;
    this.crawlId = crawlId;
    this.xmlSchema = xmlSchema;
    this.protocol = protocol;
    this.unitQualifier = unitQualifier;
    this.created = created;
  }

  public Fragment(UUID datasetKey) {
    this.datasetKey = checkNotNull(datasetKey, "datasetKey can't be null");
  }

  public Integer getKey() {
    return key;
  }

  public void setKey(Integer key) {
    this.key = key;
  }

  public UUID getDatasetKey() {
    return datasetKey;
  }

  public byte[] getData() {
    return data;
  }

  public void setData(byte[] data) {
    if (data != null) {
      this.data = Arrays.copyOf(data, data.length);
    }
  }

  public byte[] getDataHash() {
    return dataHash;
  }

  public void setDataHash(byte[] dataHash) {
    if (dataHash != null) {
      this.dataHash = Arrays.copyOf(dataHash, dataHash.length);
    }
  }

  public FragmentType getFragmentType() {
    return fragmentType;
  }

  public void setFragmentType(FragmentType fragmentType) {
    this.fragmentType = fragmentType;
  }

  public Date getHarvestedDate() {
    return harvestedDate;
  }

  public void setHarvestedDate(Date harvestedDate) {
    this.harvestedDate = harvestedDate;
  }

  public Integer getCrawlId() {
    return crawlId;
  }

  public void setCrawlId(Integer crawlId) {
    this.crawlId = crawlId;
  }

  public OccurrenceSchemaType getXmlSchema() {
    return xmlSchema;
  }

  public void setXmlSchema(OccurrenceSchemaType xmlSchema) {
    this.xmlSchema = xmlSchema;
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

  public Long getCreated() {
    return created;
  }

  public void setCreated(Long created) {
    this.created = created;
  }

  @Override
  public int hashCode() {
    return Objects
      .hashCode(key, datasetKey, data, dataHash, unitQualifier, fragmentType, harvestedDate, crawlId, xmlSchema,
        protocol, created);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final Fragment other = (Fragment) obj;
    return Objects.equal(this.key, other.key) && Objects.equal(this.datasetKey, other.datasetKey) && Objects
      .equal(this.data, other.data) && Objects.equal(this.dataHash, other.dataHash) && Objects
             .equal(this.unitQualifier, other.unitQualifier) && Objects.equal(this.fragmentType, other.fragmentType)
           && Objects.equal(this.harvestedDate, other.harvestedDate) && Objects.equal(this.crawlId, other.crawlId)
           && Objects.equal(this.xmlSchema, other.xmlSchema) && Objects.equal(this.protocol, other.protocol) && Objects
      .equal(this.created, other.created);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("key", key).add("datasetKey", datasetKey).add("data", data)
      .add("dataHash", dataHash).add("unitQualifier", unitQualifier).add("fragmentType", fragmentType)
      .add("harvestedDate", harvestedDate).add("crawlId", crawlId).add("xmlSchema", xmlSchema).add("protocol", protocol)
      .add("created", created).toString();
  }

  public enum FragmentType {
    XML, JSON
  }
}
