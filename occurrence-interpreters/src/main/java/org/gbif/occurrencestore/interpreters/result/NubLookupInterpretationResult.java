package org.gbif.occurrencestore.interpreters.result;

import com.google.common.base.Objects;

/**
 * The immutable result of a nub lookup.
 */
public class NubLookupInterpretationResult {

  private final Integer usageKey;
  private final Integer kingdomKey;
  private final Integer phylumKey;
  private final Integer classKey;
  private final Integer orderKey;
  private final Integer familyKey;
  private final Integer genusKey;
  private final Integer speciesKey;
  private final String kingdom;
  private final String phylum;
  private final String clazz;
  private final String order;
  private final String family;
  private final String genus;
  private final String species;
  private final String scientificName;
  private final Integer confidence;

  public NubLookupInterpretationResult() {
    this.usageKey = null;
    this.kingdomKey = null;
    this.phylumKey = null;
    this.classKey = null;
    this.orderKey = null;
    this.familyKey = null;
    this.genusKey = null;
    this.speciesKey = null;
    this.kingdom = null;
    this.phylum = null;
    this.clazz = null;
    this.order = null;
    this.family = null;
    this.genus = null;
    this.species = null;
    this.scientificName = null;
    this.confidence = null;
  }

  public NubLookupInterpretationResult(Integer usageKey, Integer kingdomKey, Integer phylumKey, Integer classKey,
    Integer orderKey, Integer familyKey, Integer genusKey, Integer speciesKey, String kingdom, String phylum,
    String clazz, String order, String family, String genus, String species, String scientificName,
    Integer confidence) {
    this.usageKey = usageKey;
    this.kingdomKey = kingdomKey;
    this.phylumKey = phylumKey;
    this.classKey = classKey;
    this.orderKey = orderKey;
    this.familyKey = familyKey;
    this.genusKey = genusKey;
    this.speciesKey = speciesKey;
    this.kingdom = kingdom;
    this.phylum = phylum;
    this.clazz = clazz;
    this.order = order;
    this.family = family;
    this.genus = genus;
    this.species = species;
    this.scientificName = scientificName;
    this.confidence = confidence;
  }

  public Integer getUsageKey() {
    return usageKey;
  }

  public Integer getKingdomKey() {
    return kingdomKey;
  }

  public Integer getPhylumKey() {
    return phylumKey;
  }

  public Integer getClassKey() {
    return classKey;
  }

  public Integer getOrderKey() {
    return orderKey;
  }

  public Integer getFamilyKey() {
    return familyKey;
  }

  public Integer getGenusKey() {
    return genusKey;
  }

  public Integer getSpeciesKey() {
    return speciesKey;
  }

  public String getKingdom() {
    return kingdom;
  }

  public String getPhylum() {
    return phylum;
  }

  public String getClazz() {
    return clazz;
  }

  public String getOrder() {
    return order;
  }

  public String getFamily() {
    return family;
  }

  public String getGenus() {
    return genus;
  }

  public String getSpecies() {
    return species;
  }

  public String getScientificName() {
    return scientificName;
  }

  public Integer getConfidence() {
    return confidence;
  }

  @Override
  public int hashCode() {
    return Objects
      .hashCode(usageKey, kingdomKey, phylumKey, classKey, orderKey, familyKey, genusKey, speciesKey, kingdom, phylum,
        clazz, order, family, genus, species, scientificName, confidence);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final NubLookupInterpretationResult other = (NubLookupInterpretationResult) obj;
    return Objects.equal(this.usageKey, other.usageKey) && Objects.equal(this.kingdomKey, other.kingdomKey) && Objects
      .equal(this.phylumKey, other.phylumKey) && Objects.equal(this.classKey, other.classKey) && Objects
      .equal(this.orderKey, other.orderKey) && Objects.equal(this.familyKey, other.familyKey) && Objects
      .equal(this.genusKey, other.genusKey) && Objects.equal(this.speciesKey, other.speciesKey) && Objects
      .equal(this.kingdom, other.kingdom) && Objects.equal(this.phylum, other.phylum) && Objects
      .equal(this.clazz, other.clazz) && Objects.equal(this.order, other.order) && Objects
      .equal(this.family, other.family) && Objects.equal(this.genus, other.genus) && Objects
      .equal(this.species, other.species) && Objects.equal(this.scientificName, other.scientificName) && Objects
      .equal(this.confidence, other.confidence);
  }
}
