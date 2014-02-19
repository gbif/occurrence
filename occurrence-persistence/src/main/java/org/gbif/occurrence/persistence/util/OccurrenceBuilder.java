package org.gbif.occurrence.persistence.util;

import org.gbif.api.model.common.Identifier;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.util.ClassificationUtils;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.EstablishmentMeans;
import org.gbif.api.vocabulary.IdentifierType;
import org.gbif.api.vocabulary.LifeStage;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.api.vocabulary.Rank;
import org.gbif.api.vocabulary.Sex;
import org.gbif.api.vocabulary.TypeStatus;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.hbase.util.ResultReader;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.persistence.api.Fragment;
import org.gbif.occurrence.persistence.api.InternalTerm;
import org.gbif.occurrence.persistence.hbase.ExtResultReader;
import org.gbif.occurrence.persistence.hbase.FieldNameUtil;
import org.gbif.occurrence.persistence.hbase.TableConstants;

import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
import javax.validation.ValidationException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class to build object models from the HBase occurrence "row".
 */
public class OccurrenceBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceBuilder.class);
  private static final byte[] CF = Bytes.toBytes(TableConstants.OCCURRENCE_COLUMN_FAMILY);

  //TODO: move these maps to Classification, Term or RankUtils
  public static final Map<Rank, Term> rank2taxonTerm = ImmutableMap.<Rank, Term>builder()
    .put(Rank.KINGDOM, DwcTerm.kingdom)
    .put(Rank.PHYLUM, DwcTerm.phylum)
    .put(Rank.CLASS, DwcTerm.class_)
    .put(Rank.ORDER, DwcTerm.order)
    .put(Rank.FAMILY, DwcTerm.family)
    .put(Rank.GENUS, DwcTerm.genus)
    .put(Rank.SUBGENUS, DwcTerm.subgenus)
    .put(Rank.SPECIES, GbifTerm.species)
    .build();

  public static final Map<Rank, Term> rank2KeyTerm = ImmutableMap.<Rank, Term>builder()
    .put(Rank.KINGDOM, GbifTerm.kingdomKey)
    .put(Rank.PHYLUM, GbifTerm.phylumKey)
    .put(Rank.CLASS, GbifTerm.classKey)
    .put(Rank.ORDER, GbifTerm.orderKey)
    .put(Rank.FAMILY, GbifTerm.familyKey)
    .put(Rank.GENUS, GbifTerm.genusKey)
    .put(Rank.SUBGENUS, GbifTerm.subgenusKey)
    .put(Rank.SPECIES, GbifTerm.speciesKey)
    .build();

  // should never be instantiated
  private OccurrenceBuilder() {
  }

  /**
   * Builds a Fragment object from the given result, assigning the passed in key.
   *
   * @param result an HBase scan/get Result
   *
   * @return the Fragment or null if the passed in Result is null
   *
   * @throws ValidationException if the fragment as stored in the table is invalid
   */
  public static Fragment buildFragment(@Nullable Result result) {
    if (result == null) {
      return null;
    }

    int key = Bytes.toInt(result.getRow());

    String rawDatasetKey = ExtResultReader.getString(result, GbifTerm.datasetKey);
    if (rawDatasetKey == null) {
      throw new ValidationException("Fragment with key [" + key + "] has no datasetKey.");
    }
    UUID datasetKey = UUID.fromString(rawDatasetKey);

    Integer crawlId = ExtResultReader.getInteger(result, InternalTerm.crawlId);
    if (crawlId == null) {
      throw new ValidationException("Fragment with key [" + key + "] has no crawlId.");
    }
    Long harvested = ExtResultReader.getLong(result, GbifTerm.lastCrawled);
    if (harvested == null) {
      throw new ValidationException("Fragment with key [" + key + "] has no harvestedDate.");
    }
    Date harvestedDate = new Date(harvested);
    String unitQualifier = ExtResultReader.getString(result, GbifTerm.unitQualifier);
    byte[] data = ExtResultReader.getBytes(result, InternalTerm.fragment);
    byte[] dataHash = ExtResultReader.getBytes(result, InternalTerm.fragmentHash);
    Long created = ExtResultReader.getLong(result, InternalTerm.fragmentCreated);
    String rawSchema = ExtResultReader.getString(result, InternalTerm.xmlSchema);
    OccurrenceSchemaType schema;
    if (rawSchema == null) {
      // this is typically called just before updating the fragment, meaning schemaType will then be correctly set
      LOG.debug("Fragment with key [{}] has no schema type - assuming DWCA.", key);
      schema = OccurrenceSchemaType.DWCA;
    } else {
      schema = OccurrenceSchemaType.valueOf(rawSchema);
    }
    String rawProtocol = ExtResultReader.getString(result, GbifTerm.protocol);
    EndpointType protocol = rawProtocol == null ? null : EndpointType.valueOf(rawProtocol);

    Fragment frag;
    if (schema == null || schema == OccurrenceSchemaType.DWCA) {
      frag =
        new Fragment(datasetKey, data, dataHash, Fragment.FragmentType.JSON, protocol, harvestedDate, crawlId, schema,
          null, created);
    } else {
      frag =
        new Fragment(datasetKey, data, dataHash, Fragment.FragmentType.XML, protocol, harvestedDate, crawlId, schema,
          unitQualifier, created);
    }
    frag.setKey(key);

    return frag;
  }

  /**
   * Utility to build an API Occurrence from an HBase row.
   *
   * @return A complete occurrence, or null
   */
  public static Occurrence buildOccurrence(@Nullable Result row) {
    if (row == null || row.isEmpty()) {
      return null;
    } else {
      Occurrence occ = new Occurrence(buildVerbatimOccurrence(row));

      // filter out verbatim terms that have been interpreted
      for (Term t : TermUtils.interpretedSourceTerms()) {
        occ.getVerbatimFields().remove(t);
      }

      Integer key = Bytes.toInt(row.getRow());
      occ.setKey(key);

      // taxonomy terms
      occ.setTaxonKey(ExtResultReader.getInteger(row, DwcTerm.taxonID));
      occ.setScientificName(ExtResultReader.getString(row, DwcTerm.scientificName));
      occ.setGenericName(ExtResultReader.getString(row, DwcTerm.genericName));
      occ.setSpecificEpithet(ExtResultReader.getString(row, DwcTerm.specificEpithet));
      occ.setInfraspecificEpithet(ExtResultReader.getString(row, DwcTerm.infraspecificEpithet));
      occ.setTaxonRank(ExtResultReader.getEnum(row, DwcTerm.taxonRank, Rank.class));
      for (Rank r : Rank.DWC_RANKS) {
        ClassificationUtils.setHigherRankKey(occ,
                                             r,
                                             ExtResultReader.getInteger(row, OccurrenceBuilder.rank2KeyTerm.get(r)));
        ClassificationUtils.setHigherRank(occ,
                                          r,
                                          ExtResultReader.getString(row, OccurrenceBuilder.rank2taxonTerm.get(r)));
      }

      // other java properties
      occ.setBasisOfRecord(ExtResultReader.getEnum(row, DwcTerm.basisOfRecord, BasisOfRecord.class));
      occ.setElevation(ExtResultReader.getInteger(row, GbifTerm.elevation));
      occ.setElevationAccuracy(ExtResultReader.getInteger(row, GbifTerm.elevationAccuracy));
      occ.setDepth(ExtResultReader.getInteger(row, GbifTerm.depth));
      occ.setDepthAccuracy(ExtResultReader.getInteger(row, GbifTerm.depthAccuracy));
      occ.setDistanceAboveSurface(ExtResultReader.getInteger(row, GbifTerm.distanceAboveSurface));
      occ.setDistanceAboveSurfaceAccuracy(ExtResultReader.getInteger(row, GbifTerm.distanceAboveSurfaceAccuracy));

      occ.setDatasetKey(ExtResultReader.getUuid(row, GbifTerm.datasetKey));
      occ.setPublishingOrgKey(ExtResultReader.getUuid(row, InternalTerm.publishingOrgKey));
      occ.setPublishingCountry(Country.fromIsoCode(ExtResultReader.getString(row, GbifTerm.publishingCountry)));

      occ.setLastInterpreted(ExtResultReader.getDate(row, GbifTerm.lastInterpreted));
      occ.setModified(ExtResultReader.getDate(row, DcTerm.modified));
      occ.setDateIdentified(ExtResultReader.getDate(row, DwcTerm.dateIdentified));
      occ.setProtocol(ExtResultReader.getEnum(row, GbifTerm.protocol, EndpointType.class));

      occ.setDecimalLatitude(ExtResultReader.getDouble(row, DwcTerm.decimalLatitude));
      occ.setDecimalLongitude(ExtResultReader.getDouble(row, DwcTerm.decimalLongitude));
      occ.setCoordinateAccuracy(ExtResultReader.getDouble(row, GbifTerm.coordinateAccuracy));
      occ.setCountry(Country.fromIsoCode(ExtResultReader.getString(row, DwcTerm.countryCode)));
      occ.setStateProvince(ExtResultReader.getString(row, DwcTerm.stateProvince));
      occ.setContinent(ExtResultReader.getEnum(row, DwcTerm.continent, Continent.class));
      occ.setWaterBody(ExtResultReader.getString(row, DwcTerm.waterBody));

      occ.setEventDate(ExtResultReader.getDate(row, DwcTerm.eventDate));
      occ.setYear(ExtResultReader.getInteger(row, DwcTerm.year));
      occ.setMonth(ExtResultReader.getInteger(row, DwcTerm.month));
      occ.setDay(ExtResultReader.getInteger(row, DwcTerm.day));

      occ.setIndividualCount(ExtResultReader.getInteger(row, DwcTerm.individualCount));
      occ.setEstablishmentMeans(ExtResultReader.getEnum(row, DwcTerm.establishmentMeans, EstablishmentMeans.class));
      occ.setLifeStage(ExtResultReader.getEnum(row, DwcTerm.lifeStage, LifeStage.class));
      occ.setSex(ExtResultReader.getEnum(row, DwcTerm.sex, Sex.class));

      occ.setTypeStatus(ExtResultReader.getEnum(row, DwcTerm.typeStatus, TypeStatus.class));
      occ.setTypifiedName(ExtResultReader.getString(row, DwcTerm.typifiedName));

      occ.setIdentifiers(extractIdentifiers(key, row));
      occ.setIssues(extractIssues(row));

      return occ;
    }
  }

  /**
   * Utility to build an API Occurrence from an HBase row.
   *
   * @return A complete occurrence, or null
   */
  public static VerbatimOccurrence buildVerbatimOccurrence(@Nullable Result row) {
    if (row == null || row.isEmpty()) {
      return null;
    }

    VerbatimOccurrence verb = new VerbatimOccurrence();
    verb.setKey(Bytes.toInt(row.getRow()));
    verb.setDatasetKey(ExtResultReader.getUuid(row, GbifTerm.datasetKey));
    verb.setPublishingOrgKey(ExtResultReader.getUuid(row, InternalTerm.publishingOrgKey));
    verb.setPublishingCountry(Country.fromIsoCode(ExtResultReader.getString(row, GbifTerm.publishingCountry)));
    verb.setLastCrawled(ExtResultReader.getDate(row, GbifTerm.lastCrawled));
    verb.setLastParsed(ExtResultReader.getDate(row, GbifTerm.lastParsed));
    verb.setProtocol(EndpointType.fromString(ExtResultReader.getString(row, GbifTerm.protocol)));

    for (KeyValue kv : row.raw()) {
      // all verbatim Term fields in row are prefixed. Columns without that prefix return null!
      Term term = FieldNameUtil.getTermFromVerbatimColumn(kv.getQualifier());
      if (term != null) {
        verb.setVerbatimField(term, Bytes.toString(kv.getValue()));
      }
    }

    return verb;
  }

  private static List<Identifier> extractIdentifiers(Integer key, Result result) {
    List<Identifier> records = Lists.newArrayList();
    Integer maxCount = ExtResultReader.getInteger(result, InternalTerm.identifierCount);
    if (maxCount != null) {
      for (int count = 0; count < maxCount; count++) {
        String idCol = TableConstants.IDENTIFIER_COLUMN + count;
        String idTypeCol = TableConstants.IDENTIFIER_TYPE_COLUMN + count;
        String id = ResultReader.getString(result, TableConstants.OCCURRENCE_COLUMN_FAMILY, idCol, null);
        String rawType = ResultReader.getString(result, TableConstants.OCCURRENCE_COLUMN_FAMILY, idTypeCol, null);
        if (id != null && rawType != null) {
          IdentifierType idType = null;
          try {
            idType = IdentifierType.valueOf(rawType);
          } catch (IllegalArgumentException e) {
            LOG.warn("Unrecognized value for IdentifierType from field [{}] - data is corrupt.", rawType);
          }
          if (idType != null) {
            Identifier record = new Identifier();
            record.setEntityKey(key);
            record.setIdentifier(id);
            record.setType(idType);
            records.add(record);
          }
        }
      }
    }
    return records;
  }

  private static Set<OccurrenceIssue> extractIssues(Result result) {
    Set<OccurrenceIssue> issues = EnumSet.noneOf(OccurrenceIssue.class);
    for (OccurrenceIssue issue : OccurrenceIssue.values()) {
      String column = FieldNameUtil.getColumn(issue);
      byte[] val = result.getValue(CF, Bytes.toBytes(column));
      if (val != null) {
        issues.add(issue);
      }
    }

    return issues;
  }

}
