package org.gbif.occurrence.persistence.util;

import org.gbif.api.model.common.Identifier;
import org.gbif.api.model.common.MediaObject;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.util.ClassificationUtils;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.EstablishmentMeans;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.IdentifierType;
import org.gbif.api.vocabulary.LifeStage;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.api.vocabulary.Rank;
import org.gbif.api.vocabulary.Sex;
import org.gbif.api.vocabulary.TypeStatus;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.hbase.util.ResultReader;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.common.json.ExtensionSerDeserUtils;
import org.gbif.occurrence.common.json.MediaSerDeserUtils;
import org.gbif.occurrence.persistence.api.Fragment;
import org.gbif.occurrence.persistence.hbase.Columns;
import org.gbif.occurrence.persistence.hbase.ExtResultReader;

import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
import javax.validation.ValidationException;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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

  // TODO: move these maps to Classification, Term or RankUtils
  public static final Map<Rank, Term> rank2taxonTerm =
    ImmutableMap.<Rank, Term>builder().put(Rank.KINGDOM, DwcTerm.kingdom).put(Rank.PHYLUM, DwcTerm.phylum)
      .put(Rank.CLASS, DwcTerm.class_).put(Rank.ORDER, DwcTerm.order).put(Rank.FAMILY, DwcTerm.family)
      .put(Rank.GENUS, DwcTerm.genus).put(Rank.SUBGENUS, DwcTerm.subgenus).put(Rank.SPECIES, GbifTerm.species).build();

  public static final Map<Rank, Term> rank2KeyTerm =
    ImmutableMap.<Rank, Term>builder().put(Rank.KINGDOM, GbifTerm.kingdomKey).put(Rank.PHYLUM, GbifTerm.phylumKey)
      .put(Rank.CLASS, GbifTerm.classKey).put(Rank.ORDER, GbifTerm.orderKey).put(Rank.FAMILY, GbifTerm.familyKey)
      .put(Rank.GENUS, GbifTerm.genusKey).put(Rank.SUBGENUS, GbifTerm.subgenusKey)
      .put(Rank.SPECIES, GbifTerm.speciesKey).build();

  // should never be instantiated
  private OccurrenceBuilder() {
  }

  /**
   * Builds a Fragment object from the given result, assigning the passed in key.
   *
   * @param result an HBase scan/get Result
   * @return the Fragment or null if the passed in Result is null
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

    Integer crawlId = ExtResultReader.getInteger(result, GbifInternalTerm.crawlId);
    if (crawlId == null) {
      throw new ValidationException("Fragment with key [" + key + "] has no crawlId.");
    }
    Long harvested = ExtResultReader.getLong(result, GbifTerm.lastCrawled);
    if (harvested == null) {
      throw new ValidationException("Fragment with key [" + key + "] has no harvestedDate.");
    }
    Date harvestedDate = new Date(harvested);
    String unitQualifier = ExtResultReader.getString(result, GbifInternalTerm.unitQualifier);
    byte[] data = ExtResultReader.getBytes(result, GbifInternalTerm.fragment);
    byte[] dataHash = ExtResultReader.getBytes(result, GbifInternalTerm.fragmentHash);
    Long created = ExtResultReader.getLong(result, GbifInternalTerm.fragmentCreated);
    String rawSchema = ExtResultReader.getString(result, GbifInternalTerm.xmlSchema);
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
      Occurrence occ = new Occurrence(buildVerbatimOccurrence(row, false));

      // filter out verbatim terms that have been interpreted
      for (Term t : TermUtils.interpretedSourceTerms()) {
        occ.getVerbatimFields().remove(t);
      }

      Integer key = Bytes.toInt(row.getRow());
      occ.setKey(key);

      // taxonomy terms
      occ.setTaxonKey(ExtResultReader.getInteger(row, GbifTerm.taxonKey));
      occ.setScientificName(ExtResultReader.getString(row, DwcTerm.scientificName));
      occ.setGenericName(ExtResultReader.getString(row, GbifTerm.genericName));
      occ.setSpecificEpithet(ExtResultReader.getString(row, DwcTerm.specificEpithet));
      occ.setInfraspecificEpithet(ExtResultReader.getString(row, DwcTerm.infraspecificEpithet));
      occ.setTaxonRank(ExtResultReader.getEnum(row, DwcTerm.taxonRank, Rank.class));
      for (Rank r : Rank.DWC_RANKS) {
        ClassificationUtils
          .setHigherRankKey(occ, r, ExtResultReader.getInteger(row, OccurrenceBuilder.rank2KeyTerm.get(r)));
        ClassificationUtils
          .setHigherRank(occ, r, ExtResultReader.getString(row, OccurrenceBuilder.rank2taxonTerm.get(r)));
      }

      // other java properties
      occ.setBasisOfRecord(ExtResultReader.getEnum(row, DwcTerm.basisOfRecord, BasisOfRecord.class));
      occ.setElevation(ExtResultReader.getDouble(row, GbifTerm.elevation));
      occ.setElevationAccuracy(ExtResultReader.getDouble(row, GbifTerm.elevationAccuracy));
      occ.setDepth(ExtResultReader.getDouble(row, GbifTerm.depth));
      occ.setDepthAccuracy(ExtResultReader.getDouble(row, GbifTerm.depthAccuracy));

      occ.setDatasetKey(ExtResultReader.getUuid(row, GbifTerm.datasetKey));
      occ.setPublishingOrgKey(ExtResultReader.getUuid(row, GbifInternalTerm.publishingOrgKey));
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
      occ.setTypifiedName(ExtResultReader.getString(row, GbifTerm.typifiedName));

      occ.setReferences(ExtResultReader.getUri(row, DcTerm.references));

      occ.setIdentifiers(extractIdentifiers(key, row));
      occ.setIssues(extractIssues(row));
      occ.setMedia(buildMedia(row));

      return occ;
    }
  }


  /**
   * Utility to build an API Occurrence from an HBase row.
   *
   * @return A complete verbatim occurrence, or null
   */
  public static VerbatimOccurrence buildVerbatimOccurrence(@Nullable Result row) {
    return buildVerbatimOccurrence(row, true);
  }

  /**
   * Utility to build an API Occurrence from an HBase row.
   *
   * @param readExtensions if true reads verbatim extension data into extensions map
   * @return A complete verbatim occurrence, or null
   */
  private static VerbatimOccurrence buildVerbatimOccurrence(@Nullable Result row, boolean readExtensions) {
    if (row == null || row.isEmpty()) {
      return null;
    }

    VerbatimOccurrence verb = new VerbatimOccurrence();
    verb.setKey(Bytes.toInt(row.getRow()));
    verb.setDatasetKey(ExtResultReader.getUuid(row, GbifTerm.datasetKey));
    verb.setPublishingOrgKey(ExtResultReader.getUuid(row, GbifInternalTerm.publishingOrgKey));
    verb.setPublishingCountry(Country.fromIsoCode(ExtResultReader.getString(row, GbifTerm.publishingCountry)));
    verb.setLastCrawled(ExtResultReader.getDate(row, GbifTerm.lastCrawled));
    verb.setLastParsed(ExtResultReader.getDate(row, GbifTerm.lastParsed));
    verb.setProtocol(EndpointType.fromString(ExtResultReader.getString(row, GbifTerm.protocol)));

    for (KeyValue kv : row.raw()) {
      // all verbatim Term fields in row are prefixed. Columns without that prefix return null!
      // extensions are also kept with a v_ prefix, so explicitly ignore them.
      Term term = Columns.termFromVerbatimColumn(kv.getQualifier());
      if (term != null && !TermUtils.isExtensionTerm(term)) {
        verb.setVerbatimField(term, Bytes.toString(kv.getValue()));
      }
    }

    if (readExtensions) {
      verb.setExtensions(readVerbatimExtensions(row));
    }
    return verb;
  }

  /**
   * Reads the extensions from a result row.
   */
  private static Map<Extension, List<Map<Term, String>>> readVerbatimExtensions(@Nullable Result row) {
    Map<Extension, List<Map<Term, String>>> extensions = Maps.newHashMap();
    for (Extension extension : Extension.values()) {
      String jsonExtensions = ExtResultReader.getString(row, Columns.verbatimColumn(extension));
      if (!Strings.isNullOrEmpty(jsonExtensions)) {
        extensions.put(extension, ExtensionSerDeserUtils.fromJson(jsonExtensions));
      }
    }
    return extensions;
  }

  private static List<Identifier> extractIdentifiers(Integer key, Result result) {
    List<Identifier> records = Lists.newArrayList();
    Integer maxCount = ExtResultReader.getInteger(result, GbifInternalTerm.identifierCount);
    if (maxCount != null) {
      for (int count = 0; count < maxCount; count++) {
        String idCol = Columns.idColumn(count);
        String idTypeCol = Columns.idTypeColumn(count);
        String id = ResultReader.getString(result, Columns.OCCURRENCE_COLUMN_FAMILY, idCol, null);
        String rawType = ResultReader.getString(result, Columns.OCCURRENCE_COLUMN_FAMILY, idTypeCol, null);
        if (id != null && rawType != null) {
          IdentifierType idType = null;
          try {
            idType = IdentifierType.valueOf(rawType);
          } catch (IllegalArgumentException e) {
            LOG.warn("Unrecognized value for IdentifierType from field [{}] - data is corrupt.", rawType);
          }
          if (idType != null) {
            Identifier record = new Identifier();
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
      String column = Columns.column(issue);
      byte[] val = result.getValue(Columns.CF, Bytes.toBytes(column));
      if (val != null) {
        issues.add(issue);
      }
    }

    return issues;
  }

  /**
   * Builds the list of media objects.
   */
  public static List<MediaObject> buildMedia(Result result) {
    List<MediaObject> media = null;
    String mediaJson = ExtResultReader.getString(result, Columns.column(Extension.MULTIMEDIA));
    if (mediaJson != null && !mediaJson.isEmpty()) {
      try {
        media = MediaSerDeserUtils.fromJson(mediaJson);
      } catch (Exception e) {
        LOG.warn("Unable to deserialize media objects from hbase", e);
      }
    }

    return media;
  }

}
