package org.gbif.occurrence.search;

import org.gbif.api.model.common.MediaObject;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.util.VocabularyUtils;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.EstablishmentMeans;
import org.gbif.api.vocabulary.License;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.TypeStatus;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.json.MediaSerDeserUtils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.io.Resources;
import org.apache.commons.beanutils.PropertyUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseDate;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.ParseLong;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvMapReader;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.CsvContext;

/**
 * Utility class that loads and processes occurrence records read from a CSV file.
 * The expected columns in that file are:
 * "key","altitude","basisOfRecord","catalogNumber","classKey","clazz","collectionCode","dataProviderId",
 * "dataResourceId","datasetKey","depth","occurrenceId","family","familyKey","genus",
 * "genusKey","institutionCode","country","continent","kingdom","kingdomKey","latitude","longitude","modified",
 * "occurrenceMonth","taxonKey","occurrenceDate","order","orderKey","otherIssue",
 * "publishingOrgKey","phylum","phylumKey","scientificName","species","speciesKey",
 * "unitQualifier","year","locality","county","stateProvince","continent",
 * "collectorName","collectorNumber","identifierName", "identificationDate".
 * Each cvs line is interpreted into an Occurrence object; to process each object a predicate or list of predicates are
 * passed as parameters to the function loadOccurrences.
 */
public class OccurrenceDataLoader {

  /**
   * Produces a Basis of Record instance.
   */
  private static class EstablishmentMeansProcessor implements CellProcessor {

    @Override
    public EstablishmentMeans execute(Object value, CsvContext context) {
      Enum<?> establishmentMeans = VocabularyUtils.lookupEnum((String) value, EstablishmentMeans.class);
      if (establishmentMeans != null) {
        return (EstablishmentMeans) establishmentMeans;
      }
      return null;
    }

  }

  private static class IssueProcessor implements CellProcessor {

    private ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public Set<OccurrenceIssue> execute(Object value, CsvContext context) {
      Set<OccurrenceIssue> occurrenceIssues = Sets.newHashSet();
      try {
        Set<String> issues = MAPPER.readValue(value.toString(), new TypeReference<Set<String>>() {
        });
        if(issues != null && !issues.isEmpty()) {
          for (String issueStr : issues) {
            occurrenceIssues.add(VocabularyUtils.lookupEnum(issueStr, OccurrenceIssue.class));
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      return occurrenceIssues;
    }
  }


  /**
   * Produces a Basis of Record instance.
   */
  private static class BasisOfRecordProcessor implements CellProcessor {

    @Override
    public BasisOfRecord execute(Object value, CsvContext context) {
      Enum<?> basisOfRecord = VocabularyUtils.lookupEnum((String) value, BasisOfRecord.class);
      if (basisOfRecord != null) {
        return (BasisOfRecord) basisOfRecord;
      }
      return null;
    }

  }


  /**
   * Produces a TypeStatus instance.
   */
  private static class TypeStatusProcessor implements CellProcessor {

    @Override
    public TypeStatus execute(Object value, CsvContext context) {
      Enum<?> typeStatus = VocabularyUtils.lookupEnum((String) value, TypeStatus.class);
      if (typeStatus != null) {
        return (TypeStatus) typeStatus;
      }
      return null;
    }

  }


  /**
   * Produces a MediaType instance.
   */
  private static class MediaListProcessor implements CellProcessor {

    @Override
    public List<MediaObject> execute(Object value, CsvContext context) {
      if (value != null) {
        return MediaSerDeserUtils.fromJson((String) value);
      }
      return null;
    }

  }


  /**
   * Produces a Continent instance.
   */
  private static class ContinentProcessor implements CellProcessor {

    @Override
    public Continent execute(Object value, CsvContext context) {
      Enum<?> continent = VocabularyUtils.lookupEnum((String) value, Continent.class);
      if (continent != null) {
        return (Continent) continent;
      }
      return null;
    }
  }

  /**
   * Produces a Country instance.
   */
  private static class CountryProcessor implements CellProcessor {

    @Override
    public Country execute(Object value, CsvContext context) {
      return Country.fromIsoCode((String) value);
    }

  }


  /**
   * Produces an UUID instance from a string object.
   */
  private static class UUIDProcessor implements CellProcessor {

    @Override
    public UUID execute(Object value, CsvContext context) {
      return UUID.fromString((String) value);
    }

  }

  /**
   * Produces a EndpointType instance.
   */
  private static class EndpointTypeProcessor implements CellProcessor {

    @Override
    public EndpointType execute(Object value, CsvContext context) {
      Enum<?> endpointType = VocabularyUtils.lookupEnum((String) value, EndpointType.class);
      if (endpointType != null) {
        return (EndpointType) endpointType;
      }
      return null;
    }
  }

  /**
   * Produces a License instance.
   */
  private static class LicenseProcessor implements CellProcessor {

    @Override
    public License execute(Object value, CsvContext context) {
      Enum<?> license = VocabularyUtils.lookupEnum((String) value, License.class);
      if (license != null) {
        return (License) license;
      }
      return null;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceDataLoader.class);

  // Date format used in the CSV file.
  private static final String DATE_FORMAT = "yyyy-MM-dd"; // Tue Nov 23 17:00:00 CST 1954


  // List of processors, a processor is defined for each column
  private final static CellProcessor[] CELL_PROCESSORS = new CellProcessor[] {
    new ParseLong(), // key
    new Optional(new ParseDouble()), // elevation
    new Optional(new BasisOfRecordProcessor()), // basisOfRecord
    new Optional(), // catalogNumber
    new Optional(new ParseInt()), // classKey
    new Optional(), // clazz
    new Optional(), // collectionCode
    new Optional(new UUIDProcessor()), // datasetKey
    new Optional(new ParseDouble()),// depth
    new Optional(),// occurrenceId
    new Optional(),// family
    new Optional(new ParseInt()),// familyKey
    new Optional(),// genus
    new Optional(new ParseInt()),// genusKey
    new Optional(),// institutionCode
    new Optional(new CountryProcessor()),// country
    new Optional(),// kingdom
    new Optional(new ParseInt()),// kingdomKey
    new Optional(new ParseDouble()),// latitude
    new Optional(new ParseDouble()),// longitude
    new Optional(new ParseDate(DATE_FORMAT)),// lastInterpreted
    new Optional(new ParseInt()),// month
    new Optional(new ParseInt()),// taxonKey
    new Optional(new ParseDate(DATE_FORMAT)),// eventDate
    new Optional(),// order
    new Optional(new ParseInt()),// orderKey
    new Optional(new UUIDProcessor()),// publishingOrgKey
    new Optional(),// phylum
    new Optional(new ParseInt()),// phylumKey
    new Optional(),// scientificName
    new Optional(),// species
    new Optional(new ParseInt()),// speciesKey
    new Optional(),// unitQualifier
    new Optional(new ParseInt()),// year
    new Optional(),// locality
    new Optional(),// county
    new Optional(),// stateProvince
    new Optional(new ContinentProcessor()),// continent
    new Optional(),// collectorName
    new Optional(),// recordNumber
    new Optional(),// identifierName
    new Optional(new ParseDate(DATE_FORMAT)),// identificationDate
    new Optional(new TypeStatusProcessor()),// typeStatus
    new Optional(new MediaListProcessor()),// List<Media> in JSON
    new Optional(new EstablishmentMeansProcessor()),// establishmentMeans.
    new Optional(new IssueProcessor()),// issues.
    new Optional(),// organismId
    new Optional(),// waterBody
    new Optional(new EndpointTypeProcessor()), // protocol
    new Optional(new LicenseProcessor()), // license
    new Optional(new ParseInt()) // crawlId
  };


  // Column headers
  private final static String[] HEADER = new String[] {
    "key",
    "elevation",
    "basisOfRecord",
    "catalogNumber",
    "classKey",
    "clazz",
    "collectionCode",
    "datasetKey",
    "depth",
    "occurrenceId",
    "family",
    "familyKey",
    "genus",
    "genusKey",
    "institutionCode",
    "country",
    "kingdom",
    "kingdomKey",
    "decimalLatitude",
    "decimalLongitude",
    "lastInterpreted",
    "month",
    "taxonKey",
    "eventDate",
    "order",
    "orderKey",
    "publishingOrgKey",
    "phylum",
    "phylumKey",
    "scientificName",
    "species",
    "speciesKey",
    "unitQualifier",
    "year",
    "locality",
    "county",
    "stateProvince",
    "continent",
    "recordedBy",
    "recordNumber",
    "identifiedBy",
    "dateIdentified",
    "typeStatus",
    "media",
    "establishmentMeans",
    "issues",
    "organismID",
    "waterBody",
    "protocol",
    "license",
    "crawlId"
  };


  // Verbatim field names
  private final static Set<String> VERBATIM_FIELDS = new ImmutableSet.Builder<String>().add(
    "catalogNumber",
    "collectionCode",
    "occurrenceId",
    "institutionCode",
    "unitQualifier",
    "locality",
    "county",
    "stateProvince",
    "recordedBy",
    "recordNumber",
    "identifiedBy",
    "organismID").build();


  /**
   * Reads a CSV file and produces occurrence records for each line.
   * Each occurrence object is processed by the list of processors.
   *
   * @param fileName CSV file
   * @param processors list of processors(predicates) that consume occurrence objects
   */
  public static void processOccurrences(String fileName, Predicate<Occurrence>... processors) {
    ICsvMapReader reader = null;
    int line = 1;
    try {
      reader =
        new CsvMapReader(new FileReader(new File(Resources.getResource(fileName).toURI())),
          CsvPreference.STANDARD_PREFERENCE);
      reader.getHeader(true);
      Map<String, Object> occurrenceMap;
      while ((occurrenceMap = reader.read(HEADER, CELL_PROCESSORS)) != null) {
        Occurrence occurrence = convertMap(occurrenceMap);
        for (Predicate<Occurrence> predicate : processors) {
          predicate.apply(occurrence);
        }
        line++;
      }
    } catch (Exception e) {
      LOG.error(String.format("Error parsing occurrence object from file %d", line), e);
    } finally {
      try {
        Closeables.close(reader, false);
      } catch (IOException io) {
        LOG.warn("Failed to close reader", io);
      }
    }
  }

  private static Occurrence convertMap(Map<String, Object> occurrenceMap) {
    Occurrence occurrence = new Occurrence();
    for (Entry<String, Object> field : occurrenceMap.entrySet()) {
      if (VERBATIM_FIELDS.contains(field.getKey())) {
        Entry<? extends Term, String> verbatimField = toTermEntry(field);
        occurrence.setVerbatimField(verbatimField.getKey(), verbatimField.getValue());
      } else {
        setInterpretedField(field, occurrence);
      }

    }
    return occurrence;
  }


  private static Entry<? extends Term, String> toTermEntry(Entry<String, Object> field) {
    String strValue = null;
    if (field.getValue() != null) {
      strValue = (String) field.getValue();
    }
    if (field.getKey().equals(GbifInternalTerm.unitQualifier.name())) {
      return Maps.immutableEntry(GbifInternalTerm.unitQualifier, strValue);
    } else {
      Enum<?> term = VocabularyUtils.lookupEnum(field.getKey(), DwcTerm.class);
      if (term != null) {
        return Maps.immutableEntry((DwcTerm) term, strValue);
      }
    }
    return null;
  }


  private static void setInterpretedField(Entry<String, Object> rawField, Occurrence occurrence) {
    try {
      PropertyUtils.setProperty(occurrence, rawField.getKey(), rawField.getValue());
    } catch (IllegalAccessException e) {
      Throwables.propagate(e);
    } catch (InvocationTargetException e) {
      Throwables.propagate(e);
    } catch (NoSuchMethodException e) {
      Throwables.propagate(e);
    }
  }

}
