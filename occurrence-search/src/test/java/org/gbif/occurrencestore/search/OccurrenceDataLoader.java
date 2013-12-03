package org.gbif.occurrencestore.search;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Country;
import org.gbif.occurrencestore.util.BasisOfRecordConverter;

import java.io.File;
import java.io.FileReader;
import java.util.UUID;

import com.google.common.base.Predicate;
import com.google.common.io.Closeables;
import com.google.common.io.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseDate;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanReader;
import org.supercsv.io.ICsvBeanReader;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.CsvContext;

/**
 * Utility class that loads and processes occurrence records read from a CSV file.
 * The expected columns in that file are:
 * "key","altitude","basisOfRecord","catalogNumber","classKey","clazz","collectionCode","dataProviderId",
 * "dataResourceId","datasetKey","depth","occurrenceId","family","familyKey","genus",
 * "genusKey","geospatialIssue","institutionCode","country","kingdom","kingdomKey","latitude","longitude","modified",
 * "occurrenceMonth","nubKey","occurrenceDate","order","orderKey","otherIssue",
 * "owningOrgKey","phylum","phylumKey","resourceAccessPointId","scientificName","species","speciesKey","taxonomicIssue",
 * "unitQualifier","occurrenceYear","locality","county","stateProvince","continent",
 * "collectorName","identifierName", "identificationDate".
 * Each cvs line is interpreted into an Occurrence object; to process each object a predicate or list of predicates are
 * passed as parameters to the function loadOccurrences.
 */
public class OccurrenceDataLoader {

  /**
   * Produces a Basis of Record instance.
   */
  private static class BasisOfRecordProcessor implements CellProcessor {

    @Override
    public BasisOfRecord execute(Object value, CsvContext context) {
      return BOR_CONVERTER.toEnum(Integer.parseInt((String) value));
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

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceDataLoader.class);

  // Date format used in the CSV file.
  private static final String DATE_FORMAT = "yyyy-MM-dd"; // Tue Nov 23 17:00:00 CST 1954


  // Basis of record converter instance.
  private static final BasisOfRecordConverter BOR_CONVERTER = new BasisOfRecordConverter();


  // List of processors, a processor is defined for each column
  private final static CellProcessor[] CELL_PROCESSORS = new CellProcessor[] {
    new ParseInt(), // key
    new Optional(new ParseInt()), // altitude
    new Optional(new BasisOfRecordProcessor()), // basisOfRecord
    new Optional(), // catalogNumber
    new Optional(new ParseInt()), // classKey
    new Optional(), // clazz
    new Optional(), // collectionCode
    new Optional(new ParseInt()), // dataProviderId
    new Optional(new ParseInt()), // dataResourceId
    new Optional(new UUIDProcessor()), // datasetKey
    new Optional(new ParseInt()),// depth
    new Optional(),// occurrenceId
    new Optional(),// family
    new Optional(new ParseInt()),// familyKey
    new Optional(),// genus
    new Optional(new ParseInt()),// genusKey
    new Optional(new ParseInt()),// geospatialIssue
    new Optional(),// institutionCode
    new Optional(new CountryProcessor()),// country
    new Optional(),// kingdom
    new Optional(new ParseInt()),// kingdomKey
    new Optional(new ParseDouble()),// latitude
    new Optional(new ParseDouble()),// longitude
    new Optional(new ParseDate(DATE_FORMAT)),// modified
    new Optional(new ParseInt()),// occurrenceMonth
    new Optional(new ParseInt()),// nubKey
    new Optional(new ParseDate(DATE_FORMAT)),// occurrenceDate
    new Optional(),// order
    new Optional(new ParseInt()),// orderKey
    new Optional(new ParseInt()),// otherIssue
    new Optional(new UUIDProcessor()),// owningOrgKey
    new Optional(),// phylum
    new Optional(new ParseInt()),// phylumKey
    new Optional(new ParseInt()),// resourceAccessPointId
    new Optional(),// scientificName
    new Optional(),// species
    new Optional(new ParseInt()),// speciesKey
    new Optional(new ParseInt()),// taxonomicIssue
    new Optional(),// unitQualifier
    new Optional(new ParseInt()),// occurrenceYear
    new Optional(),// locality
    new Optional(),// county
    new Optional(),// stateProvince
    new Optional(),// continent
    new Optional(),// collectorName
    new Optional(),// identifierName
    new Optional(new ParseDate(DATE_FORMAT))// identificationDate
  };


  // Column headers
  private final static String[] HEADER = new String[] {
    "key",
    "altitude",
    "basisOfRecord",
    "catalogNumber",
    "classKey",
    "clazz",
    "collectionCode",
    "dataProviderId",
    "dataResourceId",
    "datasetKey",
    "depth",
    "occurrenceId",
    "family",
    "familyKey",
    "genus",
    "genusKey",
    "geospatialIssue",
    "institutionCode",
    "country",
    "kingdom",
    "kingdomKey",
    "latitude",
    "longitude",
    "modified",
    "occurrenceMonth",
    "nubKey",
    "occurrenceDate",
    "order",
    "orderKey",
    "otherIssue",
    "owningOrgKey",
    "phylum",
    "phylumKey",
    "resourceAccessPointId",
    "scientificName",
    "species",
    "speciesKey",
    "taxonomicIssue",
    "unitQualifier",
    "occurrenceYear",
    "locality",
    "county",
    "stateProvince",
    "continent",
    "collectorName",
    "identifierName",
    "identificationDate"
  };


  /**
   * Reads a CSV file and produces occurrence records for each line.
   * Each occurrence object is processed by the list of processors.
   * 
   * @param fileName CSV file
   * @param processors list of processors(predicates) that consume occurrence objects
   */
  public static void processOccurrences(String fileName, Predicate<Occurrence>... processors) {
    ICsvBeanReader reader = null;
    int line = 1;
    try {
      reader =
        new CsvBeanReader(new FileReader(new File(Resources.getResource(fileName).toURI())),
          CsvPreference.STANDARD_PREFERENCE);
      reader.getHeader(true);
      Occurrence occurrence;
      while ((occurrence = reader.read(Occurrence.class, HEADER, CELL_PROCESSORS)) != null) {
        for (Predicate<Occurrence> predicate : processors) {
          predicate.apply(occurrence);
        }
        line++;
      }
    } catch (Exception e) {
      LOG.error(String.format("Error parsing occurrence object from file %d", line), e);
    } finally {
      Closeables.closeQuietly(reader);
    }
  }
}
