package org.gbif.occurrence.search.writers;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.occurrence.persistence.hbase.Columns;

import java.io.IOException;

import com.google.common.base.Predicate;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Utility class for writing occurrence records into an HBase table.
 */
public class HBasePredicateWriter implements Predicate<Occurrence> {


  // HBase table
  private final Table hTable;

  // Column family
  private final static byte[] CF = Bytes.toBytes("o");

  /**
   * Default constructor.
   */
  public HBasePredicateWriter(Table hTable) {
    this.hTable = hTable;
  }

  /**
   * Reads and processes the occurrence object.
   */
  @Override
  public boolean apply(Occurrence input) {
    try {
      write(input);
      return true;
    } catch (IOException e) {
      return false;
    }
  }


  /**
   * Writes the occurrence record into the hbase table.
   *
   * @param occ occurrence object that will be written to hbase
   */
  private void write(Occurrence occ) throws IOException {
    Put put = new Put(Bytes.toBytes(occ.getKey()));


    if (occ.getElevation() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.elevation)),
        Bytes.toBytes(occ.getElevation()));
    }

    if (occ.getBasisOfRecord() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.basisOfRecord)),
        Bytes.toBytes(occ.getBasisOfRecord().name()));
    }
    if (occ.getVerbatimField(DwcTerm.catalogNumber) != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.catalogNumber)),
        Bytes.toBytes(occ.getVerbatimField(DwcTerm.catalogNumber)));
    }

    if (occ.getClassKey() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.classKey)),
        Bytes.toBytes(occ.getClassKey()));
    }
    if (occ.getClazz() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.class_)),
        Bytes.toBytes(occ.getClazz()));
    }

    if (occ.getVerbatimField(DwcTerm.collectionCode) != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.collectionCode)),
        Bytes.toBytes(occ.getVerbatimField(DwcTerm.collectionCode)));
    }

    if (occ.getDatasetKey() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.datasetKey)),
        Bytes.toBytes(occ.getDatasetKey().toString()));
    }

    if (occ.getDepth() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.depth)),
        Bytes.toBytes(occ.getDepth()));
    }

    if (occ.getFamily() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.family)),
        Bytes.toBytes(occ.getFamily()));
    }

    if (occ.getFamilyKey() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.familyKey)),
        Bytes.toBytes(occ.getFamilyKey()));
    }

    if (occ.getGenus() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.genus)),
        Bytes.toBytes(occ.getGenus()));
    }

    if (occ.getGenusKey() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.genusKey)),
        Bytes.toBytes(occ.getGenusKey()));
    }


    if (occ.getVerbatimField(DwcTerm.institutionCode) != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.institutionCode)),
        Bytes.toBytes(occ.getVerbatimField(DwcTerm.institutionCode)));
    }

    if (occ.getKingdom() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.kingdom)),
        Bytes.toBytes(occ.getKingdom()));
    }

    if (occ.getKingdomKey() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.kingdomKey)),
        Bytes.toBytes(occ.getKingdomKey()));
    }

    if (occ.getDecimalLatitude() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.decimalLatitude)),
        Bytes.toBytes(occ.getDecimalLatitude()));
    }

    if (occ.getDecimalLongitude() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.decimalLongitude)),
        Bytes.toBytes(occ.getDecimalLongitude()));
    }

    if (occ.getVerbatimField(DwcTerm.locality) != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.locality)),
        Bytes.toBytes(occ.getVerbatimField(DwcTerm.locality)));
    }

    if (occ.getCountry() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.countryCode)),
        Bytes.toBytes(occ.getCountry().getIso2LetterCode()));
    }

    if (occ.getVerbatimField(DwcTerm.county) != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.county)),
        Bytes.toBytes(occ.getVerbatimField(DwcTerm.county)));
    }

    if (occ.getStateProvince() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.stateProvince)),
        Bytes.toBytes(occ.getStateProvince()));
    }

    if (occ.getContinent() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.continent)),
        Bytes.toBytes(occ.getContinent().name()));
    }

    if (occ.getVerbatimField(DwcTerm.recordedBy) != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.recordedBy)),
        Bytes.toBytes(occ.getVerbatimField(DwcTerm.recordedBy)));
    }

    if (occ.getVerbatimField(DwcTerm.identifiedBy) != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.identifiedBy)),
        Bytes.toBytes(occ.getVerbatimField(DwcTerm.identifiedBy)));
    }

    if (occ.getDateIdentified() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.dateIdentified)),
        Bytes.toBytes(occ.getDateIdentified().getTime()));
    }

    if (occ.getModified() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DcTerm.modified)),
        Bytes.toBytes(occ.getModified().getTime()));
    }

    if (occ.getMonth() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.month)),
        Bytes.toBytes(occ.getMonth()));
    }

    if (occ.getTaxonKey() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.taxonKey)),
        Bytes.toBytes(occ.getTaxonKey()));
    }

    if (occ.getEventDate() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.eventDate)),
        Bytes.toBytes(occ.getEventDate().getTime()));
    }

    if (occ.getOrder() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.order)),
        Bytes.toBytes(occ.getOrder()));
    }

    if (occ.getOrderKey() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.orderKey)),
        Bytes.toBytes(occ.getOrderKey()));
    }

    if (occ.getPublishingOrgKey() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifInternalTerm.publishingOrgKey)),
        Bytes.toBytes(occ.getPublishingOrgKey().toString()));
    }

    if (occ.getPhylum() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.phylum)),
        Bytes.toBytes(occ.getPhylum()));
    }

    if (occ.getPhylumKey() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.phylumKey)),
        Bytes.toBytes(occ.getPhylumKey()));
    }

    if (occ.getScientificName() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.scientificName)),
        Bytes.toBytes(occ.getScientificName()));
    }

    if (occ.getSpecies() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.specificEpithet)),
        Bytes.toBytes(occ.getSpecies()));
    }

    if (occ.getSpeciesKey() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.speciesKey)),
        Bytes.toBytes(occ.getSpeciesKey()));
    }

    if (occ.getYear() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.year)),
        Bytes.toBytes(occ.getYear()));
    }

    if (occ.getTypeStatus() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.typeStatus)),
        Bytes.toBytes(occ.getTypeStatus().name()));
    }

    if (occ.getEstablishmentMeans() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.establishmentMeans)),
              Bytes.toBytes(occ.getEstablishmentMeans().name()));
    }

    // OccurrenceIssues
    for (OccurrenceIssue issue : occ.getIssues()) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(issue)),
              Bytes.toBytes(1));
    }

    if (occ.getWaterBody() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.waterBody)),
                    Bytes.toBytes(occ.getWaterBody()));
    }

    if (occ.getVerbatimField(DwcTerm.organismID) != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.organismID)),
                    Bytes.toBytes(occ.getVerbatimField(DwcTerm.organismID)));
    }

    if (occ.getProtocol() != null) {
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.protocol)),
                    Bytes.toBytes(occ.getProtocol().name()));
    }

    hTable.put(put);
  }

}
