package org.gbif.occurrence.search.writers;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.occurrence.persistence.hbase.Columns;

import java.io.IOException;

import com.google.common.base.Predicate;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Utility class for writing occurrence records into an HBase table.
 */
public class HBaseOccurrenceWriter implements Predicate<Occurrence> {


  // HBase table
  private final HTableInterface hTable;

  // Column family
  private final static byte[] CF = Bytes.toBytes("o");

  /**
   * Default constructor.
   */
  public HBaseOccurrenceWriter(HTableInterface hTable) {
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
      put.add(CF, Bytes.toBytes(Columns.column(GbifTerm.elevation)),
        Bytes.toBytes(occ.getElevation()));
    }

    if (occ.getBasisOfRecord() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(DwcTerm.basisOfRecord)),
        Bytes.toBytes(occ.getBasisOfRecord().name()));
    }
    if (occ.getVerbatimField(DwcTerm.catalogNumber) != null) {
      put.add(CF, Bytes.toBytes(Columns.column(DwcTerm.catalogNumber)),
        Bytes.toBytes(occ.getVerbatimField(DwcTerm.catalogNumber)));
    }

    if (occ.getClassKey() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(GbifTerm.classKey)),
        Bytes.toBytes(occ.getClassKey()));
    }
    if (occ.getClazz() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(DwcTerm.class_)),
        Bytes.toBytes(occ.getClazz()));
    }

    if (occ.getVerbatimField(DwcTerm.collectionCode) != null) {
      put.add(CF, Bytes.toBytes(Columns.column(DwcTerm.collectionCode)),
        Bytes.toBytes(occ.getVerbatimField(DwcTerm.collectionCode)));
    }

    if (occ.getDatasetKey() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(GbifTerm.datasetKey)),
        Bytes.toBytes(occ.getDatasetKey().toString()));
    }

    if (occ.getDepth() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(GbifTerm.depth)),
        Bytes.toBytes(occ.getDepth()));
    }

    if (occ.getFamily() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(DwcTerm.family)),
        Bytes.toBytes(occ.getFamily()));
    }

    if (occ.getFamilyKey() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(GbifTerm.familyKey)),
        Bytes.toBytes(occ.getFamilyKey()));
    }

    if (occ.getGenus() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(DwcTerm.genus)),
        Bytes.toBytes(occ.getGenus()));
    }

    if (occ.getGenusKey() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(GbifTerm.genusKey)),
        Bytes.toBytes(occ.getGenusKey()));
    }

    // TODO geospatial issue has changed a lot
    // if (occ.getGeospatialIssue() != null) {
    // put.add(CF, Bytes.toBytes(ColumnUtil.column(FieldName.I_GEOSPATIAL_ISSUE)),
    // Bytes.toBytes(occ.getGeospatialIssue()));
    // }

    if (occ.getVerbatimField(DwcTerm.institutionCode) != null) {
      put.add(CF, Bytes.toBytes(Columns.column(DwcTerm.institutionCode)),
        Bytes.toBytes(occ.getVerbatimField(DwcTerm.institutionCode)));
    }

    if (occ.getKingdom() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(DwcTerm.kingdom)),
        Bytes.toBytes(occ.getKingdom()));
    }

    if (occ.getKingdomKey() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(GbifTerm.kingdomKey)),
        Bytes.toBytes(occ.getKingdomKey()));
    }

    if (occ.getDecimalLatitude() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(DwcTerm.decimalLatitude)),
        Bytes.toBytes(occ.getDecimalLatitude()));
    }

    if (occ.getDecimalLongitude() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(DwcTerm.decimalLongitude)),
        Bytes.toBytes(occ.getDecimalLongitude()));
    }

    if (occ.getVerbatimField(DwcTerm.locality) != null) {
      put.add(CF, Bytes.toBytes(Columns.column(DwcTerm.locality)),
        Bytes.toBytes(occ.getVerbatimField(DwcTerm.locality)));
    }

    if (occ.getCountry() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(DwcTerm.countryCode)),
        Bytes.toBytes(occ.getCountry().getIso2LetterCode()));
    }

    if (occ.getVerbatimField(DwcTerm.county) != null) {
      put.add(CF, Bytes.toBytes(Columns.column(DwcTerm.county)),
        Bytes.toBytes(occ.getVerbatimField(DwcTerm.county)));
    }

    if (occ.getStateProvince() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(DwcTerm.stateProvince)),
        Bytes.toBytes(occ.getStateProvince()));
    }

    if (occ.getContinent() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(DwcTerm.continent)),
        Bytes.toBytes(occ.getContinent().name()));
    }

    if (occ.getVerbatimField(DwcTerm.recordedBy) != null) {
      put.add(CF, Bytes.toBytes(Columns.column(DwcTerm.recordedBy)),
        Bytes.toBytes(occ.getVerbatimField(DwcTerm.recordedBy)));
    }

    if (occ.getVerbatimField(DwcTerm.identifiedBy) != null) {
      put.add(CF, Bytes.toBytes(Columns.column(DwcTerm.identifiedBy)),
        Bytes.toBytes(occ.getVerbatimField(DwcTerm.identifiedBy)));
    }

    if (occ.getDateIdentified() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(DwcTerm.dateIdentified)),
        Bytes.toBytes(occ.getDateIdentified().getTime()));
    }

    if (occ.getModified() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(DcTerm.modified)),
        Bytes.toBytes(occ.getModified().getTime()));
    }

    if (occ.getMonth() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(DwcTerm.month)),
        Bytes.toBytes(occ.getMonth()));
    }

    if (occ.getTaxonKey() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(GbifTerm.taxonKey)),
        Bytes.toBytes(occ.getTaxonKey()));
    }

    if (occ.getEventDate() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(DwcTerm.eventDate)),
        Bytes.toBytes(occ.getEventDate().getTime()));
    }

    if (occ.getOrder() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(DwcTerm.order)),
        Bytes.toBytes(occ.getOrder()));
    }

    if (occ.getOrderKey() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(GbifTerm.orderKey)),
        Bytes.toBytes(occ.getOrderKey()));
    }

    if (occ.getPublishingOrgKey() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(GbifInternalTerm.publishingOrgKey)),
        Bytes.toBytes(occ.getPublishingOrgKey().toString()));
    }

    if (occ.getPhylum() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(DwcTerm.phylum)),
        Bytes.toBytes(occ.getPhylum()));
    }

    if (occ.getPhylumKey() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(GbifTerm.phylumKey)),
        Bytes.toBytes(occ.getPhylumKey()));
    }

    if (occ.getScientificName() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(DwcTerm.scientificName)),
        Bytes.toBytes(occ.getScientificName()));
    }

    if (occ.getSpecies() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(DwcTerm.specificEpithet)),
        Bytes.toBytes(occ.getSpecies()));
    }

    if (occ.getSpeciesKey() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(GbifTerm.speciesKey)),
        Bytes.toBytes(occ.getSpeciesKey()));
    }

    // TODO: deprecated
    // if (occ.getTaxonomicIssue() != null) {
    // put.add(CF, Bytes.toBytes(ColumnUtil.column(FieldName.I_TAXONOMIC_ISSUE)),
    // Bytes.toBytes(occ.getTaxonomicIssue()));
    // }

    // TODO: deprecated
    // if (occ.getUnitQualifier() != null) {
    // put.add(CF, Bytes.toBytes(ColumnUtil.column(FieldName.UNIT_QUALIFIER)),
    // Bytes.toBytes(occ.getUnitQualifier()));
    // }

    if (occ.getYear() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(DwcTerm.year)),
        Bytes.toBytes(occ.getYear()));
    }

    if (occ.getTypeStatus() != null) {
      put.add(CF, Bytes.toBytes(Columns.column(DwcTerm.typeStatus)),
        Bytes.toBytes(occ.getTypeStatus().name()));
    }

    hTable.put(put);
    hTable.flushCommits();
  }

}
