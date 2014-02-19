package org.gbif.occurrence.search.writers;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.persistence.api.InternalTerm;
import org.gbif.occurrence.persistence.hbase.HBaseFieldUtil;

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
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(GbifTerm.elevation).getColumnName()),
        Bytes.toBytes(occ.getElevation()));
    }

    if (occ.getBasisOfRecord() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(DwcTerm.basisOfRecord).getColumnName()),
        Bytes.toBytes(occ.getBasisOfRecord().name()));
    }
    if (occ.getVerbatimField(DwcTerm.catalogNumber) != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(DwcTerm.catalogNumber).getColumnName()),
        Bytes.toBytes(occ.getVerbatimField(DwcTerm.catalogNumber)));
    }

    if (occ.getClassKey() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(GbifTerm.classKey).getColumnName()),
        Bytes.toBytes(occ.getClassKey()));
    }
    if (occ.getClazz() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(DwcTerm.class_).getColumnName()),
        Bytes.toBytes(occ.getClazz()));
    }

    if (occ.getVerbatimField(DwcTerm.collectionCode) != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(DwcTerm.collectionCode).getColumnName()),
        Bytes.toBytes(occ.getVerbatimField(DwcTerm.collectionCode)));
    }

    // deprecated - remove
    // if (occ.getDataProviderId() != null) {
    // put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.DATA_PROVIDER_ID).getColumnName()),
    // Bytes.toBytes(occ.getDataProviderId()));
    // }
    //
    // if (occ.getDataResourceId() != null) {
    // put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.DATA_RESOURCE_ID).getColumnName()),
    // Bytes.toBytes(occ.getDataResourceId()));
    // }

    if (occ.getDatasetKey() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(GbifTerm.datasetKey).getColumnName()),
        Bytes.toBytes(occ.getDatasetKey().toString()));
    }

    if (occ.getDepth() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(GbifTerm.depth).getColumnName()),
        Bytes.toBytes(occ.getDepth()));
    }

    if (occ.getFamily() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(DwcTerm.family).getColumnName()),
        Bytes.toBytes(occ.getFamily()));
    }

    if (occ.getFamilyKey() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(GbifTerm.familyKey).getColumnName()),
        Bytes.toBytes(occ.getFamilyKey()));
    }

    if (occ.getGenus() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(DwcTerm.genus).getColumnName()),
        Bytes.toBytes(occ.getGenus()));
    }

    if (occ.getGenusKey() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(GbifTerm.genusKey).getColumnName()),
        Bytes.toBytes(occ.getGenusKey()));
    }

    // TODO geospatial issue has changed a lot
    // if (occ.getGeospatialIssue() != null) {
    // put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_GEOSPATIAL_ISSUE).getColumnName()),
    // Bytes.toBytes(occ.getGeospatialIssue()));
    // }

    if (occ.getVerbatimField(DwcTerm.institutionCode) != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(DwcTerm.institutionCode).getColumnName()),
        Bytes.toBytes(occ.getVerbatimField(DwcTerm.institutionCode)));
    }

    if (occ.getKingdom() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(DwcTerm.kingdom).getColumnName()),
        Bytes.toBytes(occ.getKingdom()));
    }

    if (occ.getKingdomKey() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(GbifTerm.kingdomKey).getColumnName()),
        Bytes.toBytes(occ.getKingdomKey()));
    }

    if (occ.getDecimalLatitude() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(DwcTerm.decimalLatitude).getColumnName()),
        Bytes.toBytes(occ.getDecimalLatitude()));
    }

    if (occ.getDecimalLongitude() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(DwcTerm.decimalLongitude).getColumnName()),
        Bytes.toBytes(occ.getDecimalLongitude()));
    }

    if (occ.getVerbatimField(DwcTerm.locality) != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(DwcTerm.locality).getColumnName()),
        Bytes.toBytes(occ.getVerbatimField(DwcTerm.locality)));
    }

    if (occ.getCountry() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(DwcTerm.countryCode).getColumnName()),
        Bytes.toBytes(occ.getCountry().getIso2LetterCode()));
    }

    if (occ.getVerbatimField(DwcTerm.county) != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(DwcTerm.county).getColumnName()),
        Bytes.toBytes(occ.getVerbatimField(DwcTerm.county)));
    }

    if (occ.getStateProvince() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(DwcTerm.stateProvince).getColumnName()),
        Bytes.toBytes(occ.getStateProvince()));
    }

    if (occ.getContinent() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(DwcTerm.continent).getColumnName()),
        Bytes.toBytes(occ.getContinent().name()));
    }

    if (occ.getVerbatimField(DwcTerm.recordedBy) != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(DwcTerm.recordedBy).getColumnName()),
        Bytes.toBytes(occ.getVerbatimField(DwcTerm.recordedBy)));
    }

    if (occ.getVerbatimField(DwcTerm.identifiedBy) != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(DwcTerm.identifiedBy).getColumnName()),
        Bytes.toBytes(occ.getVerbatimField(DwcTerm.identifiedBy)));
    }

    if (occ.getDateIdentified() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(DwcTerm.dateIdentified).getColumnName()),
        Bytes.toBytes(occ.getDateIdentified().getTime()));
    }

    if (occ.getModified() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(DcTerm.modified).getColumnName()),
        Bytes.toBytes(occ.getModified().getTime()));
    }

    if (occ.getMonth() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(DwcTerm.month).getColumnName()),
        Bytes.toBytes(occ.getMonth()));
    }

    if (occ.getTaxonKey() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(GbifTerm.taxonKey).getColumnName()),
        Bytes.toBytes(occ.getTaxonKey()));
    }

    if (occ.getEventDate() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(DwcTerm.eventDate).getColumnName()),
        Bytes.toBytes(occ.getEventDate().getTime()));
    }

    if (occ.getOrder() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(DwcTerm.order).getColumnName()),
        Bytes.toBytes(occ.getOrder()));
    }

    if (occ.getOrderKey() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(GbifTerm.orderKey).getColumnName()),
        Bytes.toBytes(occ.getOrderKey()));
    }

    // TODO: other issue now deprecated
    // if (occ.getOtherIssue() != null) {
    // put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_OTHER_ISSUE).getColumnName()),
    // Bytes.toBytes(occ.getOtherIssue()));
    // }

    if (occ.getPublishingOrgKey() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(InternalTerm.publishingOrgKey).getColumnName()),
        Bytes.toBytes(occ.getPublishingOrgKey().toString()));
    }

    if (occ.getPhylum() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(DwcTerm.phylum).getColumnName()),
        Bytes.toBytes(occ.getPhylum()));
    }

    if (occ.getPhylumKey() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(GbifTerm.phylumKey).getColumnName()),
        Bytes.toBytes(occ.getPhylumKey()));
    }

    // deprecated
    // if (occ.getResourceAccessPointId() != null) {
    // put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.RESOURCE_ACCESS_POINT_ID).getColumnName()),
    // Bytes.toBytes(occ.getResourceAccessPointId()));
    // }

    if (occ.getScientificName() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(DwcTerm.scientificName).getColumnName()),
        Bytes.toBytes(occ.getScientificName()));
    }

    if (occ.getSpecies() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(DwcTerm.specificEpithet).getColumnName()),
        Bytes.toBytes(occ.getSpecies()));
    }

    if (occ.getSpeciesKey() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(GbifTerm.speciesKey).getColumnName()),
        Bytes.toBytes(occ.getSpeciesKey()));
    }

    // TODO: deprecated
    // if (occ.getTaxonomicIssue() != null) {
    // put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_TAXONOMIC_ISSUE).getColumnName()),
    // Bytes.toBytes(occ.getTaxonomicIssue()));
    // }

    // TODO: deprecated
    // if (occ.getUnitQualifier() != null) {
    // put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.UNIT_QUALIFIER).getColumnName()),
    // Bytes.toBytes(occ.getUnitQualifier()));
    // }

    if (occ.getYear() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(DwcTerm.year).getColumnName()),
        Bytes.toBytes(occ.getYear()));
    }

    if (occ.getTypeStatus() != null) {
      put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(DwcTerm.typeStatus).getColumnName()),
        Bytes.toBytes(occ.getTypeStatus().name()));
    }

    hTable.put(put);
    hTable.flushCommits();
  }

}
