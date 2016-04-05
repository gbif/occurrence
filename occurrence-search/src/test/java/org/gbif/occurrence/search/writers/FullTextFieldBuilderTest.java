package org.gbif.occurrence.search.writers;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.Rank;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.search.writer.FullTextFieldBuilder;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

public class FullTextFieldBuilderTest {

  private Occurrence buildTestOccurrence(){
    Date now = new Date();
    Occurrence occurrence = new Occurrence();
    occurrence.setKey(1227769253);
    occurrence.setDatasetKey(UUID.fromString("50c9509d-22c7-4a22-a47d-8c48425ef4a7"));

    occurrence.setPublishingOrgKey(UUID.fromString("28eb1a3f-1c15-4a95-931a-4af90ecb574d"));
    occurrence.setPublishingCountry(Country.UNITED_STATES);
    occurrence.setProtocol(EndpointType.DWC_ARCHIVE);
    occurrence.setLastCrawled(now);
    occurrence.setLastParsed(now);
    occurrence.setBasisOfRecord(BasisOfRecord.HUMAN_OBSERVATION);
    occurrence.setTaxonKey(6505602);
    occurrence.setKingdomKey(1);
    occurrence.setPhylumKey(52);
    occurrence.setClassKey(225);
    occurrence.setOrderKey(982);
    occurrence.setFamilyKey(2693);
    occurrence.setGenusKey(2305748);
    occurrence.setSpeciesKey(6505602);
    occurrence.setScientificName("Columbella fuscata G.B. Sowerby I, 1832");
    occurrence.setKingdom("Animalia");
    occurrence.setPhylum("Mollusca");
    occurrence.setOrder("Neogastropoda");
    occurrence.setFamily("Columbellidae");
    occurrence.setGenus("Columbella");
    occurrence.setSpecies("Columbella fuscata");
    occurrence.setGenericName("Columbella");
    occurrence.setSpecificEpithet("fuscata");
    occurrence.setTaxonRank(Rank.SPECIES);
    occurrence.setDateIdentified(now);
    occurrence.setDecimalLongitude(-110.32959d);
    occurrence.setDecimalLatitude(24.32329d);
    occurrence.setYear(2016);
    occurrence.setMonth(1);
    occurrence.setDay(2);
    Map<Term,String> verbatimFields = new HashMap<>();
    verbatimFields.put(DwcTerm.geodeticDatum,"WGS84");
    verbatimFields.put(DwcTerm.class_,"Gastropoda");
    verbatimFields.put(DwcTerm.countryCode,"MX");
    verbatimFields.put(DwcTerm.country, "Mexico");
    verbatimFields.put(DcTerm.rightsHolder, "Alison Young");
    verbatimFields.put(DcTerm.identifier, "2544088");
    verbatimFields.put(DwcTerm.verbatimEventDate, "2016-01-01 15:28:43");
    verbatimFields.put(DwcTerm.datasetName, "iNaturalist research-grade observations");
    verbatimFields.put(GbifTerm.gbifID, "1227769253");
    verbatimFields.put(DwcTerm.verbatimLocality, "La Paz, Baja California Sur, México");
    verbatimFields.put(DwcTerm.collectionCode, "Observations");
    verbatimFields.put(DwcTerm.occurrenceID, "http://www.inaturalist.org/observations/2544088");
    verbatimFields.put(DwcTerm.taxonID, "328630");
    verbatimFields.put(DwcTerm.recordedBy, "Alison Young");
    verbatimFields.put(DwcTerm.catalogNumber, "2544088");
    verbatimFields.put(DwcTerm.institutionCode, "iNaturalist");
    verbatimFields.put(DcTerm.rights, "Copyright Alison Young, licensed under a Creative Commons cc_by_nc_sa_name license: http://creativecommons.org/licenses/by-nc-sa/3.0/");
    verbatimFields.put(DwcTerm.eventTime, "23:28:43Z");
    verbatimFields.put(DwcTerm.identificationID, "4737209");
    occurrence.setVerbatimFields(verbatimFields);
    return occurrence;
  }

  @Test
  public void fullTextFieldBuidlTest(){
    Occurrence occurrence = buildTestOccurrence();
    Set<String> fullTextValues = FullTextFieldBuilder.buildFullTextField(occurrence);
    Assert.assertNotNull(fullTextValues);
  }
}
