/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.ws.provider;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

import java.io.StringWriter;
import java.util.Date;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyAdvice;

/**
 * Custom {@link ResponseBodyAdvice} to serialize {@link Occurrence} in DarwinCore XML.
 * We do not use JAXB annotations to keep the distinction between the model and its XML representation.
 * It is also easier to manage properties like Country, List, Map.
 *
 */
public class OccurrenceDwcXMLConverter {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceDwcXMLConverter.class);

  private static final FastDateFormat FDF = DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT;

  /**
   * Transforms an {@link Occurrence} object into a byte[] representing a XML document.
   *
   * @param occurrence
   * @return the {@link Occurrence} as byte[]
   * @throws ResponseStatusException if something went wrong while generating the XML document
   */
  public static String occurrenceXMLAsString(Occurrence occurrence) throws ResponseStatusException {
    StringWriter result = new StringWriter();

    try {
      DwcXMLDocument dwcXMLDocument = DwcXMLDocument.newInstance(DwcTerm.Occurrence);

      appendIfNotNull(dwcXMLDocument, GbifTerm.gbifID, occurrence.getKey());

      //this may be not the most compact way to serialize an Occurrence (e.g. reflection) but
      //it gives more freedom to handle things like date and country fields
      appendIfNotNull(dwcXMLDocument, DwcTerm.basisOfRecord, occurrence.getBasisOfRecord());
      appendIfNotNull(dwcXMLDocument, DwcTerm.individualCount, occurrence.getIndividualCount());
      appendIfNotNull(dwcXMLDocument, DwcTerm.sex, occurrence.getSex());
      appendIfNotNull(dwcXMLDocument, DwcTerm.lifeStage, occurrence.getLifeStage());
      appendIfNotNull(dwcXMLDocument, DwcTerm.establishmentMeans, occurrence.getEstablishmentMeans());
      appendIfNotNull(dwcXMLDocument, DwcTerm.datasetID, occurrence.getDatasetID());
      appendIfNotNull(dwcXMLDocument, DwcTerm.datasetName, occurrence.getDatasetName());
      appendIfNotNull(dwcXMLDocument, DwcTerm.otherCatalogNumbers, occurrence.getOtherCatalogNumbers());
      appendIfNotNull(dwcXMLDocument, DwcTerm.identifiedBy, occurrence.getIdentifiedBy());
      appendIfNotNull(dwcXMLDocument, DwcTerm.recordedBy, occurrence.getRecordedBy());
      appendIfNotNull(dwcXMLDocument, DwcTerm.preparations, occurrence.getPreparations());
      appendIfNotNull(dwcXMLDocument, DwcTerm.samplingProtocol, occurrence.getSamplingProtocol());

      appendIfNotNull(dwcXMLDocument, GbifTerm.taxonKey, occurrence.getTaxonKey());
      appendIfNotNull(dwcXMLDocument, GbifTerm.kingdomKey, occurrence.getKingdomKey());
      appendIfNotNull(dwcXMLDocument, GbifTerm.phylumKey, occurrence.getPhylumKey());
      appendIfNotNull(dwcXMLDocument, GbifTerm.classKey, occurrence.getClassKey());
      appendIfNotNull(dwcXMLDocument, GbifTerm.orderKey, occurrence.getOrderKey());
      appendIfNotNull(dwcXMLDocument, GbifTerm.familyKey, occurrence.getOrderKey());
      appendIfNotNull(dwcXMLDocument, GbifTerm.genusKey, occurrence.getGenusKey());
      appendIfNotNull(dwcXMLDocument, GbifTerm.subgenusKey, occurrence.getSubgenusKey());
      appendIfNotNull(dwcXMLDocument, GbifTerm.speciesKey, occurrence.getSpeciesKey());

      dwcXMLDocument.append(DwcTerm.scientificName, occurrence.getScientificName());
      dwcXMLDocument.append(DwcTerm.kingdom, occurrence.getKingdom());
      dwcXMLDocument.append(DwcTerm.phylum, occurrence.getPhylum());
      dwcXMLDocument.append(DwcTerm.class_, occurrence.getClazz());
      dwcXMLDocument.append(DwcTerm.order, occurrence.getOrder());
      dwcXMLDocument.append(DwcTerm.family, occurrence.getFamily());
      dwcXMLDocument.append(DwcTerm.genus, occurrence.getGenus());
      dwcXMLDocument.append(DwcTerm.subgenus, occurrence.getSubgenus());
      dwcXMLDocument.append(GbifTerm.species, occurrence.getSpecies());

      dwcXMLDocument.append(DwcTerm.genericName, occurrence.getGenericName());
      dwcXMLDocument.append(DwcTerm.specificEpithet, occurrence.getSpecificEpithet());
      dwcXMLDocument.append(DwcTerm.infraspecificEpithet, occurrence.getInfraspecificEpithet());
      appendIfNotNull(dwcXMLDocument, DwcTerm.taxonRank, occurrence.getTaxonRank());

      dwcXMLDocument.append(DwcTerm.dateIdentified, toISODateTime(occurrence.getDateIdentified()));

      appendIfNotNull(dwcXMLDocument, DwcTerm.decimalLatitude, occurrence.getDecimalLatitude());
      appendIfNotNull(dwcXMLDocument, DwcTerm.decimalLongitude, occurrence.getDecimalLongitude());
      appendIfNotNull(dwcXMLDocument, GbifTerm.coordinateAccuracy, occurrence.getCoordinateAccuracy());
      appendIfNotNull(dwcXMLDocument, GbifTerm.elevation, occurrence.getElevation());
      appendIfNotNull(dwcXMLDocument, GbifTerm.elevationAccuracy, occurrence.getElevationAccuracy());
      appendIfNotNull(dwcXMLDocument, GbifTerm.depth, occurrence.getDepth());
      appendIfNotNull(dwcXMLDocument, GbifTerm.depthAccuracy, occurrence.getDepthAccuracy());

      appendIfNotNull(dwcXMLDocument, DwcTerm.continent, occurrence.getContinent());
      appendDwcCountry(dwcXMLDocument, occurrence.getCountry());
      dwcXMLDocument.append(DwcTerm.stateProvince, occurrence.getStateProvince());
      dwcXMLDocument.append(DwcTerm.waterBody, occurrence.getWaterBody());

      appendIfNotNull(dwcXMLDocument, DwcTerm.year, occurrence.getYear());
      appendIfNotNull(dwcXMLDocument, DwcTerm.month, occurrence.getMonth());
      appendIfNotNull(dwcXMLDocument, DwcTerm.day, occurrence.getDay());
      dwcXMLDocument.append(DwcTerm.eventDate, toISODateTime(occurrence.getEventDate()));
      appendIfNotNull(dwcXMLDocument, DwcTerm.typeStatus, occurrence.getTypeStatus());

      dwcXMLDocument.append(GbifTerm.typifiedName, occurrence.getTypifiedName());
      dwcXMLDocument.append(DcTerm.modified, toISODateTime(occurrence.getModified()));
      dwcXMLDocument.append(GbifTerm.lastInterpreted, toISODateTime(occurrence.getLastInterpreted()));
      appendIfNotNull(dwcXMLDocument, DcTerm.references, occurrence.getReferences());

      appendIfNotNull(dwcXMLDocument, GbifTerm.datasetKey, occurrence.getDatasetKey());
      //append(dwcXMLDocument, GbifTerm., occurrence.getPublishingOrgKey());

      appendIfNotNull(dwcXMLDocument, GbifTerm.protocol, occurrence.getProtocol());
      dwcXMLDocument.append(GbifTerm.lastCrawled, toISODateTime(occurrence.getLastCrawled()));
      dwcXMLDocument.append(GbifTerm.lastParsed, toISODateTime(occurrence.getLastParsed()));

      for (OccurrenceIssue issue : occurrence.getIssues()) {
        dwcXMLDocument.append(GbifTerm.issue, issue.toString());
      }

      // handle verbatim values
      for (Term term : occurrence.getVerbatimFields().keySet()) {
        dwcXMLDocument.tryAppend(term, occurrence.getVerbatimField(term));
      }

      Transformer transformer = TransformerFactory.newInstance().newTransformer();
      transformer.setOutputProperty(OutputKeys.INDENT, "yes");
      DOMSource source = new DOMSource(dwcXMLDocument.getDocument());
      transformer.transform(source, new StreamResult(result));
    } catch (ParserConfigurationException | TransformerException e) {
      LOG.error("Can't generate Dwc XML for Occurrence [{}]", occurrence);
      throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
    return result.toString();
  }

  private static void appendDwcCountry(DwcXMLDocument dwcXMLDocument, Country value) {
    if (value != null) {
      dwcXMLDocument.append(DwcTerm.countryCode, value.getIso2LetterCode());
      dwcXMLDocument.append(DwcTerm.country, value.getTitle());
    }
  }

  /**
   * Specific appendIfNotNull for the supported {@link Term} implementation {@link DwcTerm}.
   *
   * @param dwcXMLDocument
   * @param term
   * @param value nulls accepted and skipped
   */
  private static void appendIfNotNull(DwcXMLDocument dwcXMLDocument, DwcTerm term, Object value) {
    if (value != null) {
      dwcXMLDocument.append(term, value.toString());
    }
  }

  /**
   * Specific appendIfNotNull for the supported {@link Term} implementation {@link DcTerm}.
   *
   * @param dwcXMLDocument
   * @param term
   * @param value nulls accepted and skipped
   */
  private static void appendIfNotNull(DwcXMLDocument dwcXMLDocument, DcTerm term, Object value) {
    if (value != null) {
      dwcXMLDocument.append(term, value.toString());
    }
  }

  /**
   * Specific appendIfNotNull for the supported {@link Term} implementation {@link GbifTerm}.
   *
   * @param dwcXMLDocument
   * @param term
   * @param value nulls accepted and skipped
   */
  private static void appendIfNotNull(DwcXMLDocument dwcXMLDocument, GbifTerm term, Object value) {
    if (value != null) {
      dwcXMLDocument.append(term, value.toString());
    }

  }

  private static String toISODateTime(Date date) {
    if (date == null) {
      return null;
    }
    return FDF.format(date);
  }

}
