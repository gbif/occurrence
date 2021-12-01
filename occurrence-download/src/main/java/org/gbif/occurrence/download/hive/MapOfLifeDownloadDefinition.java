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
package org.gbif.occurrence.download.hive;

import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.ImmutableSet;

/**
 * Definition of the fields used in the MAP_OF_LIFE download format.
 */
public class MapOfLifeDownloadDefinition extends DownloadTerms {

  public static final Set<Pair<DownloadTerms.Group, Term>> MAP_OF_LIFE_DOWNLOAD_TERMS = ImmutableSet.of(
    // Identifiers
    Pair.of(Group.INTERPRETED, GbifTerm.gbifID),
    Pair.of(Group.INTERPRETED, GbifInternalTerm.publishingOrgKey),
    Pair.of(Group.INTERPRETED, GbifTerm.datasetKey),
    Pair.of(Group.INTERPRETED, DwcTerm.occurrenceID),

    // Taxonomy
    Pair.of(Group.INTERPRETED, DwcTerm.taxonomicStatus),
    Pair.of(Group.INTERPRETED, DwcTerm.kingdom),
    Pair.of(Group.INTERPRETED, DwcTerm.phylum),
    Pair.of(Group.INTERPRETED, DwcTerm.class_),
    Pair.of(Group.INTERPRETED, DwcTerm.order),
    Pair.of(Group.INTERPRETED, DwcTerm.family),
    Pair.of(Group.INTERPRETED, DwcTerm.genus),
    Pair.of(Group.INTERPRETED, DwcTerm.subgenus),
    Pair.of(Group.INTERPRETED, GbifTerm.species),
    Pair.of(Group.INTERPRETED, DwcTerm.specificEpithet),
    Pair.of(Group.INTERPRETED, DwcTerm.infraspecificEpithet),
    Pair.of(Group.INTERPRETED, DwcTerm.taxonRank),
    Pair.of(Group.INTERPRETED, DwcTerm.scientificName),
    Pair.of(Group.INTERPRETED, GbifTerm.acceptedScientificName),
    Pair.of(Group.VERBATIM, DwcTerm.verbatimTaxonRank),
    Pair.of(Group.VERBATIM, DwcTerm.scientificName),
    Pair.of(Group.VERBATIM, DwcTerm.scientificNameAuthorship),
    Pair.of(Group.VERBATIM, DwcTerm.vernacularName),

    // Taxonomy identifiers
    Pair.of(Group.INTERPRETED, GbifTerm.taxonKey),
    Pair.of(Group.INTERPRETED, GbifTerm.acceptedTaxonKey),
    Pair.of(Group.INTERPRETED, GbifTerm.speciesKey),

    // Location
    Pair.of(Group.INTERPRETED, DwcTerm.countryCode),
    Pair.of(Group.INTERPRETED, DwcTerm.locality),
    Pair.of(Group.INTERPRETED, DwcTerm.stateProvince),
    Pair.of(Group.INTERPRETED, DwcTerm.habitat),
    Pair.of(Group.INTERPRETED, DwcTerm.locationRemarks),

    // Observation
    Pair.of(Group.INTERPRETED, DwcTerm.occurrenceStatus),
    Pair.of(Group.INTERPRETED, DwcTerm.individualCount),

    // Precise location
    Pair.of(Group.INTERPRETED, GbifTerm.hasCoordinate),
    Pair.of(Group.INTERPRETED, DwcTerm.decimalLatitude),
    Pair.of(Group.INTERPRETED, DwcTerm.decimalLongitude),
    Pair.of(Group.INTERPRETED, DwcTerm.coordinateUncertaintyInMeters),
    Pair.of(Group.INTERPRETED, DwcTerm.coordinatePrecision),
    Pair.of(Group.INTERPRETED, GbifTerm.elevation),
    Pair.of(Group.INTERPRETED, GbifTerm.elevationAccuracy),
    Pair.of(Group.INTERPRETED, GbifTerm.depth),
    Pair.of(Group.INTERPRETED, GbifTerm.depthAccuracy),
    Pair.of(Group.INTERPRETED, GbifTerm.hasGeospatialIssues),
    Pair.of(Group.VERBATIM, DwcTerm.verbatimCoordinateSystem),
    Pair.of(Group.VERBATIM, DwcTerm.verbatimElevation),
    Pair.of(Group.VERBATIM, DwcTerm.verbatimDepth),
    Pair.of(Group.VERBATIM, DwcTerm.verbatimLocality),
    Pair.of(Group.VERBATIM, DwcTerm.verbatimSRS),

    // Collection event
    Pair.of(Group.INTERPRETED, DwcTerm.eventDate),
    Pair.of(Group.INTERPRETED, DwcTerm.eventTime),
    Pair.of(Group.INTERPRETED, DwcTerm.day),
    Pair.of(Group.INTERPRETED, DwcTerm.month),
    Pair.of(Group.INTERPRETED, DwcTerm.year),
    Pair.of(Group.INTERPRETED, DwcTerm.fieldNotes),
    Pair.of(Group.INTERPRETED, DwcTerm.eventRemarks),
    Pair.of(Group.VERBATIM, DwcTerm.verbatimEventDate),

    Pair.of(Group.INTERPRETED, DwcTerm.basisOfRecord),
    Pair.of(Group.INTERPRETED, DwcTerm.institutionCode),
    Pair.of(Group.INTERPRETED, DwcTerm.collectionCode),
    Pair.of(Group.INTERPRETED, DwcTerm.catalogNumber),
    Pair.of(Group.INTERPRETED, DwcTerm.recordNumber),

    // Identification
    Pair.of(Group.INTERPRETED, DwcTerm.identifiedBy),
    Pair.of(Group.INTERPRETED, DwcTerm.identifiedByID),
    Pair.of(Group.INTERPRETED, DwcTerm.dateIdentified),
    Pair.of(Group.INTERPRETED, DwcTerm.identificationRemarks),

    // Rights
    Pair.of(Group.INTERPRETED, DcTerm.license),
    Pair.of(Group.INTERPRETED, DcTerm.rightsHolder),
    Pair.of(Group.INTERPRETED, DwcTerm.recordedBy),

    Pair.of(Group.INTERPRETED, DwcTerm.typeStatus),
    Pair.of(Group.INTERPRETED, DwcTerm.establishmentMeans),

    // Sampling
    Pair.of(Group.INTERPRETED, DwcTerm.sampleSizeUnit),
    Pair.of(Group.INTERPRETED, DwcTerm.sampleSizeValue),
    Pair.of(Group.INTERPRETED, DwcTerm.samplingEffort),
    Pair.of(Group.INTERPRETED, DwcTerm.samplingProtocol),

    Pair.of(Group.INTERPRETED, GbifTerm.lastInterpreted),
    Pair.of(Group.INTERPRETED, GbifTerm.mediaType),
    Pair.of(Group.INTERPRETED, GbifTerm.issue)
  );
}
