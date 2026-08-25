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
package org.gbif.occurrence.download.file.dwca;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.dwc.terms.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DwcaDownloadWorkerTest {

  @Test
  public void testNormalizeExtensionField() {

    DwcaDownloadWorker<VerbatimOccurrence, OccurrenceSearchParameter> worker = new DwcaDownloadWorker<>(null, null, null);

    UUID datasetKey = UUID.randomUUID();
    VerbatimOccurrence record =  new VerbatimOccurrence();
    record.setKey(1L);
    record.setDatasetKey(datasetKey);

    Map<Term, String> terms = new LinkedHashMap<>();
    terms.put(AcTerm.subjectCategoryVocabulary, "Specimen/Object");
    terms.put(XmpRightsTerm.UsageTerms, "https://creativecommons.org/publicdomain/zero/1.0/");
    terms.put(DwcTerm.scientificName, "Haloxylon ammodendron");
    terms.put(DcTerm.identifier, "http://n2t.net/ark:/65665/m36c5a41f4-7c60-43f2-ba1d-0860880efacc");
    terms.put(ExifTerm.PixelXDimension, "6729");
    terms.put(XmpRightsTerm.WebStatement, "https://naturalhistory.si.edu/research/nmnh-collections/museum-collections-policies");
    terms.put(DcElement.source, "US National Herbarium, Department of Botany, NMNH, Smithsonian Institution");
    terms.put(DcElement.creator, "Conveyor Belt");
    terms.put(AcTerm.providerLiteral, "Smithsonian Institution, NMNH, Botany");
    terms.put(DcElement.format, "image/jpeg");
    terms.put(AcTerm.licenseLogoURL, "https://www.si.edu/sites/default/files/icons/cc0.svg");
    terms.put(DcElement.rights, "CC0");
    terms.put(DcTerm.description, "Barcode 03538523");
    terms.put(DcTerm.rights, "CC0");
    terms.put(AcTerm.accessURI, "https://collections.nmnh.si.edu/media/?i=14034501&h=2000");
    terms.put(DcElement.type, "image");
    terms.put(ExifTerm.PixelYDimension, "8985");
    terms.put(DcTerm.title, "03538523.tif");

    Map<String,String> result = worker.toExtensionRecord(terms, record);

    Map<String, String> expected = new LinkedHashMap<>();
    expected.put(GbifTerm.gbifID.simpleName().toLowerCase(), "1");
    expected.put(GbifTerm.datasetKey.simpleName().toLowerCase(), datasetKey.toString());
    expected.put(AcTerm.subjectCategoryVocabulary.simpleName().toLowerCase(), "Specimen/Object");
    expected.put(XmpRightsTerm.UsageTerms.simpleName().toLowerCase(), "https://creativecommons.org/publicdomain/zero/1.0/");
    expected.put(DwcTerm.scientificName.simpleName().toLowerCase(), "Haloxylon ammodendron");
    expected.put(DcTerm.identifier.simpleName().toLowerCase(), "http://n2t.net/ark:/65665/m36c5a41f4-7c60-43f2-ba1d-0860880efacc");
    expected.put(ExifTerm.PixelXDimension.simpleName().toLowerCase(), "6729");
    expected.put(XmpRightsTerm.WebStatement.simpleName().toLowerCase(), "https://naturalhistory.si.edu/research/nmnh-collections/museum-collections-policies");
    expected.put(DcElement.source.simpleName().toLowerCase(), "US National Herbarium, Department of Botany, NMNH, Smithsonian Institution");
    expected.put(DcElement.creator.simpleName().toLowerCase(), "Conveyor Belt");
    expected.put(AcTerm.providerLiteral.simpleName().toLowerCase(), "Smithsonian Institution, NMNH, Botany");
    expected.put("format", "image/jpeg");
    expected.put(AcTerm.licenseLogoURL.simpleName().toLowerCase(), "https://www.si.edu/sites/default/files/icons/cc0.svg");
    expected.put("dc_rights", "CC0"); // this one wins
    expected.put(DcTerm.description.simpleName().toLowerCase(), "Barcode 03538523");
    expected.put("dcterms_rights", "CC0"); // this one win
    expected.put(AcTerm.accessURI.simpleName().toLowerCase(), "https://collections.nmnh.si.edu/media/?i=14034501&h=2000");
    expected.put(DcElement.type.simpleName().toLowerCase(), "image");
    expected.put(ExifTerm.PixelYDimension.simpleName().toLowerCase(), "8985");
    expected.put(DcTerm.title.simpleName().toLowerCase(), "03538523.tif");

    assertEquals(expected, result);
  }
}
