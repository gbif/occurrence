package org.gbif.occurrence.search.es;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.junit.jupiter.api.Test;

public class VocabularyFieldTranslatorTest {

  @Test
  public void geoTimeTranslationTest() {
    Map<OccurrenceSearchParameter, Set<String>> params = new HashMap<>();
    params.put(OccurrenceSearchParameter.GEOLOGICAL_TIME, Set.of("mesozoic,cenozoic"));
    VocabularyFieldTranslator.translateVocabs(params, new ConceptClientMock());
    assertEquals(
        "0.0,251.902", params.get(OccurrenceSearchParameter.GEOLOGICAL_TIME).iterator().next());

    params.put(OccurrenceSearchParameter.GEOLOGICAL_TIME, Set.of("cenozoic,mesozoic"));
    VocabularyFieldTranslator.translateVocabs(params, new ConceptClientMock());
    assertEquals(
        "0.0,251.902", params.get(OccurrenceSearchParameter.GEOLOGICAL_TIME).iterator().next());

    params.put(OccurrenceSearchParameter.GEOLOGICAL_TIME, Set.of("neogene,*"));
    VocabularyFieldTranslator.translateVocabs(params, new ConceptClientMock());
    assertEquals(
        "*,23.03", params.get(OccurrenceSearchParameter.GEOLOGICAL_TIME).iterator().next());

    params.put(OccurrenceSearchParameter.GEOLOGICAL_TIME, Set.of("*,neogene"));
    VocabularyFieldTranslator.translateVocabs(params, new ConceptClientMock());
    assertEquals("2.58,*", params.get(OccurrenceSearchParameter.GEOLOGICAL_TIME).iterator().next());

    params.put(OccurrenceSearchParameter.GEOLOGICAL_TIME, Set.of("burdigalian,pliocene"));
    VocabularyFieldTranslator.translateVocabs(params, new ConceptClientMock());
    assertEquals(
        "2.58,20.44", params.get(OccurrenceSearchParameter.GEOLOGICAL_TIME).iterator().next());

    params.put(OccurrenceSearchParameter.GEOLOGICAL_TIME, Set.of("neogene,cenozoic"));
    assertThrows(
        IllegalArgumentException.class,
        () -> VocabularyFieldTranslator.translateVocabs(params, new ConceptClientMock()));

    params.put(OccurrenceSearchParameter.GEOLOGICAL_TIME, Set.of("cenozoic,neogene"));
    assertThrows(
        IllegalArgumentException.class,
        () -> VocabularyFieldTranslator.translateVocabs(params, new ConceptClientMock()));

    params.put(OccurrenceSearchParameter.GEOLOGICAL_TIME, Set.of("miocene"));
    VocabularyFieldTranslator.translateVocabs(params, new ConceptClientMock());
    assertEquals("23.03", params.get(OccurrenceSearchParameter.GEOLOGICAL_TIME).iterator().next());

    params.put(OccurrenceSearchParameter.GEOLOGICAL_TIME, Set.of("neogene,foo"));
    assertThrows(
        IllegalArgumentException.class,
        () -> VocabularyFieldTranslator.translateVocabs(params, new ConceptClientMock()));
  }
}
