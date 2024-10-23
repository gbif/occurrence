package org.gbif.occurrence.search.es;

import java.util.List;
import java.util.Map;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.vocabulary.api.AddTagAction;
import org.gbif.vocabulary.api.ConceptListParams;
import org.gbif.vocabulary.api.ConceptView;
import org.gbif.vocabulary.api.DeprecateConceptAction;
import org.gbif.vocabulary.client.ConceptClient;
import org.gbif.vocabulary.model.Concept;
import org.gbif.vocabulary.model.Definition;
import org.gbif.vocabulary.model.HiddenLabel;
import org.gbif.vocabulary.model.Label;
import org.gbif.vocabulary.model.Tag;
import org.gbif.vocabulary.model.search.KeyNameResult;

public class ConceptClientMock implements ConceptClient {

  private static final List<Tag> CENOZOIC_TAGS =
      List.of(Tag.of("rank: Era"), Tag.of("startAge: 66.0"), Tag.of("endAge: 0"));
  private static final List<Tag> MESOZOIC_TAGS =
      List.of(Tag.of("rank: Era"), Tag.of("startAge: 251.902"), Tag.of("endAge: 66.0"));
  private static final List<Tag> PALEOZOIC_TAGS =
      List.of(Tag.of("rank: Era"), Tag.of("startAge: 538.8"), Tag.of("endAge: 251.902"));
  private static final List<Tag> NEOGENE_TAGS =
      List.of(Tag.of("rank: Period"), Tag.of("startAge: 23.03"), Tag.of("endAge: 2.58"));
  private static final List<Tag> QUATERNARY_TAGS =
      List.of(Tag.of("rank: Period"), Tag.of("startAge: 2.58"), Tag.of("endAge: 0"));
  private static final List<Tag> MIOCENE_TAGS =
      List.of(Tag.of("rank: Epoch"), Tag.of("startAge: 23.03"), Tag.of("endAge: 5.333"));
  private static final List<Tag> PLIOCENE_TAGS =
      List.of(Tag.of("rank: Epoch"), Tag.of("startAge: 5.333"), Tag.of("endAge: 2.58"));
  private static final List<Tag> BURDIGALIAN_TAGS =
      List.of(Tag.of("rank: Age"), Tag.of("startAge: 20.44"), Tag.of("endAge: 15.98"));
  private static final List<Tag> ZANCLEAN_TAGS =
      List.of(Tag.of("rank: Age"), Tag.of("startAge: 5.333"), Tag.of("endAge: 3.600"));

  private static final Map<String, List<Tag>> TAGS_MAP =
      Map.of(
          "cenozoic",
          CENOZOIC_TAGS,
          "mesozoic",
          MESOZOIC_TAGS,
          "paleozoic",
          PALEOZOIC_TAGS,
          "neogene",
          NEOGENE_TAGS,
          "quaternary",
          QUATERNARY_TAGS,
          "miocene",
          MIOCENE_TAGS,
          "pliocene",
          PLIOCENE_TAGS,
          "burdigalian",
          BURDIGALIAN_TAGS,
          "zanclean",
          ZANCLEAN_TAGS);

  @Override
  public PagingResponse<ConceptView> listConcepts(String s, ConceptListParams conceptListParams) {
    return null;
  }

  @Override
  public ConceptView get(String vocabularyName, String conceptName, boolean b, boolean b1) {
    return getConcept(conceptName);
  }

  @Override
  public ConceptView create(String s, Concept concept) {
    return null;
  }

  @Override
  public ConceptView update(String s, String s1, Concept concept) {
    return null;
  }

  @Override
  public List<KeyNameResult> suggest(String s, SuggestParams suggestParams) {
    return null;
  }

  @Override
  public void deprecate(String s, String s1, DeprecateConceptAction deprecateConceptAction) {}

  @Override
  public void restoreDeprecated(String s, String s1, boolean b) {}

  @Override
  public List<Definition> listDefinitions(String s, String s1, ListParams listParams) {
    return null;
  }

  @Override
  public Definition getDefinition(String s, String s1, long l) {
    return null;
  }

  @Override
  public Definition addDefinition(String s, String s1, Definition definition) {
    return null;
  }

  @Override
  public Definition updateDefinition(String s, String s1, long l, Definition definition) {
    return null;
  }

  @Override
  public void deleteDefinition(String s, String s1, long l) {}

  @Override
  public List<Tag> listTags(String s, String s1) {
    return null;
  }

  @Override
  public void addTag(String s, String s1, AddTagAction addTagAction) {}

  @Override
  public void removeTag(String s, String s1, String s2) {}

  @Override
  public List<Label> listLabels(String s, String s1, ListParams listParams) {
    return null;
  }

  @Override
  public PagingResponse<Label> listAlternativeLabels(String s, String s1, ListParams listParams) {
    return null;
  }

  @Override
  public PagingResponse<HiddenLabel> listHiddenLabels(
      String s, String s1, PagingRequest pagingRequest) {
    return null;
  }

  @Override
  public Long addLabel(String s, String s1, Label label) {
    return null;
  }

  @Override
  public Long addAlternativeLabel(String s, String s1, Label label) {
    return null;
  }

  @Override
  public Long addHiddenLabel(String s, String s1, HiddenLabel hiddenLabel) {
    return null;
  }

  @Override
  public void deleteLabel(String s, String s1, long l) {}

  @Override
  public void deleteAlternativeLabel(String s, String s1, long l) {}

  @Override
  public void deleteHiddenLabel(String s, String s1, long l) {}

  @Override
  public PagingResponse<ConceptView> listConceptsLatestRelease(
      String s, ConceptListParams conceptListParams) {
    return null;
  }

  @Override
  public ConceptView getFromLatestRelease(String s, String s1, boolean b, boolean b1) {
    return getConcept(s1);
  }

  @Override
  public List<Definition> listDefinitionsFromLatestRelease(
      String s, String s1, ListParams listParams) {
    return null;
  }

  @Override
  public List<Label> listLabelsFromLatestRelease(String s, String s1, ListParams listParams) {
    return null;
  }

  @Override
  public PagingResponse<Label> listAlternativeLabelsFromLatestRelease(
      String s, String s1, ListParams listParams) {
    return null;
  }

  @Override
  public PagingResponse<HiddenLabel> listHiddenLabelsFromLatestRelease(
      String s, String s1, ListParams listParams) {
    return null;
  }

  @Override
  public List<KeyNameResult> suggestLatestRelease(String s, SuggestParams suggestParams) {
    return null;
  }

  private ConceptView getConcept(String s) {
    if (!TAGS_MAP.containsKey(s)) {
      return null;
    }

    Concept concept = new Concept();
    concept.setName(s.toLowerCase());
    concept.setTags(TAGS_MAP.getOrDefault(concept.getName(), null));
    return new ConceptView(concept);
  }
}
