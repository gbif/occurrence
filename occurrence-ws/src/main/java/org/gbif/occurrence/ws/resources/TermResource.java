package org.gbif.occurrence.ws.resources;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * List {@link Term} used in the context of an occurrence.
 *
 */
@RestController
@RequestMapping(
  value = "occurrence/term",
  produces = MediaType.APPLICATION_JSON_VALUE
)
public class TermResource {


  private static final List<TermWrapper>
    OCCURRENCE_TERMS = Stream.concat(StreamSupport.stream(TermUtils.interpretedTerms().spliterator(), false),
                                     StreamSupport.stream(TermUtils.verbatimTerms().spliterator(), false))
                                                            .map(TermWrapper::new)
                                                            .sorted(Comparator.comparing(TermWrapper::getSource)
                                                                      .thenComparing(TermWrapper::getGroup, Comparator.nullsLast(String::compareTo))
                                                                      .thenComparing(TermWrapper::getSimpleName))
                                                            .collect(Collectors.toList());
  @GetMapping
  public List<TermWrapper> getInterpretation() {
    return OCCURRENCE_TERMS;
  }


  public static class TermWrapper {

    private final String simpleName;
    private final String qualifiedName;
    private final String group;
    private final String source;

    public TermWrapper(Term term) {
      simpleName = term.simpleName();
      qualifiedName = term.qualifiedName();
      source = term.getClass().getSimpleName();

      // Not too clean but we can't override the Term's @JsonSerialize
      if (DwcTerm.class.equals(term.getClass())) {
        group = ((DwcTerm)term).getGroup();
      } else if (GbifTerm.class.equals(term.getClass())) {
        group = ((GbifTerm)term).getGroup();
      } else {
        group = null;
      }
    }

    public String getSimpleName() {
      return simpleName;
    }

    public String getQualifiedName() {
      return qualifiedName;
    }

    public String getGroup() {
      return group;
    }

    public String getSource() {
      return source;
    }

  }

}
