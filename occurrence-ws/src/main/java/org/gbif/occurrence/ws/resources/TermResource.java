package org.gbif.occurrence.ws.resources;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;

import java.util.Set;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.Singleton;

/**
 * List {@link Term} used in the context of an occurrence.
 *
 */
@Path("occurrence/term")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class TermResource {

  /**
   * Function to "wrap" a DwcTerm inside a TermWrapper.
   * @return a TermWrapper instance that contains a term
   */
  private static final Function<Term, TermWrapper> TO_TERM_WRAPPER = new Function<Term, TermWrapper>() {
    @Override
    public TermWrapper apply(@Nullable Term term) {
      return new TermWrapper(term);
    }
  };


  private static final Set<TermWrapper> OCCURRENCE_TERMS =
          ImmutableSet.copyOf(
                  Iterables.transform(ImmutableSet.copyOf(
                          ImmutableSet.<Term>builder()
                                  .addAll(TermUtils.interpretedTerms())
                                  .addAll(TermUtils.verbatimTerms())
                                  .build()), TO_TERM_WRAPPER));

  @GET
  public Set<TermWrapper> getInterpretation() {
    return OCCURRENCE_TERMS;
  }


  private static class TermWrapper {

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
