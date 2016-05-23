package org.gbif.occurrence.ws.resources;

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;

import java.util.Set;
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

  private static Set<TermWrapper> OCCURRENCE_TERMS =
          ImmutableSet.copyOf(
                  Iterables.transform(ImmutableSet.copyOf(
                          ImmutableSet.<Term>builder()
                                  .addAll(TermUtils.interpretedTerms())
                                  .addAll(TermUtils.verbatimTerms())
                                  .build())
                          , buildTermToTermWrapperFunction())
          );

  @GET
  public Set<TermWrapper> getInterpretation() {
    return OCCURRENCE_TERMS;
  }

  /**
   * Function to "wrap" a DwcTerm inside a TermWrapper.
   * @return
   */
  private static Function<Term, TermWrapper> buildTermToTermWrapperFunction(){
    return new Function<Term, TermWrapper>() {
      @Override
      public TermWrapper apply(Term term) {
        return new TermWrapper(term);
      }
    };
  }

  private static class TermWrapper {

    private final String simpleName;
    private final String qualifiedName;

    public TermWrapper(Term term){
      simpleName = term.simpleName();
      qualifiedName = term.qualifiedName();
    }

    public String getSimpleName() {
      return simpleName;
    }

    public String getQualifiedName() {
      return qualifiedName;
    }
  }

}
