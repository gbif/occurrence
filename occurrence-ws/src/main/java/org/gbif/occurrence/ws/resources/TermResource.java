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
package org.gbif.occurrence.ws.resources;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
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
 */
@Tag(name = "Occurrences")
@RestController
@RequestMapping(
  value = "occurrence/term",
  produces = MediaType.APPLICATION_JSON_VALUE
)
public class TermResource {


  private static final List<TermWrapper>
    OCCURRENCE_TERMS = Stream.concat(StreamSupport.stream(TermUtils.interpretedTerms().spliterator(), false),
                                     StreamSupport.stream(TermUtils.verbatimTerms().spliterator(), false))
                                                            .distinct()
                                                            .map(TermWrapper::new)
                                                            .sorted(Comparator.comparing(TermWrapper::getSource)
                                                                      .thenComparing(TermWrapper::getGroup, Comparator.nullsLast(String::compareTo))
                                                                      .thenComparing(TermWrapper::getSimpleName))
                                                            .collect(Collectors.toList());

  @Operation(
    operationId = "occurrenceTerms",
    summary = "Occurrence terms",
    description = "Lists the definitions of the terms (JSON properties, field names) of occurrences.")
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
