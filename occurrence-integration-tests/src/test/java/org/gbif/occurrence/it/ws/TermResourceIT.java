package org.gbif.occurrence.it.ws;

import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.ws.resources.TermResource;

import java.util.List;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@ActiveProfiles("test")
@AutoConfigureMockMvc
@SpringBootTest(
  classes = OccurrenceWsItConfiguration.class,
  webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class TermResourceIT {

  private final TermResource termResource;

  @Autowired
  public TermResourceIT(TermResource termResource) {
    this.termResource = termResource;
  }

  /**
   * Test that all verbatim and interpreted Terms are returned by the termResource.getInterpretation methdd.
   */
  @Test
  public void testGetInterpretation() {
    List<TermResource.TermWrapper> terms =  termResource.getInterpretation();
    Assertions.assertNotNull(terms);
    Assertions.assertTrue(StreamSupport.stream(TermUtils.interpretedTerms().spliterator(), false)
                            .allMatch(t -> terms.stream().anyMatch(tw -> tw.getSimpleName().equals(t.simpleName()))));

    Assertions.assertTrue(StreamSupport.stream(TermUtils.verbatimTerms().spliterator(), false)
                            .allMatch(t -> terms.stream().anyMatch(tw -> tw.getSimpleName().equals(t.simpleName()))));

  }

}
