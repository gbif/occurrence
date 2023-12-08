package org.gbif.occurrence.downloads.launcher.services.launcher.oozie;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.gbif.api.model.predicate.CompoundPredicate;
import org.gbif.api.model.predicate.DisjunctionPredicate;
import org.gbif.api.model.predicate.EqualsPredicate;
import org.gbif.api.model.predicate.InPredicate;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.junit.Test;

public class PredicateOptimizerTest {

  @Test
  public void testOptimizer() {
    EqualsPredicate equalsPredicate1 =
        new EqualsPredicate(OccurrenceSearchParameter.TAXON_KEY, "1", false);
    EqualsPredicate equalsPredicate2 =
        new EqualsPredicate(OccurrenceSearchParameter.TAXON_KEY, "2", false);
    EqualsPredicate equalsPredicate3 =
        new EqualsPredicate(OccurrenceSearchParameter.TAXON_KEY, "3", false);

    List<Predicate> equalsPredicates =
        Arrays.asList(equalsPredicate1, equalsPredicate2, equalsPredicate3);
    DisjunctionPredicate disjunctionPredicate = new DisjunctionPredicate(equalsPredicates);

    Predicate optimizedPredicate = PredicateOptimizer.optimize(disjunctionPredicate);

    assertTrue(optimizedPredicate instanceof CompoundPredicate);
    CompoundPredicate compoundPredicate = (CompoundPredicate) optimizedPredicate;

    assertEquals(1, compoundPredicate.getPredicates().size());

    compoundPredicate
        .getPredicates()
        .forEach(
            p -> {
              assertTrue(p instanceof InPredicate);
              InPredicate inPredicate = (InPredicate) p;
              assertEquals(OccurrenceSearchParameter.TAXON_KEY, inPredicate.getKey());
              assertEquals(3, inPredicate.getValues().size());
              assertTrue(inPredicate.getValues().contains("1"));
              assertTrue(inPredicate.getValues().contains("2"));
              assertTrue(inPredicate.getValues().contains("3"));
            });
  }
}
