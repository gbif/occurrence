import java.io.File;

import org.gbif.api.model.occurrence.predicate.ConjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.occurrence.download.query.EsQueryVisitor;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;

public class InPredicateTest {


  public static void main(String[] args) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    ConjunctionPredicate p =
    mapper.readValue(new File("/Users/xrc439/dev/gbif/occurrence/occurrence-ws/src/test/resources/biquery.json"),
                     ConjunctionPredicate.class);
    BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
    new EsQueryVisitor().visit(p,queryBuilder);
    System.out.println(queryBuilder.toString());

  }
}
