//package org.gbif.occurrence.trino.udf;
//
//import io.trino.metadata.InternalFunctionBundle;
//import io.trino.spi.type.VarcharType;
//import io.trino.sql.query.QueryAssertions;
//import org.junit.jupiter.api.AfterAll;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.Test;
//
//import static org.assertj.core.api.Assertions.assertThat;
//
//public class UdfsTest {
//
//  private static QueryAssertions assertions;
//
//  @BeforeAll
//  public static void setUp() {
//    assertions = new QueryAssertions();
//    assertions.addFunctions(InternalFunctionBundle.extractFunctions(new GbifUdfs().getFunctions()));
//  }
//
//  @AfterAll
//  public static void teardown() {
//    assertions.close();
//    assertions = null;
//  }
//
////  @Test
//  public void cleanDelimitersTest() {
//    //    assertions.function("cleanDelimiters", Collections.singletonList("test\n")).evaluate();
//    assertThat(assertions.function("cleanDelimiters", "'test\\n'"))
//        .hasType(VarcharType.createUnboundedVarcharType())
//        .isEqualTo("test");
//  }
//
//  @Test
//  public void basisOfRecordParseUdfTest() {
//    assertThat(assertions.function("parseBoR", "'foo'")).isEqualTo("OCCURRENCE");
//  }
//
////  @Test
//  public void speciesMatchUdfTest() {
//    assertThat(
//            assertions.function(
//                "nubLookup",
//                "'https://api.gbif-uat.org/v1/'",
//                "'Animalia'",
//                    "null",
//                    "null",
//                    "null",
//                    "null",
//                    "null",
//                    "null",
//                    "null",
//                    "null",
//                    "null"))
//        .isEqualTo("OCCURRENCE");
//  }
//}
