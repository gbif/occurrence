package org.gbif.occurrence.hive.udf;

import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ContainsUDFTest {

  @Test
  public void evaluateTest() {

    // Simple rectangle (has Rectangle JTS structure)
    assertTrue(contains(" POLYGON ((30 10, 30 20, 15 20, 15 10, 30 10))", 15.0, 20.0));
    assertFalse(contains("POLYGON ((30 10, 30 20, 15 20, 15 10, 30 10))", 45.0, 20.0));

    // Simple polygon
    assertTrue(contains(" POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))", 30.0, 20.0));
    assertFalse(contains("POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))", 45.0, 20.0));

    // Enormous meridian-centric rectangle
    assertTrue(contains(" POLYGON ((170 -80, 170 70, -160 70, -160 -80, 170 -80))", -10.0, -40.0));
    assertFalse(contains("POLYGON ((170 -80, 170 70, -160 70, -160 -80, 170 -80))", -175.0, -40.0));

    // Enormous meridian-centric polygon — requires extra points because DatelineRule.ccwRect only
    // The Spatial4J developer writes “Spatial4j does it this way because it seemed a real-world polygon wouldn't have
    // longitudinal points ≥ 180 degrees apart” https://github.com/locationtech/spatial4j/issues/46
    // We occasionally get these polygons (e.g. drawn around the Pacific Ocean), and the client needs to interpolote
    // them.
    //assertTrue(contains(" POLYGON ((170 -80, 170 70, -160 65, -160 -80, 170 -80))", -10.0, -10.0));
    //assertFalse(contains("POLYGON ((170 -80, 170 70, -160 65, -160 -80, 170 -80))", -175.0, -40.0));
    assertTrue(contains(" POLYGON ((170 -80, 170 70, 5 67.5, -160 65, -160 -80, 5 -80, 170 -80))", -10.0, -10.0));
    assertFalse(contains("POLYGON ((170 -80, 170 70, 5 67.5, -160 65, -160 -80, 5 -80, 170 -80))", -175.0, -40.0));

    assertTrue(contains("POLYGON((-153.28125 -68.2954,178.59375 -68.2954,178.59375 65.77395,-153.28125 65.77395,-153.28125 -68.2954))", 0.0, 0.0));

    // Simple rectangle around Taveuni
    assertTrue(contains(" POLYGON ((-179.75006 -17.12845, -179.75006 -16.60277, 179.78577 -16.60277, 179.78577 -17.12845, -179.75006 -17.12845))", -16.8, -180.0));
    assertTrue(contains(" POLYGON ((-179.75006 -17.12845, -179.75006 -16.60277, 179.78577 -16.60277, 179.78577 -17.12845, -179.75006 -17.12845))", -16.8, 180.0));
    assertFalse(contains("POLYGON ((-179.75006 -17.12845, -179.75006 -16.60277, 179.78577 -16.60277, 179.78577 -17.12845, -179.75006 -17.12845))", -26.8, -180.0));
    assertFalse(contains("POLYGON ((-179.75006 -17.12845, -179.75006 -16.60277, 179.78577 -16.60277, 179.78577 -17.12845, -179.75006 -17.12845))", -26.8, 180.0));

    // Simple polygon around Taveuni, as the Portal produces it
    assertTrue(contains(" POLYGON((-180.14832 -16.72643, -180.21423 -16.82899, -180.12085 -17.12058, -179.89838 -17.12845, -179.75006 -16.86054, -179.8764 -16.60277, -180.14832 -16.72643))", -16.8, -180.0));
    assertTrue(contains(" POLYGON((-180.14832 -16.72643, -180.21423 -16.82899, -180.12085 -17.12058, -179.89838 -17.12845, -179.75006 -16.86054, -179.8764 -16.60277, -180.14832 -16.72643))", -16.8, 180.0));
    assertFalse(contains("POLYGON((-180.14832 -16.72643, -180.21423 -16.82899, -180.12085 -17.12058, -179.89838 -17.12845, -179.75006 -16.86054, -179.8764 -16.60277, -180.14832 -16.72643))", -26.8, -180.0));
    assertFalse(contains("POLYGON((-180.14832 -16.72643, -180.21423 -16.82899, -180.12085 -17.12058, -179.89838 -17.12845, -179.75006 -16.86054, -179.8764 -16.60277, -180.14832 -16.72643))", -26.8, 180.0));

    // Simple polygon around Taveuni, as Wicket produces it
    assertTrue(contains(" POLYGON((179.85168 -16.72643, 179.78577 -16.82899, 179.87915 -17.12058, -179.89838 -17.12845, -179.75006 -16.86054, -179.8764 -16.60277, 179.85168 -16.72643))", -16.8, -180.0));
    assertTrue(contains(" POLYGON((179.85168 -16.72643, 179.78577 -16.82899, 179.87915 -17.12058, -179.89838 -17.12845, -179.75006 -16.86054, -179.8764 -16.60277, 179.85168 -16.72643))", -16.8, 180.0));
    assertFalse(contains("POLYGON((179.85168 -16.72643, 179.78577 -16.82899, 179.87915 -17.12058, -179.89838 -17.12845, -179.75006 -16.86054, -179.8764 -16.60277, 179.85168 -16.72643))", -26.8, -180.0));
    assertFalse(contains("POLYGON((179.85168 -16.72643, 179.78577 -16.82899, 179.87915 -17.12058, -179.89838 -17.12845, -179.75006 -16.86054, -179.8764 -16.60277, 179.85168 -16.72643))", -26.8, 180.0));

    // Multipolygon around Taveuni
    assertTrue(contains(" MULTIPOLYGON (((180 -16.658979090909092, 180 -17.12485513597339, 179.87915 -17.12058, 179.78577 -16.82899, 179.85168 -16.72643, 180 -16.658979090909092)), ((-180 -17.12485513597339, -180 -16.658979090909092, -179.8764 -16.60277, -179.75006 -16.86054, -179.89838 -17.12845, -180 -17.12485513597339)))", -16.8, -180.0));
    assertTrue(contains(" MULTIPOLYGON (((180 -16.658979090909092, 180 -17.12485513597339, 179.87915 -17.12058, 179.78577 -16.82899, 179.85168 -16.72643, 180 -16.658979090909092)), ((-180 -17.12485513597339, -180 -16.658979090909092, -179.8764 -16.60277, -179.75006 -16.86054, -179.89838 -17.12845, -180 -17.12485513597339)))", -16.8, 180.0));
    assertFalse(contains("MULTIPOLYGON (((180 -16.658979090909092, 180 -17.12485513597339, 179.87915 -17.12058, 179.78577 -16.82899, 179.85168 -16.72643, 180 -16.658979090909092)), ((-180 -17.12485513597339, -180 -16.658979090909092, -179.8764 -16.60277, -179.75006 -16.86054, -179.89838 -17.12845, -180 -17.12485513597339)))", -26.8, -180.0));
    assertFalse(contains("MULTIPOLYGON (((180 -16.658979090909092, 180 -17.12485513597339, 179.87915 -17.12058, 179.78577 -16.82899, 179.85168 -16.72643, 180 -16.658979090909092)), ((-180 -17.12485513597339, -180 -16.658979090909092, -179.8764 -16.60277, -179.75006 -16.86054, -179.89838 -17.12845, -180 -17.12485513597339)))", -26.8, 180.0));

    // Enormous antimeridian-centric rectangle
    assertTrue(contains(" POLYGON ((-20 -80, -20 75, 30 75, 30 -80, -20 -80))", 0.0, 180.0));
    assertTrue(contains(" POLYGON ((-20 -80, -20 75, 30 75, 30 -80, -20 -80))", 0.0, -180.0));

    // Enormous antimeridian-centric polygon — requires extra points because DatelineRule.ccwRect (NB *rect*) only
    // applies to rectangles.
    //assertTrue(contains(" POLYGON ((-20 -80, -20 75, -140 60, 100 80, 30 70, 30 -80, 100 -60, -140 -80, -20 -80))", 0.0, 180.0));
    //assertTrue(contains(" POLYGON ((-20 -80, -20 75, -140 60, 100 80, 30 70, 30 -80, 100 -60, -140 -80, -20 -80))", 0.0, -180.0));
    assertTrue(contains(" POLYGON ((-20 -80, -20 75, -140 60, 160 70, 100 80, 30 70, 30 -80, 100 -60, 160 -70, -140 -80, -20 -80))", 0.0, 170.0));
    assertTrue(contains(" POLYGON ((-20 -80, -20 75, -140 60, 160 70, 100 80, 30 70, 30 -80, 100 -60, 160 -70, -140 -80, -20 -80))", 0.0, -170.0));

    // Rectangle around the Pacific (original motivation for these tests, https://github.com/gbif/portal-feedback/issues/2272)
    assertTrue(contains(" POLYGON ((-78.13477 -50.88867, -78.13477 49.13086, 105.0293 49.13086, 105.0293 -50.88867, -78.13477 -50.88867))", 10.0, 170.0));
    assertFalse(contains("POLYGON ((-78.13477 -50.88867, -78.13477 49.13086, 105.0293 49.13086, 105.0293 -50.88867, -78.13477 -50.88867))", 10.0, 10.0));
  }

  private boolean contains(String geom, Double latitude, Double longitude) {
    return new ContainsUDF().evaluate(new Text(geom), latitude, longitude).get();
  }
}
