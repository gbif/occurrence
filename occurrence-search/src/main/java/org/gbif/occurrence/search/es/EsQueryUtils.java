package org.gbif.occurrence.search.es;

class EsQueryUtils {

  private EsQueryUtils() {}

  // ES fields for queries
  static final String QUERY = "query";
  static final String BOOL = "bool";
  static final String MUST = "must";
  static final String MATCH = "match";
  static final String TERM = "term";
  static final String SHOULD = "should";
  static final String FILTER = "filter";
  static final String LOCATION = "location";
  static final String DISTANCE = "distance";
  static final String GEO_DISTANCE = "geo_distance";
  static final String GEO_BOUNDING_BOX = "geo_bounding_box";
  static final String LAT = "lat";
  static final String LON = "lon";
  static final String TOP_LEFT = "top_left";
  static final String BOTTOM_RIGHT = "bottom_rigth";

  // cords constants
  static final double MIN_DIFF = 0.000001;
  static final double MIN_LON = -180;
  static final double MAX_LON = 180;
  static final double MIN_LAT = -90;
  static final double MAX_LAT = 90;
}
