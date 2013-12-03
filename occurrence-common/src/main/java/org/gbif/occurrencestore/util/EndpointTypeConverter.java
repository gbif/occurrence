package org.gbif.occurrencestore.util;

import org.gbif.api.vocabulary.EndpointType;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

/**
 * Converter turning an EndpointType enum into an integer and vice versa.
 * The integer value is the one used by HBase persistence.
 */
public class EndpointTypeConverter {

  private final Map<Integer, EndpointType> mapToEnum =
    ImmutableMap.<Integer, EndpointType>builder()
      .put(0, EndpointType.BIOCASE)
      .put(1, EndpointType.DIGIR)
      .put(2, EndpointType.DIGIR_MANIS)
      .put(3, EndpointType.DWC_ARCHIVE)
      // here used to be DWC_ARCHIVE_OCCURRENCE & CHECKLIST (4 and 5)
      .put(6, EndpointType.EML)
      .put(7, EndpointType.FEED)
      .put(8, EndpointType.OAI_PMH)
      .put(9, EndpointType.OTHER)
      .put(10, EndpointType.TAPIR)
      .put(11, EndpointType.TCS_RDF)
      .put(12, EndpointType.TCS_XML)
      .put(13, EndpointType.WFS)
      .put(14, EndpointType.WMS)
      .build();

  private final Map<EndpointType, Integer> mapFromEnum =
    ImmutableMap.<EndpointType, Integer>builder()
      .put(EndpointType.BIOCASE, 0)
      .put(EndpointType.DIGIR, 1)
      .put(EndpointType.DIGIR_MANIS, 2)
      .put(EndpointType.DWC_ARCHIVE, 3)
        // here used to be DWC_ARCHIVE_OCCURRENCE & CHECKLIST (4 and 5)
      .put(EndpointType.EML, 6)
      .put(EndpointType.FEED, 7)
      .put(EndpointType.OAI_PMH, 8)
      .put(EndpointType.OTHER, 9)
      .put(EndpointType.TAPIR, 10)
      .put(EndpointType.TCS_RDF, 11)
      .put(EndpointType.TCS_XML, 12)
      .put(EndpointType.WFS, 13)
      .put(EndpointType.WMS, 14)
      .build();


  public Integer fromEnum(EndpointType value) {
    if (value == null) {
      return null;
    }
    if (mapFromEnum.containsKey(value)) {
      return mapFromEnum.get(value);
    }
    throw new IllegalArgumentException("Enumeration value " + value.name() + " unknown");
  }

  public EndpointType toEnum(Integer key) {
    return mapToEnum.get(key);
  }
}
