package org.gbif.occurrencestore.util;

import org.gbif.api.vocabulary.BasisOfRecord;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

/**
 * Converter turning a basis of record enum into an integer and vice versa.
 * The integer value is the one used by HBase persistence.
 */
public class BasisOfRecordConverter {

  // TODO: 4 used to be germplams and is hangover from mysql days - once the dictionary parser (gbif-parsers) is fixed
  // to return only enums and the only int handling is here, this class can hold 1:1 mapping enum:int.
  // See: GBIFCOM-141
  private final Map<Integer, BasisOfRecord> mapToEnum =
    ImmutableMap.<Integer, BasisOfRecord>builder()
      .put(0, BasisOfRecord.UNKNOWN)
      .put(1, BasisOfRecord.OBSERVATION)
      .put(2, BasisOfRecord.PRESERVED_SPECIMEN)
      .put(3, BasisOfRecord.LIVING_SPECIMEN)
      .put(4, BasisOfRecord.LIVING_SPECIMEN)
      .put(5, BasisOfRecord.FOSSIL_SPECIMEN)
      .put(6, BasisOfRecord.LITERATURE)
      .build();

  private final Map<BasisOfRecord, Integer> mapFromEnum =
    ImmutableMap.<BasisOfRecord, Integer>builder()
      .put(BasisOfRecord.UNKNOWN, 0)
      .put(BasisOfRecord.OBSERVATION, 1)
      .put(BasisOfRecord.PRESERVED_SPECIMEN, 2)
      .put(BasisOfRecord.LIVING_SPECIMEN, 3)
      .put(BasisOfRecord.FOSSIL_SPECIMEN, 5)
      .put(BasisOfRecord.LITERATURE, 6)
      .put(BasisOfRecord.HUMAN_OBSERVATION, 7)
      .put(BasisOfRecord.MACHINE_OBSERVATION, 8)
      .build();


  public BasisOfRecord toEnum(Integer key) {
    if (key == null) return BasisOfRecord.UNKNOWN;
    BasisOfRecord val = mapToEnum.get(key);
    return val == null ? BasisOfRecord.UNKNOWN : val;
  }

  public Integer fromEnum(BasisOfRecord value) {
    if (value == null) return null;
    if (mapFromEnum.containsKey(value)) {
      return mapFromEnum.get(value);
    }
    throw new IllegalArgumentException("Enumeration value " + value.name() + " unknown");
  }
}
