package org.gbif.occurrence.processor.identifiers;

import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.api.vocabulary.OccurrenceSchemaType;

import javax.annotation.Nullable;

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A simple class to determine whether triplets and/or occurrenceIds are valid as identifiers for the given dataset.
 */
public class IdentifierStrategy {

  private final boolean tripletsValid;
  private final boolean occurrenceIdsValid;

  public IdentifierStrategy(OccurrenceSchemaType schemaType, @Nullable DwcaValidationReport validationReport) {
    checkNotNull(schemaType, "schemaType can't be null");
    if (schemaType == OccurrenceSchemaType.DWCA) {
      checkNotNull(validationReport, "validationReport can't be null if schema is DWCA");
      checkNotNull(validationReport.getOccurrenceReport(), "validation.occurrenceReport can't be null if schema is DWCA");
    }
    tripletsValid = tripletsValid(schemaType, validationReport);
    occurrenceIdsValid = occurrenceIdsValid(schemaType, validationReport);
  }

  /**
   * For XML datasets triplets are always valid. For DwC-A datasets triplets are valid if there are more than 0 unique
   * triplets in the dataset, and exactly 0 triplets referenced by more than one record.
   */
  private static boolean tripletsValid(OccurrenceSchemaType schemaType, DwcaValidationReport validationReport) {
    boolean valid = true;
    if (schemaType == OccurrenceSchemaType.DWCA) {
      OccurrenceValidationReport report = validationReport.getOccurrenceReport();
      valid = report.getUniqueTriplets() > 0
              && report.getCheckedRecords() - report.getRecordsWithInvalidTriplets() == report.getUniqueTriplets();
    }

    return valid;
  }

  /**
   * For XML datasets occurrenceIds are always ok. For DwC-A datasets occurrenceIds are valid if each record has a unique
   * occurrenceId.
   */
  private static boolean occurrenceIdsValid(OccurrenceSchemaType schemaType, DwcaValidationReport validationReport) {
    boolean valid = true;
    if (schemaType == OccurrenceSchemaType.DWCA) {
      OccurrenceValidationReport report = validationReport.getOccurrenceReport();
      valid = report.getCheckedRecords() > 0 && report.getUniqueOccurrenceIds() == report.getCheckedRecords();
    }

    return valid;
  }

  public boolean isTripletsValid() {
    return tripletsValid;
  }

  public boolean isOccurrenceIdsValid() {
    return occurrenceIdsValid;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(tripletsValid, occurrenceIdsValid);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final IdentifierStrategy other = (IdentifierStrategy) obj;
    return Objects.equal(this.tripletsValid, other.tripletsValid) && Objects
      .equal(this.occurrenceIdsValid, other.occurrenceIdsValid);
  }
}
