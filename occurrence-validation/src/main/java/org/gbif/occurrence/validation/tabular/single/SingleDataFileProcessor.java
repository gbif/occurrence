package org.gbif.occurrence.validation.tabular.single;

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.validation.api.DataFile;
import org.gbif.occurrence.validation.api.DataFileProcessor;
import org.gbif.occurrence.validation.api.DataFileValidationResult;
import org.gbif.occurrence.validation.api.RecordProcessor;
import org.gbif.occurrence.validation.api.RecordSource;
import org.gbif.occurrence.validation.model.RecordInterpretionBasedEvaluationResult;
import org.gbif.occurrence.validation.tabular.RecordSourceFactory;
import org.gbif.occurrence.validation.util.TempTermsUtils;

import java.io.File;
import java.util.Map;

public class SingleDataFileProcessor implements DataFileProcessor {

  private final RecordProcessor recordProcessor;
  private final SimpleValidationCollector collector;

  public SingleDataFileProcessor(RecordProcessor recordProcessor) {
    this.recordProcessor = recordProcessor;
    collector = new SimpleValidationCollector();
  }

  @Override
  public DataFileValidationResult process(DataFile dataFile) {

    try( RecordSource recordSource = RecordSourceFactory.fromDelimited(new File(dataFile.getFileName()), dataFile.getDelimiterChar(),
            dataFile.isHasHeaders(), TempTermsUtils.buildTermMapping(dataFile.getColumns()))){
      RecordInterpretionBasedEvaluationResult result;
      Map<Term, String> record;
      while ((record = recordSource.read()) != null) {
        result = recordProcessor.process(record);
        collector.accumulate(result);
      }
      return new DataFileValidationResult(collector.getAggregatedResult());
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

}
