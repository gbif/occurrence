package org.gbif.occurrence.validation.tabular;

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.validation.api.RecordSource;
import org.gbif.tabular.MappedTabularDataFileReader;
import org.gbif.tabular.MappedTabularDataLine;

import java.io.IOException;
import java.util.Map;

/**
 * Temporary class
 */
public class TabularFileReader implements RecordSource {

  private MappedTabularDataFileReader<Term> wrapped;

  public TabularFileReader(MappedTabularDataFileReader<Term> wrapped){
    this.wrapped = wrapped;
  }

  @Override
  public Map<Term, String> read() throws IOException {
    MappedTabularDataLine<Term> row = wrapped.read();
    if(row != null) {
      return row.getMappedData();
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    wrapped.close();
  }
}
