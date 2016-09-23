package org.gbif.occurrence.validation.api;

import org.gbif.dwc.terms.Term;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * Interface representing a source of records (file, map ...)
 */
public interface RecordSource extends Closeable {
  Map<Term, String> read() throws IOException;
}
