package org.gbif.occurrence.parsing.dwca;

import org.gbif.dwc.record.DarwinCoreRecord;
import org.gbif.dwc.text.Archive;
import org.gbif.dwc.text.ArchiveFactory;
import org.gbif.dwc.text.UnsupportedArchiveException;
import org.gbif.occurrence.model.RawOccurrenceRecord;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Reads a Darwin Core Archive using the dwca-reader package, and parses it into RawOccurrenceRecords.  Reads
 * the file using a size-configurable, asynchronously filled buffer to reduce memory requirements when reading
 * enormous archives.  Emphatically not thread-safe.
 */
public class BufferedDwcaParser implements Iterator<RawOccurrenceRecord>, Iterable<RawOccurrenceRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(BufferedDwcaParser.class);

  private Integer bufferSize;
  private File dwcaDir;
  private Iterator<DarwinCoreRecord> dwcrIter;
  private ExecutorService threadPool;
  private List<RawOccurrenceRecord> buffer;
  private Iterator<RawOccurrenceRecord> bufferIter;
  private Future<List<RawOccurrenceRecord>> futureBuffer;

  /**
   * Reads the given DwC-A directory in chunks of bufferSize number of rors.
   *
   * @param unzippedDwcaDir the directory containing the unzipped DwC-A
   * @param bufferSize      # of RoRs to parse into the buffer (size of calls to next())
   */
  public BufferedDwcaParser(File unzippedDwcaDir, Integer bufferSize) {
    this.bufferSize = bufferSize;
    this.dwcaDir = unzippedDwcaDir;
    init();
    this.threadPool = Executors.newSingleThreadExecutor();
    fillBuffer();
  }

  private void init() {
    try {
      Archive archive = ArchiveFactory.openArchive(dwcaDir);
      this.dwcrIter = archive.iteratorDwc();
    } catch (UnsupportedArchiveException e) {
      LOG.warn("Could not open dwca", e);
      throw new IllegalArgumentException("Could not open dwca [" + dwcaDir.getAbsolutePath() + "]");
    } catch (IOException e) {
      LOG.warn("Could not open dwca", e);
      throw new IllegalArgumentException("Could not open dwca [" + dwcaDir.getAbsolutePath() + "]");
    }
  }

  @Override
  public RawOccurrenceRecord next() {
    if (!this.hasNext()) return null;

    RawOccurrenceRecord ror = null;

    if (bufferIter == null || !bufferIter.hasNext()) {
      try {
        buffer = futureBuffer.get();
        fillBuffer();
        if (buffer != null) bufferIter = buffer.iterator();
      } catch (InterruptedException e) {
        LOG.warn("Buffer filling was interrupted - returning empty buffer", e);
      } catch (ExecutionException e) {
        LOG.warn("Buffer filling failed - returning empty buffer", e);
      }
    }

    if (bufferIter != null && bufferIter.hasNext()) {
      ror = bufferIter.next();
    }

    return ror;
  }

  @Override
  public boolean hasNext() {
    return (bufferIter != null && bufferIter.hasNext()) || futureBuffer != null;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Remove is not supported.");
  }

  @Override
  public Iterator<RawOccurrenceRecord> iterator() {
    return this;
  }

  private void fillBuffer() {
    if (dwcrIter.hasNext()) {
      futureBuffer = threadPool.submit(new BufferFiller());
    } else {
      futureBuffer = null;
    }
  }

  private class BufferFiller implements Callable<List<RawOccurrenceRecord>> {

    public List<RawOccurrenceRecord> call() {
      List<RawOccurrenceRecord> buffer = new ArrayList<RawOccurrenceRecord>(bufferSize);
      int count = 0;
      while (dwcrIter.hasNext() && count < bufferSize) {
        count++;
        DarwinCoreRecord dwcr = dwcrIter.next();
        RawOccurrenceRecord ror = new RawOccurrenceRecord(dwcr);
        buffer.add(ror);
      }

      return buffer;
    }
  }
}
