package org.gbif.occurrence.download.file.dwca;

import org.gbif.api.model.common.MediaObject;
import org.gbif.api.vocabulary.MediaType;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.file.DownloadFileWork;
import org.gbif.occurrence.download.file.OccurrenceMapReader;
import org.gbif.occurrence.download.file.Result;
import org.gbif.occurrence.download.file.common.DatasetUsagesCollector;
import org.gbif.occurrence.download.file.common.SearchQueryProcessor;
import org.gbif.occurrence.persistence.util.OccurrenceBuilder;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import java.util.Map;

import akka.actor.UntypedActor;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.converters.DateConverter;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanWriter;
import org.supercsv.io.CsvMapWriter;
import org.supercsv.io.ICsvBeanWriter;
import org.supercsv.io.ICsvMapWriter;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.CsvContext;

import static org.gbif.occurrence.common.download.DownloadUtils.DELIMETERS_MATCH_PATTERN;

/**
 * Actor that creates part files of for the DwcA download format.
 */
public class DownloadDwcaActor extends UntypedActor {

  private static final Logger LOG = LoggerFactory.getLogger(DownloadDwcaActor.class);

  static {
    //https://issues.apache.org/jira/browse/BEANUTILS-387
    ConvertUtils.register(new DateConverter(null), Date.class);
  }

  private static final String[] INT_COLUMNS =
    Lists.transform(Lists.newArrayList(TermUtils.interpretedTerms()), Term::simpleName).toArray(new String[0]);
  private static final String[] VERB_COLUMNS =
    Lists.transform(Lists.newArrayList(TermUtils.verbatimTerms()), Term::simpleName).toArray(new String[0]);
  private static final String[] MULTIMEDIA_COLUMNS =
    Lists.transform(Lists.newArrayList(TermUtils.multimediaTerms()), Term::simpleName).toArray(new String[0]);
  private static final CellProcessor[] MEDIA_CELL_PROCESSORS = {new NotNull(), // coreid
    new MediaTypeProcessor(), // type
    new CleanStringProcessor(), // format
    new URIProcessor(), // identifier
    new URIProcessor(), // references
    new CleanStringProcessor(), // title
    new CleanStringProcessor(), // description
    new DateProcessor(), // created
    new CleanStringProcessor(), // creator
    new CleanStringProcessor(), // contributor
    new CleanStringProcessor(), // publisher
    new CleanStringProcessor(), // audience
    new CleanStringProcessor(), // source
    new CleanStringProcessor(), // license
    new CleanStringProcessor() // rightsHolder
  };

  /**
   * Writes the multimedia objects into the file referenced by multimediaCsvWriter.
   */
  private static void writeMediaObjects(ICsvBeanWriter multimediaCsvWriter,
                                        org.apache.hadoop.hbase.client.Result result,
                                        Long occurrenceKey) throws IOException {
    List<MediaObject> multimedia = OccurrenceBuilder.buildMedia(result);
    if (multimedia != null) {
      for (MediaObject mediaObject : multimedia) {
        multimediaCsvWriter.write(new InnerMediaObject(mediaObject, occurrenceKey),
                                  MULTIMEDIA_COLUMNS,
                                  MEDIA_CELL_PROCESSORS);
      }
    }
  }

  /**
   * Executes the job.query and creates a data file that will contains the records from job.from to job.to positions.
   */
  public void doWork(DownloadFileWork work) throws IOException {

    DatasetUsagesCollector datasetUsagesCollector = new DatasetUsagesCollector();

    try (
      ICsvMapWriter intCsvWriter = new CsvMapWriter(new FileWriterWithEncoding(work.getJobDataFileName()
                                                                               + TableSuffixes.INTERPRETED_SUFFIX,
                                                                               Charsets.UTF_8),
                                                    CsvPreference.TAB_PREFERENCE);
      ICsvMapWriter verbCsvWriter = new CsvMapWriter(new FileWriterWithEncoding(work.getJobDataFileName()
                                                                                + TableSuffixes.VERBATIM_SUFFIX,
                                                                                Charsets.UTF_8),
                                                     CsvPreference.TAB_PREFERENCE);
      ICsvBeanWriter multimediaCsvWriter = new CsvBeanWriter(new FileWriterWithEncoding(work.getJobDataFileName()
                                                                                        + TableSuffixes.MULTIMEDIA_SUFFIX,
                                                                                        Charsets.UTF_8),
                                                             CsvPreference.TAB_PREFERENCE)) {
      SearchQueryProcessor.processQuery(work, occurrenceKey -> {
          try {
            // Writes the occurrence record obtained from HBase as Map<String,Object>.
            org.apache.hadoop.hbase.client.Result result = work.getOccurrenceMapReader().get(occurrenceKey);
            Map<String, String> occurrenceRecordMap = OccurrenceMapReader.buildInterpretedOccurrenceMap(result);
            Map<String, String> verbOccurrenceRecordMap = OccurrenceMapReader.buildVerbatimOccurrenceMap(result);
            if (occurrenceRecordMap != null) {
              datasetUsagesCollector.incrementDatasetUsage(occurrenceRecordMap.get(GbifTerm.datasetKey.simpleName()));
              intCsvWriter.write(occurrenceRecordMap, INT_COLUMNS);
              verbCsvWriter.write(verbOccurrenceRecordMap, VERB_COLUMNS);
              writeMediaObjects(multimediaCsvWriter, result, occurrenceKey);
            } else {
              LOG.error(String.format("Occurrence id %s not found!", occurrenceKey));
            }
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        });
    } finally {
      // Unlock the assigned lock.
      work.getLock().unlock();
      LOG.info("Lock released, job detail: {} ", work);
    }
    getSender().tell(new Result(work, datasetUsagesCollector.getDatasetUsages()), getSelf());
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof DownloadFileWork) {
      doWork((DownloadFileWork) message);
    } else {
      unhandled(message);
    }
  }

  /**
   * Inner class used to export data into multimedia.txt files.
   * The structure must match the headers defined in MULTIMEDIA_COLUMNS.
   */
  public static class InnerMediaObject extends MediaObject {

    private Long gbifID;

    /**
     * Default constructor.
     * Required by CVS serialization.
     */
    public InnerMediaObject() {
      // default constructor
    }

    /**
     * Default constructor.
     * Copies the fields of the media object parameter and assigns the coreid.
     */
    public InnerMediaObject(MediaObject mediaObject, Long gbifID) {
      try {
        BeanUtils.copyProperties(this, mediaObject);
        this.gbifID = gbifID;
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).addValue(super.toString()).add("gbifID", gbifID).toString();
    }

    /**
     * Id column for the multimedia.txt file.
     */
    public Long getGbifID() {
      return gbifID;
    }

    public void setGbifID(Long gbifID) {
      this.gbifID = gbifID;
    }

  }

  /**
   * Produces a MediaType instance.
   */
  private static class MediaTypeProcessor implements CellProcessor {

    @Override
    public String execute(Object value, CsvContext context) {
      return value != null ? ((MediaType) value).name() : "";
    }

  }

  /**
   * Produces a String instance clean of delimiter.
   * If the value is null an empty string is returned.
   */
  private static class CleanStringProcessor implements CellProcessor {

    @Override
    public String execute(Object value, CsvContext context) {
      return value != null ? DELIMETERS_MATCH_PATTERN.matcher((String) value).replaceAll(" ") : "";
    }

  }

  /**
   * Produces a URI instance.
   */
  private static class URIProcessor implements CellProcessor {

    @Override
    public String execute(Object value, CsvContext context) {
      return value != null ? ((URI) value).toString() : "";
    }

  }

  /**
   * Produces a date instance.
   */
  private static class DateProcessor implements CellProcessor {

    @Override
    public String execute(Object value, CsvContext context) {
      return value != null ? DownloadUtils.ISO_8601_FORMAT.format(((Date) value).toInstant().atZone(ZoneOffset.UTC)) : "";
    }
  }
}
