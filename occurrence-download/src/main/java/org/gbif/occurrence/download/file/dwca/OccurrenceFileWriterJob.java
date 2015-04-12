package org.gbif.occurrence.download.file.dwca;

import org.gbif.api.model.common.MediaObject;
import org.gbif.api.vocabulary.MediaType;
import org.gbif.common.search.util.SolrConstants;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.file.FileJob;
import org.gbif.occurrence.download.file.OccurrenceMapReader;
import org.gbif.occurrence.download.file.Result;
import org.gbif.occurrence.download.file.common.DatasetUsagesCollector;
import org.gbif.occurrence.download.file.common.SolrQueryProcessor;
import org.gbif.occurrence.persistence.util.OccurrenceBuilder;
import org.gbif.wrangler.lock.Lock;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.converters.DateConverter;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
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

/**
 * Job that creates a part of CSV file. The file is generated according to the fileJob field.
 */
public class OccurrenceFileWriterJob implements Callable<Result> {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceFileWriterJob.class);

  static {
    //https://issues.apache.org/jira/browse/BEANUTILS-387
    ConvertUtils.register(new DateConverter(null), Date.class);
  }

  private static Function<Term, String> SIMPLENAME_FUNC = new Function<Term, String>() {

    @Nullable
    @Override
    public String apply(@Nullable Term input) {
      return input.simpleName();
    }
  };

  private static final String[] INT_COLUMNS = Lists.transform(Lists.newArrayList(TermUtils.interpretedTerms()),
    SIMPLENAME_FUNC).toArray(new String[0]);
  private static final String[] VERB_COLUMNS = Lists.transform(Lists.newArrayList(TermUtils.verbatimTerms()),
    SIMPLENAME_FUNC).toArray(new String[0]);
  private static final String[] MULTIMEDIA_COLUMNS = Lists.transform(Lists.newArrayList(TermUtils.multimediaTerms()),
    SIMPLENAME_FUNC).toArray(new String[0]);

  /**
   * Inner class used to export data into multimedia.txt files.
   * The structure must match the headers defined in MULTIMEDIA_COLUMNS.
   */
  public static class InnerMediaObject extends MediaObject {

    @Override
    public String toString() {
      return Objects.toStringHelper(this).addValue(super.toString()).add("gbifID", gbifID).toString();
    }


    private Integer gbifID;

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
    public InnerMediaObject(MediaObject mediaObject, Integer gbifID) {
      try {
        BeanUtils.copyProperties(this, mediaObject);
        this.gbifID = gbifID;
      } catch (IllegalAccessException e) {
        Throwables.propagate(e);
      } catch (InvocationTargetException e) {
        Throwables.propagate(e);
      }
    }

    /**
     * Id column for the multimedia.txt file.
     */
    public Integer getGbifID() {
      return gbifID;
    }


    public void setGbifID(Integer gbifID) {
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
      return value != null ? ((String) value).replaceAll(DownloadUtils.DELIMETERS_MATCH, " ") : "";
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
      return value != null ? (new SimpleDateFormat(DownloadUtils.ISO_8601_FORMAT).format((Date) value)).toString() : "";
    }

  }

  private static final CellProcessor[] MEDIA_CELL_PROCESSORS = new CellProcessor[] {
    new NotNull(), // coreid
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

  private final FileJob fileJob;

  private final Lock lock;

  private final SolrServer solrServer;

  private final OccurrenceMapReader occurrenceMapReader;

  /**
   * Default constructor.
   */
  public OccurrenceFileWriterJob(FileJob fileJob, Lock lock, SolrServer solrServer,
    OccurrenceMapReader occurrenceMapReader) {
    this.fileJob = fileJob;
    this.lock = lock;
    this.solrServer = solrServer;
    this.occurrenceMapReader = occurrenceMapReader;
  }

  /**
   * Executes the job.query and creates a data file that will contains the records from job.from to job.to positions.
   */
  @Override
  public Result call() throws IOException {

    final DatasetUsagesCollector datasetUsagesCollector = new DatasetUsagesCollector();

    try (
      ICsvMapWriter intCsvWriter =
        new CsvMapWriter(new FileWriterWithEncoding(fileJob.getBaseTableName() + Constants.INTERPRETED_SUFFIX, Charsets.UTF_8),
          CsvPreference.TAB_PREFERENCE);
      ICsvMapWriter verbCsvWriter =
        new CsvMapWriter(new FileWriterWithEncoding(fileJob.getBaseTableName() + Constants.VERBATIM_SUFFIX, Charsets.UTF_8),
          CsvPreference.TAB_PREFERENCE);
      ICsvBeanWriter multimediaCsvWriter =
        new CsvBeanWriter(new FileWriterWithEncoding(fileJob.getBaseTableName() + Constants.MULTIMEDIA_SUFFIX, Charsets.UTF_8),
          CsvPreference.TAB_PREFERENCE))
      {
        SolrQueryProcessor.processQuery(fileJob,solrServer, new Predicate<Integer>() {
          @Override
          public boolean apply(@Nullable Integer occurrenceKey) {
            try {
              // Writes the occurrence record obtained from HBase as Map<String,Object>.
              org.apache.hadoop.hbase.client.Result result = occurrenceMapReader.get(occurrenceKey);
              Map<String, String> occurrenceRecordMap = OccurrenceMapReader.buildInterpretedOccurrenceMap(result);
              Map<String, String> verbOccurrenceRecordMap = OccurrenceMapReader.buildVerbatimOccurrenceMap(result);
              if (occurrenceRecordMap != null) {
                datasetUsagesCollector.collectUsage(occurrenceRecordMap);
                intCsvWriter.write(occurrenceRecordMap, INT_COLUMNS);
                verbCsvWriter.write(verbOccurrenceRecordMap, VERB_COLUMNS);
                writeMediaObjects(multimediaCsvWriter, result, occurrenceKey);
                return true;
              } else {
                LOG.error(String.format("Occurrence id %s not found!", occurrenceKey));
              }
            } catch (Exception e) {
              Throwables.propagate(e);
            }
            return false;
          }
        });
    } finally {
      // Unlock the assigned lock.
      lock.unlock();
      LOG.info("Lock released, job detail: {} ", fileJob.toString());
    }
    return new Result(fileJob, datasetUsagesCollector.getDatasetUsages());
  }

  /**
   * Writes the multimedia objects into the file referenced by multimediaCsvWriter.
   */
  private void writeMediaObjects(ICsvBeanWriter multimediaCsvWriter, org.apache.hadoop.hbase.client.Result result,
    Integer occurrenceKey)
    throws IOException {
    List<MediaObject> multimedias = OccurrenceBuilder.buildMedia(result);
    if (multimedias != null) {
      for (MediaObject mediaObject : multimedias) {
        multimediaCsvWriter.write(new InnerMediaObject(mediaObject, occurrenceKey), MULTIMEDIA_COLUMNS,
          MEDIA_CELL_PROCESSORS);
      }
    }
  }

  /**
   * Creates a SolrQuery that contains the query parameter as the filter query value.
   */
  private SolrQuery createSolrQuery(String query) {
    SolrQuery solrQuery = new SolrQuery();
    solrQuery.setQuery(SolrConstants.DEFAULT_QUERY);
    if (!Strings.isNullOrEmpty(query)) {
      solrQuery.addFilterQuery(query);
    }
    return solrQuery;
  }

}
