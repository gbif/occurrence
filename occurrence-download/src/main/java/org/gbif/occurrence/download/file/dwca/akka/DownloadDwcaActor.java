/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.download.file.dwca.akka;

import org.gbif.api.model.common.MediaObject;
import org.gbif.api.model.event.Event;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.MediaType;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.file.DownloadFileWork;
import org.gbif.occurrence.download.file.Result;
import org.gbif.occurrence.download.file.TableSuffixes;
import org.gbif.occurrence.download.file.common.DatasetUsagesCollector;
import org.gbif.occurrence.download.file.common.SearchQueryProcessor;
import org.gbif.occurrence.download.hive.ExtensionTable;
import org.gbif.occurrence.download.hive.HiveColumns;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Date;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.converters.DateConverter;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.encoder.DefaultCsvEncoder;
import org.supercsv.io.CsvBeanWriter;
import org.supercsv.io.CsvMapWriter;
import org.supercsv.io.ICsvBeanWriter;
import org.supercsv.io.ICsvMapWriter;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.CsvContext;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import akka.actor.AbstractActor;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.occurrence.common.download.DownloadUtils.DELIMETERS_MATCH_PATTERN;

/**
 * Actor that creates part files of for the DwcA download format.
 */
@Slf4j
@AllArgsConstructor
public class DownloadDwcaActor<T extends VerbatimOccurrence> extends AbstractActor {

  private final SearchQueryProcessor<T> searchQueryProcessor;

  private final Function<T,Map<String,String>> verbatimMapper;

  private final Function<T,Map<String,String>> interpretedMapper;

  private final Map<Extension,ICsvMapWriter> extensionICsvMapWriterMap = new EnumMap<>(Extension.class);

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
  private static final CellProcessor[] MEDIA_CELL_PROCESSORS = {
    new NotNull(), // coreid
    new MediaTypeProcessor(), // type
    new CleanStringProcessor(), // format
    new URIProcessor(), // identifier
    new URIProcessor(), // references
    new CleanStringProcessor(), // title
    new CleanStringProcessor(), // description
    new CleanStringProcessor(), // source
    new CleanStringProcessor(), // audience
    new DateProcessor(), // created
    new CleanStringProcessor(), // creator
    new CleanStringProcessor(), // contributor
    new CleanStringProcessor(), // publisher
    new CleanStringProcessor(), // license
    new CleanStringProcessor() // rightsHolder
  };


  @SneakyThrows
  private ICsvMapWriter getExtensionWriter(Extension extension, DownloadFileWork work) {
    return extensionICsvMapWriterMap.computeIfAbsent(extension, ext -> {
      try {
        String outPath = work.getJobDataFileName() + '_' + new ExtensionTable(ext).getHiveTableName();
        log.info("Writing to extension file {}", outPath);
        CsvPreference preference =
          new CsvPreference.Builder(CsvPreference.TAB_PREFERENCE)
            .useEncoder(new DefaultCsvEncoder())
            .build();
      return new CsvMapWriter(new FileWriterWithEncoding(outPath, StandardCharsets.UTF_8), preference);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
    }
    });
  }

  /**
   * Gets the record key/id depending on the instances type.
   */
  private String getRecordKey(T record) {
    if (record instanceof Event) {
      return  ((Event)record).getId();
    } else {
      return record.getKey().toString();
    }
  }

  /**
   * Writes the multimedia objects into the file referenced by multimediaCsvWriter.
   */
  private void writeMediaObjects(ICsvBeanWriter multimediaCsvWriter, T record) throws IOException {
    List<MediaObject> multimedia = getMedia(record);
    for (MediaObject mediaObject : multimedia) {
      multimediaCsvWriter.write(
          new InnerMediaObject(mediaObject, getRecordKey(record)),
          MULTIMEDIA_COLUMNS,
          MEDIA_CELL_PROCESSORS);
    }
  }

  /**
   * Extracts media objects from known types.
   */
  private List<MediaObject> getMedia(T record) {
    if (record instanceof Event) {
      return ((Event)record).getMedia();
    }
    if (record instanceof Occurrence) {
      return ((Occurrence)record).getMedia();
    }
    return Collections.emptyList();
  }

  /**
   * Writes the extensions objects into the file referenced by extensionCsvWriter.
   */
  private void writeExtensions(DownloadFileWork work, T record) throws IOException {
    if (!work.getExtensions().isEmpty()) {
      Map<String,List<Map<Term, String>>> exportExtensions = record.getExtensions()
        .entrySet().stream()
        .filter(e ->  work.getExtensions().contains(Extension.fromRowType(e.getKey())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      for (Map.Entry<String,List<Map<Term, String>>> dwcExtension : exportExtensions.entrySet()) {
        CsvExtension csvExtension = CsvExtension.getCsvExtension(dwcExtension.getKey());
        for (Map<Term, String> row : dwcExtension.getValue()) {
          getExtensionWriter(Extension.fromRowType(dwcExtension.getKey()), work)
            .write(toExtensionRecord(row, record), csvExtension.getColumns(), csvExtension.getProcessors());
        }
      }
    }
  }

  private Map<String,String> toExtensionRecord(Map<Term, String> row, T record) {
    Map<String,String> extensionData = new LinkedHashMap<>();
    extensionData.put("gbifid", getRecordKey(record));
    extensionData.put("datasetkey", record.getDatasetKey().toString());
    extensionData.putAll(row.entrySet().stream()
                           .collect(Collectors.toMap(e -> HiveColumns.columnFor(e.getKey()), Map.Entry::getValue)));
    return extensionData;
  }

  @SneakyThrows
  private void closeExtensionWriters() {
    for (ICsvMapWriter writer : extensionICsvMapWriterMap.values()) {
      writer.flush();
      writer.close();
    }
  }

  /**
   * Executes the job.query and creates a data file that will contain the records from job.from to job.to positions.
   */
  public void doWork(DownloadFileWork work) throws IOException {

    DatasetUsagesCollector datasetUsagesCollector = new DatasetUsagesCollector();

    CsvPreference preference =
        new CsvPreference.Builder(CsvPreference.TAB_PREFERENCE)
            .useEncoder(new DefaultCsvEncoder())
            .build();

    try (ICsvMapWriter intCsvWriter =
            new CsvMapWriter(
                new FileWriterWithEncoding(
                    work.getJobDataFileName() + TableSuffixes.INTERPRETED_SUFFIX,
                    StandardCharsets.UTF_8),
                preference);
        ICsvMapWriter verbCsvWriter =
            new CsvMapWriter(
                new FileWriterWithEncoding(
                    work.getJobDataFileName() + TableSuffixes.VERBATIM_SUFFIX,
                    StandardCharsets.UTF_8),
                preference);
        ICsvBeanWriter multimediaCsvWriter =
            new CsvBeanWriter(
                new FileWriterWithEncoding(
                    work.getJobDataFileName() + TableSuffixes.MULTIMEDIA_SUFFIX,
                    StandardCharsets.UTF_8),
                preference)) {
      searchQueryProcessor.processQuery(work, record -> {
          try {
            // Writes the occurrence record obtained from Elasticsearch as Map<String,Object>.

            if (record != null) {
              datasetUsagesCollector.incrementDatasetUsage(record.getDatasetKey().toString());
              intCsvWriter.write(interpretedMapper.apply(record), INT_COLUMNS);
              verbCsvWriter.write(verbatimMapper.apply(record), VERB_COLUMNS);
              writeMediaObjects(multimediaCsvWriter, record);
              writeExtensions(work, record);
            }
          } catch (Exception e) {
            getSender().tell(e, getSelf()); // inform our master
            throw Throwables.propagate(e);
          }
        });
      getSender().tell(new Result(work, datasetUsagesCollector.getDatasetUsages()), getSelf());
    } finally {
      closeExtensionWriters();
      // Unlock the assigned lock.
      work.getLock().unlock();
      log.info("Lock released, job detail: {} ", work);
    }
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(DownloadFileWork.class, this::doWork)
      .build();
  }

  /**
   * Inner class used to export data into multimedia.txt files.
   * The structure must match the headers defined in MULTIMEDIA_COLUMNS.
   */
  @Data
  public static class InnerMediaObject extends MediaObject {

    private String gbifID;

    /**
     * Default constructor.
     * Copies the fields of the media object parameter and assigns the coreid.
     */
    public InnerMediaObject(MediaObject mediaObject, String gbifID) {
      try {
        BeanUtils.copyProperties(this, mediaObject);
        this.gbifID = gbifID;
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw Throwables.propagate(e);
      }
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
  protected static class CleanStringProcessor implements CellProcessor {

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
      return value != null ? DownloadUtils.ISO_8601_ZONED.format(((Date) value).toInstant().atZone(ZoneOffset.UTC)) : "";
    }
  }
}
