package org.gbif.occurrence.download.file.simpleavro;

import akka.actor.UntypedActor;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.converters.DateConverter;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.download.file.DownloadFileWork;
import org.gbif.occurrence.download.file.Result;
import org.gbif.occurrence.download.file.common.DatasetUsagesCollector;
import org.gbif.occurrence.download.file.common.SolrQueryProcessor;
import org.gbif.occurrence.download.hive.DownloadTerms;
import org.gbif.utils.file.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Map;

import static org.gbif.occurrence.download.file.OccurrenceMapReader.buildOccurrenceMap;

/**
 * Actor that creates a part of the simple Avro download file.
 */
public class SimpleAvroDownloadActor extends UntypedActor {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleAvroDownloadActor.class);

  static {
    //https://issues.apache.org/jira/browse/BEANUTILS-387
    ConvertUtils.register(new DateConverter(null), Date.class);
  }

  private static final String[] COLUMNS =
    Collections2.transform(DownloadTerms.SIMPLE_DOWNLOAD_TERMS, new Function<Term, String>() {
                             @Nullable
                             @Override
                             public String apply(@Nullable Term input) {
                               return input.simpleName();
                             }
                           }).toArray(new String[DownloadTerms.SIMPLE_DOWNLOAD_TERMS.size()]);

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof DownloadFileWork) {
      doWork((DownloadFileWork) message);
    } else {
      unhandled(message);
    }
  }

  /**
   * Executes the job.query and creates a data file that will contains the records from job.from to job.to positions.
   */
  private void doWork(final DownloadFileWork work) throws IOException {

    // TODO: Something in this method is wrong, or else in the Aggregator.

    final DatasetUsagesCollector datasetUsagesCollector = new DatasetUsagesCollector();

    final Schema simpleDownloadSchema = Schema.parse(FileUtils.getClasspathFile("/download-workflow/simple-avro/occurrence.avsc"));

    try {
      DataFileWriter<Map> avroWriter = new DataFileWriter(new ReflectDatumWriter<>(simpleDownloadSchema));
      avroWriter.setCodec(CodecFactory.deflateCodec(-1));
      avroWriter.setFlushOnEveryBlock(false);
      avroWriter.create(simpleDownloadSchema, new File(work.getJobDataFileName()));

      SolrQueryProcessor.processQuery(work, occurrenceKey -> {
        try {
          org.apache.hadoop.hbase.client.Result result = work.getOccurrenceMapReader().get(occurrenceKey);
          Map<String, String> occurrenceRecordMap = buildOccurrenceMap(result, DownloadTerms.SIMPLE_DOWNLOAD_TERMS);
          if (occurrenceRecordMap != null) {
            //collect usages
            datasetUsagesCollector.collectDatasetUsage(occurrenceRecordMap.get(GbifTerm.datasetKey.simpleName()),
              occurrenceRecordMap.get(DcTerm.license.simpleName()));
            //write results
            avroWriter.append(occurrenceRecordMap);
            return true;
          } else {
            LOG.error(String.format("Occurrence id %s not found!", occurrenceKey));
          }
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
        return false;
      });

    } finally {
      // Release the lock
      work.getLock().unlock();
      LOG.info("Lock released, job detail: {} ", work.toString());
    }
    getSender().tell(new Result(work, datasetUsagesCollector.getDatasetUsages(),
            datasetUsagesCollector.getDatasetLicenses()), getSelf());
  }

}
