package org.gbif.occurrence.beam.solr;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.occurrence.persistence.util.OccurrenceBuilder;
import org.gbif.occurrence.search.writer.SolrOccurrenceWriter;

import java.io.IOException;
import java.util.Objects;

import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.solr.SolrIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;

/**
 * Reads documents from Solr filtered by a query, loads the records from HBase and updates Solr using latest HBase data.
 */
public class SolrUpdateByQuery {


  public static void main(String[] args) {

    Counter docsIndexed =  Metrics.counter(BulkUpdateOptions.class,"docsIndexed");
    Counter docsFailed =  Metrics.counter(BulkUpdateOptions.class,"docsFailed");

    PipelineOptionsFactory.register(BulkUpdateOptions.class);
    BulkUpdateOptions options = PipelineOptionsFactory.fromArgs(args).as(BulkUpdateOptions.class);
    options.setRunner(SparkRunner.class);
    Pipeline p = Pipeline.create(options);

    PCollection<SolrDocument> docsIn = p.apply("Read from Solr",
      SolrIO.read().withConnectionConfiguration(SolrIO.ConnectionConfiguration.create(options.getSolrZk()))
        .from(options.getSolrCollection())
        .withQuery(options.getSolrQuery()));

    String hbaseZk = options.getHbaseZk();

    String tableName = options.getTable();

    PCollection<SolrInputDocument> docsOut = docsIn.apply("Convert to Occurrence", ParDo.of(

      new DoFn<SolrDocument, SolrInputDocument>() {

        private Connection connection;
        private Table table;

        @Setup
        public void setup() throws IOException {
          Configuration hbaseConfig = HBaseConfiguration.create();
          hbaseConfig.set("hbase.zookeeper.quorum", hbaseZk);
          connection = ConnectionFactory.createConnection(hbaseConfig);
          table = connection.getTable(TableName.valueOf(tableName));
        }

        @Teardown
        public void tearDown() throws IOException {
          if(Objects.nonNull(table)) {
            table.close();
            connection.close();
          }
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
          try {
              SolrDocument solrDocument = c.element();
              Get get = new Get(Bytes.toBytes((Integer)solrDocument.get("key")));
              Result row = table.get(get);
              Occurrence occurrence = OccurrenceBuilder.buildOccurrence(row);
              if (Objects.nonNull(occurrence)) {
                SolrInputDocument document = SolrOccurrenceWriter.buildOccSolrDocument(occurrence);
                c.output(document);
                docsIndexed.inc();
                c.output(document);
              }

          } catch (Exception e) {
            // Expected for bad data
            docsFailed.inc();
          }
        }
      }));

    docsOut.apply(SolrIO.write().withConnectionConfiguration(SolrIO.ConnectionConfiguration.create(options.getSolrZk())).to(options.getSolrCollection()));
    PipelineResult result = p.run();
    result.waitUntilFinish();
  }
}
