package org.gbif.occurrence.beam.solr;

import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.hbase.HBaseIO;
import org.apache.beam.sdk.io.solr.SolrIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.solr.common.SolrInputDocument;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.occurrence.beam.solr.io.PatchedSolrIO;
import org.gbif.occurrence.persistence.util.OccurrenceBuilder;
import org.gbif.occurrence.search.writer.SolrOccurrenceWriter;

/**
 * Executes a pipeline that reads HBase and loads SOLR.
 */
public class BulkloadSolr {
  // TODO - paramaterise this
  //private static final String HBASE_ZK = "c4zk1.gbif-uat.org,c4zk2.gbif-uat.org,c4zk3.gbif-uat.org";
  //private static final String HBASE_TABLE = "uat_occurrence";
  //private static final String SOLR_HOST = "c4zk1.gbif-uat.org,c4zk2.gbif-uat.org,c4zk3.gbif-uat.org/solrp";
  //private static final String SOLR_COLLECTION = "tim-occurrence";


  private static final String HBASE_ZK = "c5zk1.gbif.org,c5zk2.gbif.org,c5zk3.gbif.org";
  private static final String HBASE_TABLE = "prod_e_occurrence";
  private static final String SOLR_HOST = "c5zk1.gbif.org,c5zk2.gbif.org,c5zk3.gbif.org/solr5";
  private static final String SOLR_COLLECTION = "tim-occurrence";

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(SparkRunner.class);
    Pipeline p = Pipeline.create(options);


    Configuration hbaseConfig = HBaseConfiguration.create();
    hbaseConfig.set("hbase.zookeeper.quorum", HBASE_ZK);

    Scan scan = new Scan();
    scan.setBatch(1000); // for safety
    scan.addFamily("o".getBytes());

    PCollection<Result> rows =
        p.apply(
            "read",
            HBaseIO.read()
                .withConfiguration(hbaseConfig)
                .withScan(scan)
                .withTableId(HBASE_TABLE));

    PCollection<SolrInputDocument> docs =
        rows.apply(
            "convert to SOLR docs",
            ParDo.of(
                new DoFn<Result, SolrInputDocument>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    Result row = c.element();
                    try {
                      Occurrence occurrence = OccurrenceBuilder.buildOccurrence(row);

                      if (occurrence.getDecimalLatitude() != null && occurrence.getDecimalLongitude() != null
                              && occurrence.getDecimalLatitude() > 40 && occurrence.getDecimalLatitude() < 70 ) {
                        SolrInputDocument document = SolrOccurrenceWriter.buildOccSolrDocument(occurrence);
                        c.output(document);

                      }

                    } catch (NullPointerException e) {
                      // Expected for bad data
                    }
                  }
                }));

      final PatchedSolrIO.ConnectionConfiguration conn = PatchedSolrIO.ConnectionConfiguration.create(SOLR_HOST);

      docs.apply(PatchedSolrIO.write().to(SOLR_COLLECTION).withConnectionConfiguration(conn));

      PipelineResult result = p.run();
      result.waitUntilFinish();
  }
}
