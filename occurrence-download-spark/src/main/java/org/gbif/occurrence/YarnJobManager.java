package org.gbif.occurrence;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

import org.gbif.api.model.occurrence.DownloadRequest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;

import javax.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class YarnJobManager implements JobManager {

  private static final EnumSet<YarnApplicationState> YARN_APPLICATION_STATES =
    EnumSet.of(
      YarnApplicationState.NEW,
      YarnApplicationState.NEW_SAVING,
      YarnApplicationState.SUBMITTED,
      YarnApplicationState.ACCEPTED,
      YarnApplicationState.RUNNING);

  private static final Set<String> DOWNLOADS_TYPES = Collections.singleton("SPARK");

  private final YarnClient yarnClient;

  public YarnJobManager(String pathToYarnSite, String pathToCoreSite, String pathToHdfsSite) {
    this.yarnClient = createYarnClient(pathToYarnSite, pathToCoreSite, pathToHdfsSite);
  }

  @Override
  public Optional<String> createJob(@NotNull String jobId, @NotNull DownloadRequest downloadRequest) {
    try {
      SparkConf sparkConf = new SparkConf().setMaster("yarn").setAppName(jobId).set("deploy-mode", "cluster");

      ClientArguments arguments = new ClientArguments(new String[]{
        "--jar", "hdfs://ha-nn/pipelines/jars/ingest-gbif.jar",
        "--class", "org.gbif.pipelines.ingest.pipelines.VerbatimToIdentifierPipeline",
        "--arg", "--datasetId=13b70480-bd69-11dd-b15f-b8a03c50a862",
        "--arg", "--attempt=432",
        "--arg",
        "--interpretationTypes=LOCATION,TEMPORAL,GRSCICOLL,MULTIMEDIA,BASIC,TAXONOMY,IDENTIFIER_ABSENT,AMPLIFICATION,IMAGE,CLUSTERING,OCCURRENCE,VERBATIM,MULTIMEDIA_TABLE,AUDUBON,LOCATION_FEATURE,MEASUREMENT_OR_FACT,METADATA",
        "--arg", "--runner=SparkRunner",
        "--arg", "--targetPath=hdfs://ha-nn/data/ingest",
        "--arg", "--metaFileName=verbatim-to-identifier.yml",
        "--arg", "--inputPath=hdfs://ha-nn/data/ingest/13b70480-bd69-11dd-b15f-b8a03c50a862/432/verbatim.avro",
        "--arg", "--avroCompressionType=snappy",
        "--arg", "--avroyncInterval=2097152",
        "--arg", "--hdfsSiteConfig=/home/crap/config/hdfs-site.xml",
        "--arg", "--coreSiteConfig=/home/crap/config/core-site.xml",
        "--arg", "--properties=hdfs://ha-nn/pipelines/jars/pipelines.yaml",
        "--arg", "--experiments=use_deprecated_read",
        "--arg", "--tripletValid=true",
        "--arg", "--occurrenceIdValid=true"
      });

      Client client = new Client(arguments, sparkConf);

      YarnClientApplication application = yarnClient.createApplication();

      ContainerLaunchContext containerLaunchContext =
        ContainerLaunchContext.newInstance(null, null, null, null, null, null);

      ApplicationSubmissionContext context =
        client.createApplicationSubmissionContext(application, containerLaunchContext);

      context.setApplicationName(jobId);
      context.setQueue("root.download-admin");

      ApplicationId applicationId = yarnClient.submitApplication(context);

      return Optional.of(applicationId.toString());
    } catch (YarnException | IOException ex) {
      log.error("Error", ex);
      return Optional.empty();
    }
  }

  @Override
  public void cancelJob(@NotNull String jobId) {
    try {
      for (ApplicationReport ar : yarnClient.getApplications(DOWNLOADS_TYPES, YARN_APPLICATION_STATES)) {
        if (ar.getName().equals(jobId)) {
          ApplicationId applicationId = ar.getApplicationId();
          log.info("Killing jobId {}, aplicationId: {}", jobId, applicationId);
          yarnClient.killApplication(applicationId);
        }
      }
    } catch (YarnException | IOException ex) {
      log.error("Exception during the killing the jobId {}", jobId, ex);
    }
  }

  // TODO: move to Spring bean
  @Override
  public void close() {
    try {
      if (yarnClient != null) {
        yarnClient.close();
      }
    } catch (IOException ex) {
      log.error("Exception during the closing YARN client", ex);
    }
  }

  // TODO: move to Spring bean
  private YarnClient createYarnClient(String pathToYarnSite, String pathToCoreSite, String pathToHdfsSite) {
    Configuration cfg = new Configuration();
    cfg.addResource(new Path(pathToYarnSite));
    cfg.addResource(new Path(pathToCoreSite));
    cfg.addResource(new Path(pathToHdfsSite));

    YarnConfiguration configuration = new YarnConfiguration(cfg);
    YarnClient client = YarnClient.createYarnClient();
    client.init(configuration);
    client.start();

    return client;
  }
}
