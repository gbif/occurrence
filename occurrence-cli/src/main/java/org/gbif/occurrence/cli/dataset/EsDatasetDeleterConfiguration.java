package org.gbif.occurrence.cli.dataset;

import org.gbif.common.messaging.config.MessagingConfiguration;
import org.gbif.occurrence.cli.common.GangliaConfiguration;

import java.util.Arrays;
import java.util.StringJoiner;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

/** Configuration required to update ES with changes from deleted datasets. */
public class EsDatasetDeleterConfiguration {

  @ParametersDelegate @NotNull @Valid
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @ParametersDelegate @Valid @NotNull
  public GangliaConfiguration ganglia = new GangliaConfiguration();

  @Parameter(names = "--pool-size")
  @Min(1)
  public int poolSize = 1;

  @Parameter(names = "--queue-name")
  @NotNull
  public String queueName;

  @Parameter(names = "--es-hosts")
  @NotNull
  public String[] esHosts;

  @Parameter(names = "--es-index")
  @NotNull
  public String[] esIndex;

  @Parameter(names = "--es-connect-timeout")
  public int esConnectTimeout = 7500;

  @Parameter(names = "--es-socket-timeout")
  public int esSocketTimeout = 125000;

  @Parameter(names = "--es-sniff-interval")
  public int esSniffInterval = 300000;

  @Parameter(names = "--es-sniff-after-failure-delay")
  public int esSniffAfterFailureDelay = 30000;

  @Parameter(names = "--hdfs-site-config")
  @NotNull
  public String hdfsSiteConfig;

  @Parameter(names = "--core-site-config")
  @NotNull
  public String coreSiteConfig;

  @Parameter(names = "--hdfs-view-dir-path")
  @NotNull
  public String hdfsViewDirPath;

  @Parameter(names = "--ingest-dir-path")
  @NotNull
  public String ingestDirPath;

  @Override
  public String toString() {
    return new StringJoiner(
      ", ", EsDatasetDeleterConfiguration.class.getSimpleName() + "[", "]")
        .add("messaging=" + messaging)
        .add("ganglia=" + ganglia)
        .add("poolSize=" + poolSize)
        .add("queueName='" + queueName + "'")
        .add("esHosts=" + Arrays.toString(esHosts))
        .add("esIndex=" + Arrays.toString(esIndex))
        .add("esConnectTimeout=" + esConnectTimeout)
        .add("esSocketTimeout=" + esSocketTimeout)
        .add("esSniffInterval=" + esSniffInterval)
        .add("esSniffAfterFailureDelay=" + esSniffAfterFailureDelay)
        .add("coreSiteConfig=" + coreSiteConfig)
        .add("hdfsSiteConfig=" + hdfsSiteConfig)
        .toString();
  }
}
