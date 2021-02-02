package org.gbif.occurrence.cli.download;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import org.gbif.cli.PropertyName;
import org.gbif.common.messaging.config.MessagingConfiguration;
import org.gbif.occurrence.cli.common.GangliaConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.StringJoiner;

/** Configuration required to upload GBIF downloads to cloud services. */
public class CloudUploaderConfiguration {

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

  @Parameter(names = "--ws-url")
  @NotNull
  public String wsUrl = "http://api.gbif.org/v1/";

  @Parameter(names = "--hdfs-site-config")
  @NotNull
  public String hdfsSiteConfig;

  @Parameter(names = "--core-site-config")
  @NotNull
  public String coreSiteConfig;

  @Override
  public String toString() {
    return new StringJoiner(
      ", ", CloudUploaderConfiguration.class.getSimpleName() + "[", "]")
        .add("messaging=" + messaging)
        .add("ganglia=" + ganglia)
        .add("poolSize=" + poolSize)
        .add("queueName=" + queueName)
        .add("coreSiteConfig=" + coreSiteConfig)
        .add("hdfsSiteConfig=" + hdfsSiteConfig)
        .toString();
  }
}
