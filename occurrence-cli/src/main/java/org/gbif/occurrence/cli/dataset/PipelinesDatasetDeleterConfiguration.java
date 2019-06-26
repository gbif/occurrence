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
public class PipelinesDatasetDeleterConfiguration {

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

  @Override
  public String toString() {
    return new StringJoiner(
            ", ", PipelinesDatasetDeleterConfiguration.class.getSimpleName() + "[", "]")
        .add("messaging=" + messaging)
        .add("ganglia=" + ganglia)
        .add("poolSize=" + poolSize)
        .add("queueName='" + queueName + "'")
        .add("esHosts=" + Arrays.toString(esHosts))
        .add("esIndex=" + Arrays.toString(esIndex))
        .toString();
  }
}
