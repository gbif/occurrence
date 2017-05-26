package org.gbif.occurrence.cli.common;

import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic configuration for a service that can be scheduled.
 */
public class SchedulingConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(HiveJdbcConfiguration.class);

  @Parameter(names = "--start-time")
  public String startTime;

  @NotNull
  @Parameter(names = "--frequency-in-hour")
  public double frequencyInHour = 24;


  /**
   * Parse and return {@link #startTime} as {@link LocalTime}.
   * If {@link #startTime} can not be parsed or not provided, return {@code LocalTime.now()}
   *
   * @return {@link #startTime} or {@code LocalTime.now()} if it can't be parsed
   */
  public LocalTime parseStartTime() {
    LocalTime t = LocalTime.now();

    if(StringUtils.isBlank(startTime)){
      return t;
    }

    try {
      t = LocalTime.parse(startTime);
    } catch (DateTimeParseException dtpEx) {
      LOG.warn("Can not parse LocalTime [" + startTime + "]. Using startTime LocalTime.now()");
    }
    return t;
  }

}
