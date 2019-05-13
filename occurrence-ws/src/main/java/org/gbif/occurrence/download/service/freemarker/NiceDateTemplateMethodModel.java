package org.gbif.occurrence.download.service.freemarker;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;

import com.google.common.base.Preconditions;
import freemarker.template.TemplateDateModel;
import freemarker.template.TemplateMethodModelEx;
import freemarker.template.TemplateModelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a java.util.Date rendering for freemarker in the form of "17th December 2014".
 */
public class NiceDateTemplateMethodModel implements TemplateMethodModelEx {
  private static final Logger LOG = LoggerFactory.getLogger(NiceDateTemplateMethodModel.class);

  @Override
  public Object exec(List arguments) throws TemplateModelException {
    Object argRaw = arguments.get(0);
    if (argRaw instanceof TemplateDateModel) {
      Date date = ((TemplateDateModel) argRaw).getAsDate();
      return format(date);
    } else {
      LOG.warn("Single date required as input: {}", argRaw);
      return null;
    }
  }

  public static String format(Date date) {
    LocalDateTime localTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("d'" + getDayOfMonthSuffix(localTime.getDayOfMonth()) + "' MMMM yyyy");
    return formatter.format(localTime);
  }

  private static String getDayOfMonthSuffix(int n) {
    Preconditions.checkArgument(n >= 1 && n <= 31, "illegal day of month: " + n);
    if (n >= 11 && n <= 13) {
      return "th";
    }
    switch (n % 10) {
      case 1:  return "st";
      case 2:  return "nd";
      case 3:  return "rd";
      default: return "th";
    }
  }

}
