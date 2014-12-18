package org.gbif.occurrence.download.service.freemarker;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
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
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    int dayOfMonth = cal.get(Calendar.DAY_OF_MONTH);

    DateFormat dateFormat = new SimpleDateFormat("d'" + getDayOfMonthSuffix(dayOfMonth) + "' MMMM yyyy");
    return dateFormat.format(date.getTime());
  }

  private static String getDayOfMonthSuffix(final int n) {
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