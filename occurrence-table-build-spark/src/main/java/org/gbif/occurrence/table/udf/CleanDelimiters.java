package org.gbif.occurrence.table.udf;


import java.util.function.Function;
import java.util.regex.Pattern;

public class CleanDelimiters implements Function<String,String> {

  public static final String DELIMETERS_MATCH =
    "\\t|\\n|\\r|(?:(?>\\u000D\\u000A)|[\\u000A\\u000B\\u000C\\u000D\\u0085\\u2028\\u2029\\u0000])";

  public static final Pattern DELIMETERS_MATCH_PATTERN = Pattern.compile(DELIMETERS_MATCH);
  @Override
  public String apply(String value) {
    return value != null? DELIMETERS_MATCH_PATTERN.matcher(value).replaceAll(" ").trim() : null;
  }
}
