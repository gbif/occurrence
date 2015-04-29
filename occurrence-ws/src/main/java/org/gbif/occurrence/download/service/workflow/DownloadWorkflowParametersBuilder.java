package org.gbif.occurrence.download.service.workflow;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.HiveColumnsUtils;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.service.Constants;
import org.gbif.occurrence.download.service.HiveQueryVisitor;
import org.gbif.occurrence.download.service.QueryBuildingException;
import org.gbif.occurrence.download.service.SolrQueryVisitor;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringEscapeUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.occurrence.common.download.DownloadUtils.downloadLink;

/**
 * Builds the configuration parameters for the download workflows.
 */
public class DownloadWorkflowParametersBuilder {

  // ObjectMappers are thread safe if not reconfigured in code
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  private static final Logger LOG = LoggerFactory.getLogger(DownloadWorkflowParametersBuilder.class);
  private static final Joiner EMAIL_JOINER = Joiner.on(';').skipNulls();
  private static final CharMatcher REPL_INVALID_HIVE_CHARS = CharMatcher.inRange('a', 'z')
    .or(CharMatcher.inRange('A', 'Z'))
    .or(CharMatcher.DIGIT)
    .or(CharMatcher.is('_'))
    .negate();

  private static final String HIVE_SELECT_INTERPRETED;
  private static final String HIVE_SELECT_VERBATIM;
  private static final String JOIN_ARRAY_FMT = "if(%1$s IS NULL,'',join_array(%1$s,';')) AS %1$s";

  static {
    List<String> columns = Lists.newArrayList();
    for (Term term : TermUtils.interpretedTerms()) {
      final String iCol = HiveColumnsUtils.getHiveColumn(term);
      if (TermUtils.isInterpretedDate(term)) {
        columns.add("toISO8601(" + iCol + ") AS " + iCol);
      } else if (TermUtils.isInterpretedNumerical(term) || TermUtils.isInterpretedDouble(term)) {
        columns.add("cleanNull(" + iCol + ") AS " + iCol);
      } else if (TermUtils.isInterpretedBoolean(term)) {
        columns.add(iCol);
      } else if (term == GbifTerm.issue) {
        // OccurrenceIssues are exposed as an String separate by ;
        columns.add(String.format(JOIN_ARRAY_FMT, iCol));
      } else if (term == GbifTerm.mediaType) {
        // MediaTypes are exposed as an String separate by ;
        columns.add(String.format(JOIN_ARRAY_FMT, iCol));
      } else if (!TermUtils.isComplexType(term)) {
        // complex type fields are not added to the select statement
        columns.add(iCol);
      }
    }
    HIVE_SELECT_INTERPRETED = Joiner.on(',').join(columns);


    columns = Lists.newArrayList();
    // manually add the GBIF occ key as first column
    columns.add(HiveColumnsUtils.getHiveColumn(GbifTerm.gbifID));
    for (Term term : TermUtils.verbatimTerms()) {
      if (GbifTerm.gbifID != term) {
        String colName = HiveColumnsUtils.getHiveColumn(term);
        columns.add("cleanDelimiters(v_" + colName + ") AS v_" + colName);
      }
    }
    HIVE_SELECT_VERBATIM = Joiner.on(',').join(columns);
  }

  private final Map<String, String> defaultProperties;
  private final Map<String, String> simpleCSVDefaultProperties;
  private final String wsUrl;

  public DownloadWorkflowParametersBuilder(Map<String, String> defaultProperties, Map<String, String> simpleCSVDefaultProperties, String wsUrl) {
    this.defaultProperties = defaultProperties;
    this.simpleCSVDefaultProperties = simpleCSVDefaultProperties;
    this.wsUrl = wsUrl;
  }

  /**
   * Use the request.format to build the workflow parameters.
   */
  public Properties buildWorkflowParameters(DownloadRequest request) {
    if (DownloadFormat.DWCA == request.getFormat()) {
      return buildDWCADownloadParameters(request);
    } else if (DownloadFormat.SIMPLE_CSV == request.getFormat()){
      return buildSimpleCSVParameters(request);
    }
    throw new IllegalStateException("Unsupported download format");
  }

  /**
   * Builds the parameters required for the SimpleCSV download workflow.
   */
  private Properties buildSimpleCSVParameters(DownloadRequest request) {

    Properties properties = new Properties();
    properties.putAll(simpleCSVDefaultProperties);
    properties.put(DownloadWorkflowParameters.SimpleCsv.GBIF_FILTER, getJsonStringPredicate(request.getPredicate()));
    properties.setProperty(Constants.USER_PROPERTY, request.getCreator());
    if (request.getNotificationAddresses() != null && !request.getNotificationAddresses().isEmpty()) {
      properties.setProperty(Constants.NOTIFICATION_PROPERTY, EMAIL_JOINER.join(request.getNotificationAddresses()));
    }

    LOG.debug("job properties: {}", properties);

    return properties;
  }

  /**
   * Serializes a predicate filter into a json string.
   */
  private static String getJsonStringPredicate(final Predicate predicate){
    StringWriter writer = new StringWriter();
    try {
      JSON_MAPPER.writeValue(writer, predicate);
      writer.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new ServiceUnavailableException("Failed to serialize download filter " + predicate, e);
    }

  }

  /**
   * Builds the parameters required for the DWCA download workflow.
   */
  private Properties buildDWCADownloadParameters(DownloadRequest request){

    HiveQueryVisitor hiveVisitor = new HiveQueryVisitor();
    SolrQueryVisitor solrVisitor = new SolrQueryVisitor();
    String hiveQuery;
    String solrQuery;
    try {
      hiveQuery = StringEscapeUtils.escapeXml(hiveVisitor.getHiveQuery(request.getPredicate()));
      solrQuery = solrVisitor.getQuery(request.getPredicate());
    } catch (QueryBuildingException e) {
      throw new ServiceUnavailableException("Error building the hive query, attempting to continue", e);
    }
    LOG.debug("Attempting to download with hive query [{}]", hiveQuery);

    final String uniqueId =
      REPL_INVALID_HIVE_CHARS.removeFrom(request.getCreator() + '_' + UUID.randomUUID().toString());
    final String tmpTable = "download_tmp_" + uniqueId;
    final String citationTable = "download_tmp_citation_" + uniqueId;

    Properties jobProps = new Properties();
    jobProps.putAll(defaultProperties);
    jobProps.setProperty("select_interpreted", HIVE_SELECT_INTERPRETED);
    jobProps.setProperty("select_verbatim", HIVE_SELECT_VERBATIM);
    jobProps.setProperty("query", hiveQuery);
    jobProps.setProperty("solr_query", solrQuery);
    jobProps.setProperty("query_result_table", tmpTable);
    jobProps.setProperty("citation_table", citationTable);
    // we dont have a downloadId yet, submit a placeholder
    jobProps.setProperty("download_link", downloadLink(wsUrl, DownloadUtils.DOWNLOAD_ID_PLACEHOLDER));
    jobProps.setProperty(Constants.USER_PROPERTY, request.getCreator());
    if (request.getNotificationAddresses() != null && !request.getNotificationAddresses().isEmpty()) {
      jobProps.setProperty(Constants.NOTIFICATION_PROPERTY, EMAIL_JOINER.join(request.getNotificationAddresses()));
    }
    // serialize the predicate filter into json
    jobProps.setProperty(Constants.FILTER_PROPERTY, getJsonStringPredicate(request.getPredicate()));

    LOG.debug("job properties: {}", jobProps);

    return jobProps;
  }
}
