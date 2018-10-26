package org.gbif.occurrence.ws.provider.hive;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.compress.utils.Lists;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.TextNode;
import org.gbif.occurrence.ws.provider.hive.Result.Read;
import org.gbif.occurrence.ws.provider.hive.Result.ReadDescribe;
import org.gbif.occurrence.ws.provider.hive.Result.ReadDescribe.DescribeResult;
import org.gbif.occurrence.ws.provider.hive.Result.ReadExplain;
import org.gbif.occurrence.ws.provider.hive.query.validator.DatasetKeyAndLicenseRequiredRule;
import org.gbif.occurrence.ws.provider.hive.query.validator.OnlyOneSelectAllowedRule;
import org.gbif.occurrence.ws.provider.hive.query.validator.OnlyPureSelectQueriesAllowedRule;
import org.gbif.occurrence.ws.provider.hive.query.validator.Query.Issue;
import org.gbif.occurrence.ws.provider.hive.query.validator.QueryContext;
import org.gbif.occurrence.ws.provider.hive.query.validator.Rule;
import org.gbif.occurrence.ws.provider.hive.query.validator.SQLShouldBeExecutableRule;
import org.gbif.occurrence.ws.provider.hive.query.validator.TableNameShouldBeOccurrenceRule;
import com.google.common.base.Throwables;

/**
 * 
 * SQL class to validate and explain the query.
 *
 */
public class HiveSQL {

  /**
   * Explains the query, in case it is not compilable throws RuntimeException.
   */
  public static class Execute implements BiFunction<String, Read, String> {

    private static final String DESCRIBE = "DESCRIBE ";
    private static final String EXPLAIN = "EXPLAIN ";

    public String explain(String query) {
      return apply(EXPLAIN.concat(query), new ReadExplain());
    }

    public String describe(String tableName) {
      return apply(DESCRIBE.concat(tableName), new ReadDescribe());
    }

    @Override
    public String apply(String query, Read read) {
      try (Connection conn = ConnectionPool.nifiPoolFromDefaultProperties().getConnection();
          Statement stmt = conn.createStatement();
          ResultSet result = stmt.executeQuery(query);) {
        return read.apply(result);
      } catch (Exception ex) {
        throw Throwables.propagate(ex);
      }
    }

  }

  /**
   * 
   * Validate SQL download query for list of checks and return {@link Result}.
   *
   */
  public static class Validate implements Function<String, HiveSQL.Validate.Result> {

    private static final String NEW_LINE = "/n";
    private static final String TAB = "\t";
    private static final String ALL_ROWS = "*";
    private static final String ALL_ROWS_NAMES =
        "gbifid    v_abstract  v_accessrights  v_accrualmethod v_accrualperiodicity    v_accrualpolicy v_alternative   v_audience  v_available v_bibliographiccitation v_conformsto    v_contributor   v_coverage  v_created   v_creator   v_date  v_dateaccepted  v_datecopyrighted   v_datesubmitted v_description   v_educationlevel    v_extent    v_format    v_hasformat v_haspart   v_hasversion    v_identifier    v_instructionalmethod   v_isformatof    v_ispartof  v_isreferencedby    v_isreplacedby  v_isrequiredby  v_isversionof   v_issued    v_language  v_license   v_mediator  v_medium    v_modified  v_provenance    v_publisher v_references    v_relation  v_replaces  v_requires  v_rights    v_rightsholder  v_source    v_spatial   v_subject   v_tableofcontents   v_temporal  v_title v_type  v_valid v_institutionid v_collectionid  v_datasetid v_institutioncode   v_collectioncode    v_datasetname   v_ownerinstitutioncode  v_basisofrecord v_informationwithheld   v_datageneralizations   v_dynamicproperties v_occurrenceid  v_catalognumber v_recordnumber  v_recordedby    v_individualcount   v_organismquantity  v_organismquantitytype  v_sex   v_lifestage v_reproductivecondition v_behavior  v_establishmentmeans    v_occurrencestatus  v_preparations  v_disposition   v_associatedmedia   v_associatedreferences  v_associatedsequences   v_associatedtaxa    v_othercatalognumbers   v_occurrenceremarks v_organismid    v_organismname  v_organismscope v_associatedoccurrences v_associatedorganisms   v_previousidentifications   v_organismremarks   v_materialsampleid  v_eventid   v_parenteventid v_fieldnumber   v_eventdate v_eventtime v_startdayofyear    v_enddayofyear  v_year  v_month v_day   v_verbatimeventdate v_habitat   v_samplingprotocol  v_samplingeffort    v_samplesizevalue   v_samplesizeunit    v_fieldnotes    v_eventremarks  v_locationid    v_highergeographyid v_highergeography   v_continent v_waterbody v_islandgroup   v_island    v_country   v_countrycode   v_stateprovince v_county    v_municipality  v_locality  v_verbatimlocality  v_minimumelevationinmeters  v_maximumelevationinmeters  v_verbatimelevation v_minimumdepthinmeters  v_maximumdepthinmeters  v_verbatimdepth v_minimumdistanceabovesurfaceinmeters   v_maximumdistanceabovesurfaceinmeters   v_locationaccordingto   v_locationremarks   v_decimallatitude   v_decimallongitude  v_geodeticdatum v_coordinateuncertaintyinmeters v_coordinateprecision   v_pointradiusspatialfit v_verbatimcoordinates   v_verbatimlatitude  v_verbatimlongitude v_verbatimcoordinatesystem  v_verbatimsrs   v_footprintwkt  v_footprintsrs  v_footprintspatialfit   v_georeferencedby   v_georeferenceddate v_georeferenceprotocol  v_georeferencesources   v_georeferenceverificationstatus    v_georeferenceremarks   v_geologicalcontextid   v_earliesteonorlowesteonothem   v_latesteonorhighesteonothem    v_earliesteraorlowesterathem    v_latesteraorhighesterathem v_earliestperiodorlowestsystem  v_latestperiodorhighestsystem   v_earliestepochorlowestseries   v_latestepochorhighestseries    v_earliestageorloweststage  v_latestageorhigheststage   v_lowestbiostratigraphiczone    v_highestbiostratigraphiczone   v_lithostratigraphicterms   v_group v_formation v_member    v_bed   v_identificationid  v_identificationqualifier   v_typestatus    v_identifiedby  v_dateidentified    v_identificationreferences  v_identificationverificationstatus  v_identificationremarks v_taxonid   v_scientificnameid  v_acceptednameusageid   v_parentnameusageid v_originalnameusageid   v_nameaccordingtoid v_namepublishedinid v_taxonconceptid    v_scientificname    v_acceptednameusage v_parentnameusage   v_originalnameusage v_nameaccordingto   v_namepublishedin   v_namepublishedinyear   v_higherclassification  v_kingdom   v_phylum    v_class v_order v_family    v_genus v_subgenus  v_specificepithet   v_infraspecificepithet  v_taxonrank v_verbatimtaxonrank v_scientificnameauthorship  v_vernacularname    v_nomenclaturalcode v_taxonomicstatus   v_nomenclaturalstatus   v_taxonremarks  identifiercount crawlid fragmentcreated xmlschema   publishingorgkey    unitqualifier   networkkey  installationkey abstract    accessrights    accrualmethod   accrualperiodicity  accrualpolicy   alternative audience    available   bibliographiccitation   conformsto  contributor coverage    created creator date_   dateaccepted    datecopyrighted datesubmitted   description educationlevel  extent  format_ hasformat   haspart hasversion  identifier  instructionalmethod isformatof  ispartof    isreferencedby  isreplacedby    isrequiredby    isversionof issued  language    license mediator    medium  modified    provenance  publisher   references  relation    replaces    requires    rights  rightsholder    source  spatial subject tableofcontents temporal    title   type    valid   institutionid   collectionid    datasetid   institutioncode collectioncode  datasetname ownerinstitutioncode    basisofrecord   informationwithheld datageneralizations dynamicproperties   occurrenceid    catalognumber   recordnumber    recordedby  individualcount organismquantity    organismquantitytype    sex lifestage   reproductivecondition   behavior    establishmentmeans  occurrencestatus    preparations    disposition associatedreferences    associatedsequences associatedtaxa  othercatalognumbers occurrenceremarks   organismid  organismname    organismscope   associatedoccurrences   associatedorganisms previousidentifications organismremarks materialsampleid    eventid parenteventid   fieldnumber eventdate   eventtime   startdayofyear  enddayofyear    year    month   day verbatimeventdate   habitat samplingprotocol    samplingeffort  samplesizevalue samplesizeunit  fieldnotes  eventremarks    locationid  highergeographyid   highergeography continent   waterbody   islandgroup island  countrycode stateprovince   county  municipality    locality    verbatimlocality    verbatimelevation   verbatimdepth   minimumdistanceabovesurfaceinmeters maximumdistanceabovesurfaceinmeters locationaccordingto locationremarks decimallatitude decimallongitude    coordinateuncertaintyinmeters   coordinateprecision pointradiusspatialfit   verbatimcoordinatesystem    verbatimsrs footprintwkt    footprintsrs    footprintspatialfit georeferencedby georeferenceddate   georeferenceprotocol    georeferencesources georeferenceverificationstatus  georeferenceremarks geologicalcontextid earliesteonorlowesteonothem latesteonorhighesteonothem  earliesteraorlowesterathem  latesteraorhighesterathem   earliestperiodorlowestsystem    latestperiodorhighestsystem earliestepochorlowestseries latestepochorhighestseries  earliestageorloweststage    latestageorhigheststage lowestbiostratigraphiczone  highestbiostratigraphiczone lithostratigraphicterms group_  formation   member  bed identificationid    identificationqualifier typestatus  identifiedby    dateidentified  identificationreferences    identificationverificationstatus    identificationremarks   taxonid scientificnameid    acceptednameusageid parentnameusageid   originalnameusageid nameaccordingtoid   namepublishedinid   taxonconceptid  scientificname  acceptednameusage   parentnameusage originalnameusage   nameaccordingto namepublishedin namepublishedinyear higherclassification    kingdom phylum  class   order_  family  genus   subgenus    specificepithet infraspecificepithet    taxonrank   verbatimtaxonrank   vernacularname  nomenclaturalcode   taxonomicstatus nomenclaturalstatus taxonremarks    datasetkey  publishingcountry   lastinterpreted elevation   elevationaccuracy   depth   depthaccuracy   distanceabovesurface    distanceabovesurfaceaccuracy    issue   hascoordinate   hasgeospatialissues taxonkey    acceptedtaxonkey    kingdomkey  phylumkey   classkey    orderkey    familykey   genuskey    subgenuskey specieskey  species genericname acceptedscientificname  typifiedname    protocol    lastparsed  lastcrawled repatriated mediatype   ext_multimedia/n";
    private static final List<Rule> ruleBase = Arrays.asList(new OnlyPureSelectQueriesAllowedRule(), new OnlyOneSelectAllowedRule(), new DatasetKeyAndLicenseRequiredRule(), new TableNameShouldBeOccurrenceRule());
    
    public static class Result {
      private final String sql;
      private final List<Issue> issues;
      private final boolean ok;
      private final String explain;
      private final String transsql;
      private final String sqlHeader;
      
      Result(String sql, String transsql, List<Issue> issues, String queryExplanation, String sqlHeader, boolean ok) {
        this.sql = sql;
        this.transsql = transsql;
        this.issues = issues;
        this.ok = ok;
        this.sqlHeader = sqlHeader;
        this.explain = queryExplanation;
      }

      public String sql() {
        return sql;
      }

      public List<Issue> issues() {
        return issues;
      }

      public boolean isOk() {
        return ok;
      }

      public String explain() {
        return explain;
      }
      
      public String transsql() {
        return transsql;
      }
      
      public String sqlHeader() {
        return sqlHeader;
      }

      @Override
      public String toString() {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("sql", sql);
        ArrayNode issuesNode = JsonNodeFactory.instance.arrayNode();
        issues.forEach(issue -> issuesNode.add(issue.description().concat(issue.comment())));
        node.put("issues", issuesNode);
        node.put("explain", new TextNode(explain));
        node.put("ok", isOk());
        return node.toString();
      }
    }

    @Override
    public HiveSQL.Validate.Result apply(String sql) {
      List<Issue> issues = Lists.newArrayList();

      QueryContext context = QueryContext.from(sql).onParseFail(issues::add);
      if (context.hasParseIssue())
        return new Result(context.sql(), context.translatedQuery(), issues, SQLShouldBeExecutableRule.COMPILATION_ERROR,"", issues.isEmpty());

      
      ruleBase.forEach(rule -> rule.apply(context).onViolation(issues::add));

      // SQL should be executable.
      SQLShouldBeExecutableRule executableRule = new SQLShouldBeExecutableRule();
      executableRule.apply(context).onViolation(issues::add);
      
      String sqlHeader = context.selectFieldNames().stream().collect(Collectors.joining(TAB)).concat(NEW_LINE);
      if (sqlHeader.trim().equals(ALL_ROWS.concat(NEW_LINE))) {
        sqlHeader = ALL_ROWS_NAMES;
      }
      return new Result(context.sql(), context.translatedQuery(), issues, executableRule.explainValue(), sqlHeader, issues.isEmpty());
    }

  }

}
