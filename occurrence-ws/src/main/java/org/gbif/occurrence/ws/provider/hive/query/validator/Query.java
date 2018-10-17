package org.gbif.occurrence.ws.provider.hive.query.validator;

/**
 * 
 * Definition of Issues not allowed in query.
 *
 */
public class Query {
  private static final String FORMAT = "Format of LEGAL query is : SELECT ... \nFROM occurrence \nWHERE ... \nGROUP BY ... HAVING ...";

  public enum Issue {
    NO_ISSUE("No Issue"), 
    EMPTY_SQL("SQL cannot be empty"), 
    ONLY_ONE_SELECT_ALLOWED("SQL query should be a SELECT query with only one SELECT"), 
    DDL_JOINS_UNION_NOT_ALLOWED("SQL query cannot use INSERT, UPDATE, DELETE, MERGE, PROCEDURE_CALL, EXCEPT, INTERSECT, UNION, JOIN or AS"), 
    DATASET_AND_LICENSE_REQUIRED("SQL should select on 'datasetkey' and 'license' fields as they are required for citations"), 
    CANNOT_EXECUTE("Query cannot be executed because of "), 
    TABLE_NAME_NOT_OCCURRENCE("Query should have table name as occurrence"), 
    PARSE_FAILED(String.format("Cannot parse the query, Provided query should follow the format %n %s", FORMAT));


    private Issue(String description) {
      this.description = description;
    }

    private final String description;
    private String comment = "";

    public String description() {
      return description;
    }

    public String comment() {
      return comment;
    }

    public Issue withComment(String comment) {
      this.comment = comment;
      return this;
    }
  }
}
