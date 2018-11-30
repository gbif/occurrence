package org.gbif.occurrence.download.service.hive.validation2;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.gbif.occurrence.download.service.hive.validation.Query.Issue;

public class Hive {

  public static class QueryContext {
    private final String sql;
    private final Optional<ASTNode> queryNode;
    private final Issue issue;
    private final Optional<ParseException> parseException;

    private QueryContext(String sql, Optional<ASTNode> queryNode, Issue issue, Optional<ParseException> parseException) {
      this.sql = sql;
      this.issue = issue;
      this.queryNode = queryNode;
      this.parseException = parseException;
    }

    public String sql() {
      return sql;
    }

    public Optional<ASTNode> queryNode() {
      return queryNode;
    }

    public void onParseFailed(BiConsumer<Issue, Exception> consumer) {
      if (parseException.isPresent())
        consumer.accept(issue, parseException.get());
    }

    public boolean hasParseIssues() {
      return !issue.equals(Issue.NO_ISSUE);
    }

    public Optional<ParseException> getParseException() {
      return parseException;
    }

    public static Optional<Node> search(ASTNode node, String token) {
      LinkedList<Node> list = new LinkedList<>(node.getChildren());
      while (!list.isEmpty()) {

        Node n = list.poll();
        if (((ASTNode) n).getText().equals(token))
          return Optional.of(n);
        Optional.ofNullable(n.getChildren()).ifPresent(x -> x.forEach(list::add));
      }
      return Optional.empty();
    }

    public static List<Node> searchMulti(ASTNode node, String token) {
      LinkedList<Node> list = new LinkedList<>(node.getChildren());
      ArrayList<Node> listOfSearchedNode = new ArrayList<>();
      while (!list.isEmpty()) {

        Node n = list.poll();
        if (((ASTNode) n).getText().equals(token))
          listOfSearchedNode.add(n);
        Optional.ofNullable(n.getChildren()).ifPresent(x -> x.forEach(list::add));
      }
      return listOfSearchedNode;
    }
  }

  public static class Parser {
    private Parser() {}
    public static QueryContext parse(String sql) {
      ParseDriver driver = new ParseDriver();
      try {
        ASTNode queryNode = driver.parse(sql);
        return new QueryContext(sql, Optional.of(queryNode), Issue.NO_ISSUE, Optional.empty());
      } catch (ParseException e) {
        return new QueryContext(sql, Optional.empty(), Issue.PARSE_FAILED, Optional.of(e));
      }
    }
  }


}
