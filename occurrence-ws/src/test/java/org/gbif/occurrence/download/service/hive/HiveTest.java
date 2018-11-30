package org.gbif.occurrence.download.service.hive;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.junit.Test;

public class HiveTest {

  @Test
  public void test1() throws ParseException {
    ParseDriver driver = new ParseDriver();
      ASTNode node = driver.parse("SELECT gbifid, countrycode, datasetkey, license FROM occurrence WHERE month=3 and year=2004");
//    ASTNode node = driver.parse("SELECT COUNT(countrycode) CODE FROM occurrence WHERE month IN (Select month from occurrence)  and year=2004");

    System.out.println(node.dump());
    //print(node);
    
    String tableName = search(node,"TOK_TABNAME").map((searchNode) -> {
      ASTNode childNode = (ASTNode)searchNode.getChildren().get(0);
      return childNode.getText();
    }).get();
    System.out.println(tableName);
    
    List<String> fieldNames = searchMulti(node,"TOK_SELEXPR").stream().map((searchNode) -> {
      return ((ASTNode)searchNode.getChildren().get(0).getChildren().get(0)).getText();
    }).collect(Collectors.toList());
    System.out.println(fieldNames);
    
    String where = search(node,"TOK_WHERE").map((searchNode) -> {
      return ((ASTNode)searchNode).toStringTree();
    }).get();
    System.out.println(where);
    
    
  }
  
  void print(ASTNode t){
      LinkedList<Node> list = new LinkedList<>(t.getChildren());
      while(!list.isEmpty()) {
        
       Node n =  list.poll();
       System.out.println(((ASTNode)n).getText()); 
       
       Optional.ofNullable(n.getChildren()).ifPresent(x-> x.forEach(list::add));
      }
  }
  
  
  Optional<Node> search(ASTNode node, String token) {
    LinkedList<Node> list = new LinkedList<>(node.getChildren());
    while(!list.isEmpty()) {
      
     Node n =  list.poll();
     if(((ASTNode)n).getText().equals(token))
       return Optional.of(n);
     Optional.ofNullable(n.getChildren()).ifPresent(x-> x.forEach(list::add));
    }
    return Optional.empty();
  }
  
  List<Node> searchMulti(ASTNode node, String token) {
    LinkedList<Node> list = new LinkedList<>(node.getChildren());
    ArrayList<Node> listOfSearchedNode = new ArrayList<>();
    while(!list.isEmpty()) {
      
     Node n =  list.poll();
     if(((ASTNode)n).getText().equals(token))
       listOfSearchedNode.add(n);
     Optional.ofNullable(n.getChildren()).ifPresent(x-> x.forEach(list::add));
    }
    return listOfSearchedNode;
  }
}
