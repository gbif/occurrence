/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.download.conf;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 * Creates properties files from a maven file that contains a profiles section.
 */
public class DownloadConfBuilder {

  private static final String XPATH_ENV_EXPR =
    "/*[name()=\"settings\"]/*[name()=\"profiles\"]/*[name()=\"profile\"][*[name()=\"id\" and text()=\"%s\"]]/*[name()=\"properties\"]";

  public static void main(String[] args) throws Exception {
    try (FileOutputStream fileOutputStream = new FileOutputStream(args[1])) {

      DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
      Document doc = dBuilder.parse(new File(args[2]));
      XPathFactory xPathfactory = XPathFactory.newInstance();
      XPath xpath = xPathfactory.newXPath();
      XPathExpression expr = xpath.compile(String.format(XPATH_ENV_EXPR, args[0]));
      Node nodeProperties = (Node) expr.evaluate(doc, XPathConstants.NODE);
      Properties properties = new Properties();
      for (int i = 0; i < nodeProperties.getChildNodes().getLength(); i++) {
        Node propertyNode = nodeProperties.getChildNodes().item(i);
        if (propertyNode.getNodeType() == Node.ELEMENT_NODE) {
          properties.setProperty(propertyNode.getNodeName(), propertyNode.getTextContent());
        }
      }
      System.out.println(properties);
      properties.store(fileOutputStream, null);
      fileOutputStream.flush();
    }
  }

  /**
   * Hidden constructor.
   */
  private DownloadConfBuilder() {
    //empty constructor
  }

}
