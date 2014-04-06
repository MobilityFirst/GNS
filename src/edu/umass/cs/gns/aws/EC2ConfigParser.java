/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.aws;

import edu.umass.cs.amazontools.AMIRecordType;
import edu.umass.cs.amazontools.RegionRecord;
import edu.umass.cs.gns.database.DataStoreType;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 * @author westy
 */
public class EC2ConfigParser {

  private String ec2username;
  private AMIRecordType amiRecordType;
  private DataStoreType dataStoreType;
  private List<EC2RegionSpec> regions = new ArrayList<EC2RegionSpec>();

  public String getEc2username() {
    return ec2username;
  }

  public AMIRecordType getAmiRecordType() {
    return amiRecordType;
  }

  public DataStoreType getDataStoreType() {
    return dataStoreType;
  }

  public List<EC2RegionSpec> getRegions() {
    return regions;
  }

  public EC2ConfigParser(String filename) {
    parseFile(filename);
  }

  public void parseFile(String filename) {
    InputStream is = ClassLoader.getSystemResourceAsStream(filename + ".xml");
    try {
      //File fXmlFile = new File(filename);
      DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
      Document doc = dBuilder.parse(is);
      //Document doc = dBuilder.parse(fXmlFile);

//	//optional, but recommended
//	//read this - http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
//	doc.getDocumentElement().normalize();

      //System.out.println("Root element: " + doc.getDocumentElement().getNodeName());

      NodeList nList = doc.getElementsByTagName("region");

      for (int temp = 0; temp < nList.getLength(); temp++) {

        Node nNode = nList.item(temp);

        //System.out.println("\nCurrent Element: " + nNode.getNodeName());

        if (nNode.getNodeType() == Node.ELEMENT_NODE) {

          Element eElement = (Element) nNode;

          EC2RegionSpec regionSpec = new EC2RegionSpec(RegionRecord.valueOf(eElement.getAttribute("name")),
                  Integer.parseInt(eElement.getAttribute("cnt")),
                  eElement.getAttribute("ip"));
          regions.add(regionSpec);

          //System.out.println("Region Spec: " + regionSpec.toString());
        }
      }
      
      ec2username = ((Element) doc.getElementsByTagName("ec2username").item(0)).getAttribute("name");
      amiRecordType = AMIRecordType.valueOf(((Element) doc.getElementsByTagName("ami").item(0)).getAttribute("name"));
      dataStoreType = DataStoreType.valueOf(((Element) doc.getElementsByTagName("datastore").item(0)).getAttribute("name"));

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
