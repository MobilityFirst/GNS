/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.installer;

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
public class HostConfigParser {

  private String username;
  private String hostType;
  private DataStoreType dataStoreType;
  private List<HostInfo> hosts = new ArrayList<HostInfo>();

  public String getEc2username() {
    return username;
  }

  public String getHostType() {
    return hostType;
  }

  public DataStoreType getDataStoreType() {
    return dataStoreType;
  }

  public List<HostInfo> getHosts() {
    return hosts;
  }

  public HostConfigParser(String filename) {
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

      NodeList nList = doc.getElementsByTagName("host");

      for (int temp = 0; temp < nList.getLength(); temp++) {

        Node nNode = nList.item(temp);

        if (nNode.getNodeType() == Node.ELEMENT_NODE) {

          Element eElement = (Element) nNode;

          hosts.add(new HostInfo(Integer.parseInt(eElement.getAttribute("id")), eElement.getAttribute("ip")));
        }
      }
      
      username = ((Element) doc.getElementsByTagName("ec2username").item(0)).getAttribute("name");
      hostType = ((Element) doc.getElementsByTagName("hosttype").item(0)).getAttribute("name");
      dataStoreType = DataStoreType.valueOf(((Element) doc.getElementsByTagName("datastore").item(0)).getAttribute("name"));

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
