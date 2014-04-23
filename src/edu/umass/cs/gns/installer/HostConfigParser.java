/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.installer;

import edu.umass.cs.gns.database.DataStoreType;

import edu.umass.cs.gns.main.GNS;
import java.awt.geom.Point2D;
import java.io.File;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
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

  private String keyname;
  private String username;
  private String hostType;
  private DataStoreType dataStoreType;
  private List<HostInfo> hosts = new ArrayList<HostInfo>();

  public String getKeyname() {
    return keyname;
  }

  public String getUsername() {
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
    String confPath = getConfPath();
    if (confPath == null) {
      return;
    }
    try {
      InputStream is = Files.newInputStream(Paths.get(confPath, filename + ".xml"));
        //InputStream is = ClassLoader.getSystemResourceAsStream(filename + ".xml");
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
          hosts.add(new HostInfo(Integer.parseInt(eElement.getAttribute("id")),
                  eElement.getAttribute("hostname"),
                  eElement.getAttribute("ip"),
                  new Point2D.Double(Double.parseDouble(eElement.getAttribute("long")),
                          Double.parseDouble(eElement.getAttribute("lat")))));
        }
      }
      keyname = ((Element) doc.getElementsByTagName("keyname").item(0)).getAttribute("name");
      username = ((Element) doc.getElementsByTagName("ec2username").item(0)).getAttribute("name");
      hostType = ((Element) doc.getElementsByTagName("hosttype").item(0)).getAttribute("name");
      dataStoreType = DataStoreType.valueOf(((Element) doc.getElementsByTagName("datastore").item(0)).getAttribute("name"));

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static String getConfPath() {
    try {
      File jarLoc = new File(GNS.class.getProtectionDomain().getCodeSource().getLocation().toURI());
      return jarLoc.getParentFile() + "/conf/";
    } catch (URISyntaxException e) {
      GNS.getLogger().info("Unable to get conf location: " + e);
      return null;
    }
  }
}
