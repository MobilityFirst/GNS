/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.installer;


import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.utils.GEOLocator;
import java.awt.geom.Point2D;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Parses an XML config file to get all info needed create hosts.
 * 
 * @author westy
 */
public class HostConfigParser {

  private static final String USERNAME = "username";
  private static final String USERNAME_OLD = "ec2username"; // for backward compatibility
  private static final String KEYNAME = "keyname";
  private static final String HOSTTYPE = "hosttype";
  private static final String DATASTORE = "datastore";
  private static final String INSTALLPATH = "installpath";
  private static final String ID = "id";
  private static final String HOSTNAME = "hostname";
  private static final String LON = "lon";
  private static final String LAT = "lat";

  private static final String fileExtension = ".xml";
  private static final String folder = "gnsInstaller";

  private String keyname;
  private String username;
  private String hostType;
  private DataStoreType dataStoreType;
  private String installPath;
  private List<HostInfo> hosts = new ArrayList<HostInfo>();

  /**
   * Returns the keyname.
   * 
   * @return the keyname
   */
  public String getKeyname() {
    return keyname;
  }

  /**
   * Returns the username.
   * 
   * @return a string
   */
  public String getUsername() {
    return username;
  }

  /**
   * Returns the host type.
   * 
   * @return the host type.
   */
  public String getHostType() {
    return hostType;
  }

  /**
   * Returns the dataStoreType.
   * 
   * @return the dataStoreType
   */
  public DataStoreType getDataStoreType() {
    return dataStoreType;
  }

  /**
   * Returns the install path.
   * 
   * @return the install path.
   */
  public String getInstallPath() {
    return installPath;
  }

  /**
   * Returns the list of hosts.
   * 
   * @return the list of hosts
   */
  public List<HostInfo> getHosts() {
    return hosts;
  }

  /**
   * Creates a HostConfigParser instance.
   * 
   * @param filename
   * @throws HostConfigParseException
   */
  public HostConfigParser(String filename) throws HostConfigParseException {
    parseFile(filename);
  }

  private void parseFile(String filename) throws HostConfigParseException {
    String confPath = getConfPath();
    if (confPath == null) {
      return;
    }
    Document doc = null;
    try {
      InputStream is = Files.newInputStream(Paths.get(confPath, folder, filename + fileExtension));
      //InputStream is = ClassLoader.getSystemResourceAsStream(filename + ".xml");
      //File fXmlFile = new File(filename);
      DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
      doc = dBuilder.parse(is);
    } catch (IOException | ParserConfigurationException | SAXException e) {
      throw new HostConfigParseException("Problem creating XML document " + e);
    }
//	//optional, but recommended
//	//read this - http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
//	doc.getDocumentElement().normalize();
    NodeList nList = doc.getElementsByTagName("host");

    for (int temp = 0; temp < nList.getLength(); temp++) {

      Node nNode = nList.item(temp);

      if (nNode.getNodeType() == Node.ELEMENT_NODE) {
        Element eElement = (Element) nNode;
        String idString = eElement.getAttribute(ID);
        String hostname = eElement.getAttribute(HOSTNAME);
        String latString = eElement.getAttribute(LAT);
        String lonString = eElement.getAttribute(LON);
        if (idString.isEmpty() || hostname.isEmpty()) {
          throw new HostConfigParseException("Missing id or hostname attribute!");
        }
        //int id = Integer.parseInt(idString);
        Point2D location = null;
        if (!lonString.isEmpty() && !latString.isEmpty()) {
          location = new Point2D.Double(Double.parseDouble(lonString), Double.parseDouble(latString));
        }
        if (location == null) {
          InetAddress inetAddress = null;
          try {
            inetAddress = InetAddress.getByName(hostname);
          } catch (UnknownHostException e) {
            throw new HostConfigParseException("Problem looking up IP address " + e);
          }
          String ip = inetAddress.getHostAddress();
          // and take a guess at the location (lat, long) of this host
          location = GEOLocator.lookupIPLocation(ip);
        }
        hosts.add(new HostInfo(idString, hostname, location));
      }
    }
    keyname = getSingleElementAttribute(doc, KEYNAME, "name");
    username = getSingleElementAttribute(doc, USERNAME, "name");
    hostType = getSingleElementAttribute(doc, HOSTTYPE, "name");
    installPath = getSingleElementAttribute(doc, INSTALLPATH, "name");
    // if last character is a / remove it
    if (installPath != null && !installPath.isEmpty()) {
      installPath = installPath.trim();
      if (installPath.endsWith(System.getProperty("file.separator"))) {
        installPath = installPath.substring(0, installPath.length() - 1);
      }
    }
    String dataStoreTypeName = getSingleElementAttribute(doc, DATASTORE, "name");
    if (username == null) { // for backwards compatibility
      username = getSingleElementAttribute(doc, USERNAME_OLD, "name");
      if (username != null) {
        System.out.println("!!!Use of deprecated element tag " + USERNAME_OLD + ". Fix this!!!");
      }
    }
    if (keyname == null || username == null || hostType == null || dataStoreTypeName == null) {
      throw new HostConfigParseException("Missing " + KEYNAME + " or " + USERNAME + " or " + HOSTTYPE + " or " + DATASTORE + " tag");
    }
    dataStoreType = DataStoreType.valueOf(dataStoreTypeName);
  }

  private String getSingleElementAttribute(Document doc, String tagName, String attributeName) {
    NodeList nodeList = doc.getElementsByTagName(tagName);
    if (nodeList.getLength() > 0) {
      return ((Element) nodeList.item(0)).getAttribute(attributeName);
    } else {
      return null;
    }
  }

  private static String getConfPath() {
    try {
      File jarLoc = new File(GNS.class.getProtectionDomain().getCodeSource().getLocation().toURI());
      return jarLoc.getParentFile() + "/conf/";
    } catch (URISyntaxException e) {
      GNS.getLogger().info("Unable to get conf location: " + e);
      return null;
    }
  }
}
