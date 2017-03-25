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

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Writes out configuration files for an set of running EC2 hosts created by
 * {@link EC2Runner}.
 *
 * @author westy
 */
public class WriteConfFile {

  /**
   * Write all the config files.
   *
   * @param configName
   * @param directory
   * @param keyName
   * @param ec2UserName
   * @param hostType
   * @param datastore
   * @param idTable
   */
  public static void writeConfFiles(String configName, String directory, String keyName, String ec2UserName, String hostType, String datastore,
          ConcurrentHashMap<String, HostInfo> idTable) {
    System.out.println("Config directory: " + directory);
    if (new File(directory).mkdirs()) {
      writeInstallerConfigFile(configName, directory, keyName, ec2UserName, hostType, datastore);
      writeNsHostsFile(directory, idTable);
      writeLnsHostsFile(directory, idTable);
    }
  }

  private static void writeInstallerConfigFile(String configName, String directory, String keyName, String ec2UserName, String hostType, String datastore) {
    try {
      Properties props = new Properties();
      props.setProperty("userName", ec2UserName);
      props.setProperty("keyFile", keyName + ".pem");
      props.setProperty("hostType", hostType);
      props.setProperty("datastore", datastore);
      File f = new File(directory + System.getProperty("file.separator") + "installer_config");
      OutputStream out = new FileOutputStream(f);
      props.store(out, "Auto generated installer config file for " + configName);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void writeNsHostsFile(String directory, ConcurrentHashMap<String, HostInfo> idTable) {
    try {
      FileWriter fw = new FileWriter(directory + System.getProperty("file.separator") + "ns_hosts.txt");
      for (HostInfo info : idTable.values()) {
        // NS ID IS ALWAYS NULL HERE.. NEED SOMETHING ELSE
        fw.write(info.getNsId() + " " + info.getHostname() + " " + info.getHostIP()
                + System.getProperty("line.separator"));
      }
      fw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void writeLnsHostsFile(String directory, ConcurrentHashMap<String, HostInfo> idTable) {
    try {
      FileWriter fw = new FileWriter(directory + System.getProperty("file.separator") + "lns_hosts.txt");
      for (HostInfo info : idTable.values()) {
        fw.write(info.getHostname() + System.getProperty("line.separator"));
      }
      fw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

// Something like this:
//<?xml version="1.0" encoding="UTF-8" standalone="no"?>
//<root>
//  <ec2username name="ec2-user"/>
//  <keyname name="aws"/>
//  <hosttype name="linux"/>
//  <datastore name="MONGO"/>
//  <host hostname="ec2-54-253-252-188.ap-southeast-2.compute.amazonaws.com" id="6" ip="54.253.252.188" lat="-33.8615" lon="151.205505"/>
//  <host hostname="ec2-54-238-57-233.ap-northeast-1.compute.amazonaws.com" id="5" ip="54.238.57.233" lat="47.634399" lon="-122.342201"/>
//  <host hostname="ec2-23-21-160-80.compute-1.amazonaws.com" id="0" ip="23.21.160.80" lat="39.043701" lon="-77.487503"/>
//  <host hostname="ec2-79-125-27-206.eu-west-1.compute.amazonaws.com" id="7" ip="79.125.27.206" lat="53.0" lon="-8.0"/>
//</root>
  private static void writeXMLFile(String filename, String keyName, String ec2UserName, String hostType, String datastore,
          ConcurrentHashMap<String, HostInfo> idTable) {

    try {

      DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

      // root elements
      Document doc = docBuilder.newDocument();
      Element rootElement = doc.createElement("root");
      doc.appendChild(rootElement);

      // Elements
      Element element = doc.createElement("ec2username");
      rootElement.appendChild(element);
      Attr attr = doc.createAttribute("name");
      attr.setValue(ec2UserName);
      element.setAttributeNode(attr);

      element = doc.createElement("keyname");
      rootElement.appendChild(element);
      attr = doc.createAttribute("name");
      attr.setValue(keyName);
      element.setAttributeNode(attr);

      element = doc.createElement("hosttype");
      rootElement.appendChild(element);
      attr = doc.createAttribute("name");
      attr.setValue(hostType);
      element.setAttributeNode(attr);

      element = doc.createElement("datastore");
      rootElement.appendChild(element);
      attr = doc.createAttribute("name");
      attr.setValue(datastore);
      element.setAttributeNode(attr);

      for (HostInfo info : idTable.values()) {
        element = doc.createElement("host");
        rootElement.appendChild(element);

        attr = doc.createAttribute("id");
        attr.setValue(info.getId().toString());
        element.setAttributeNode(attr);

        attr = doc.createAttribute("hostname");
        attr.setValue(info.getHostname());
        element.setAttributeNode(attr);

        attr = doc.createAttribute("lat");
        attr.setValue(Double.toString(info.getLocation().getY()));
        element.setAttributeNode(attr);

        attr = doc.createAttribute("lon");
        attr.setValue(Double.toString(info.getLocation().getX()));
        element.setAttributeNode(attr);
      }

      // write the content into xml file
      TransformerFactory transformerFactory = TransformerFactory.newInstance();
      transformerFactory.setAttribute("indent-number", 4);
      Transformer transformer = transformerFactory.newTransformer();
      transformer.setOutputProperty(OutputKeys.INDENT, "yes");
      DOMSource source = new DOMSource(doc);
      StreamResult result = new StreamResult(new File(filename));

      // Output to console for testing
      // StreamResult result = new StreamResult(System.out);
      transformer.transform(source, result);

      System.out.println("New config file writen to " + filename);

    } catch (ParserConfigurationException pce) {
      pce.printStackTrace();
    } catch (TransformerException tfe) {
      tfe.printStackTrace();
    }
  }

  /**
   * The main routine. For testing only.
   * 
   * @param argv
   */
  public static void main(String argv[]) {
    ConcurrentHashMap<String, HostInfo> idTable = new ConcurrentHashMap<>();
    idTable.put("0", new HostInfo("0", "host1", null));
    idTable.put("1", new HostInfo("1", "host2", null));
    idTable.put("2", new HostInfo("2", "host3", null));
    writeXMLFile("frank", "aws", "ec2-user", "linux", "MONGO", idTable);
  }

}
