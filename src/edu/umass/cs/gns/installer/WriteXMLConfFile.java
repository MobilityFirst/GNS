/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.installer;

import java.io.File;
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

public class WriteXMLConfFile {

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
  public static void writeFile(String filename, String keyName, String ec2UserName, String hostType, String datastore,
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

  @SuppressWarnings("unchecked")
  public static void main(String argv[]) {
    ConcurrentHashMap<String, HostInfo> idTable = new ConcurrentHashMap();
    idTable.put("0", new HostInfo("0", "host1", null));
    idTable.put("1", new HostInfo("1", "host2", null));
    idTable.put("2", new HostInfo("2", "host3", null));
    writeFile("frank", "aws", "ec2-user", "linux", "MONGO", idTable);
  }

}
