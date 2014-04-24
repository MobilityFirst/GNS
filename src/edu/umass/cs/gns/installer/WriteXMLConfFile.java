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
// <root>
//  <ec2username name = "ec2-user"/>
//  <hosttype name="linux"/>
//  <datastore name = "MONGO"/>
//  <host id="0" ip="ec2-67-202-13-126.compute-1.amazonaws.com"/>
//  <host id="1" ip="ec2-184-169-217-139.us-west-1.compute.amazonaws.com"/>
//  <host id="2" ip="ec2-54-184-69-227.us-west-2.compute.amazonaws.com"/>
// </root>
  public static void writeFile(String filename, String keyName, String ec2UserName, String hostType, String datastore,
          ConcurrentHashMap<Integer, HostInfo> idTable) {

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
        attr.setValue(Integer.toString(info.getId()));
        element.setAttributeNode(attr);
        
        attr = doc.createAttribute("hostname");
        attr.setValue(info.getHostname());
        element.setAttributeNode(attr);
        
        attr = doc.createAttribute("ip");
        attr.setValue(info.getIp());
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
  
  public static void main(String argv[]) {
    ConcurrentHashMap<Integer, HostInfo> idTable = new ConcurrentHashMap();
    idTable.put(0, new HostInfo(0, "host1", "127.0.0.1", null));
    idTable.put(1, new HostInfo(1, "host2", "127.0.0.2", null));
    idTable.put(2, new HostInfo(2, "host3", "127.0.0.3", null));
    writeFile("frank", "aws", "ec2-user", "linux", "MONGO", idTable);
  }
  
}
