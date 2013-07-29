package edu.umass.cs.gns.util;

import edu.umass.cs.gns.main.GNS;
import java.io.*;
import java.util.*;
import javax.xml.parsers.*;
import org.w3c.dom.*;
import org.xml.sax.*;

/**
 *   This class provides a parse for one simple type of XML config file. Namely ones that look like this:
 *   <tag>
 *     <tag key1="value1" key2="value2" ... /> // line 0
 *     <tag key1="value1" key2="value2" ... /> // line 1
 *     ...
 *   </tag>
 *   where the tags, keys and values can be anything you want.
 * 
 * Common usages is to iterate over the elements using the line index and key to extract values.
 *
 * @author	Westy (westy@cs.umass.edu)
 *
 */
public class XMLParser {
    private ArrayList<Element> elements = new ArrayList<Element>();
    
    public XMLParser(String filename) {
	parseXMLFile(filename);
    }
    
    public String getAttribute(int index, String name) {
	return getAttribute(index, name, false);
    }
    
    public String getAttribute(int index, String name, String defaultValue) {
	return getAttribute(index, name, false, defaultValue);
    }
    
    public String getAttribute(int index, String name, boolean required) {
	return getAttribute(index, name, required, null);
    }
    
    private String getAttribute(int index, String name, boolean required, String defaultValue) throws AttributeParseException {
	if (elements.get(index).hasAttribute(name)) {
	    return elements.get(index).getAttribute(name);
	} else if (required) {
	    throw new AttributeParseException(name, elements.get(index));
	}
	return defaultValue;
    }

    public int size() {
	return elements.size();
    }

    private void parseXMLFile(String filename) {
	InputStream is=ClassLoader.getSystemResourceAsStream(filename+".xml");
	try {
	    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	    DocumentBuilder builder = factory.newDocumentBuilder();
	    Document doc = builder.parse(is);
	    Element root = doc.getDocumentElement();
	    GNS.getLogger().finer("root = " + root);
	    NodeList nodes = root.getChildNodes();
	    GNS.getLogger().finer("nodes = " + nodes + " length = " + nodes.getLength());
	    for (int i = 0; i < nodes.getLength(); i++) {
		Node item = nodes.item(i);
		GNS.getLogger().finer("item = " + item);
		if (item instanceof Element) {
		    Element element = (Element) item;
		    GNS.getLogger().finer("element = " + element);
		    elements.add(element);
		}
	    }
	} catch (IOException e) {
	    GNS.getLogger().severe("Exception: "+e);
	    e.printStackTrace();
	} catch (ParserConfigurationException e) {
	     GNS.getLogger().severe("Exception: "+e);
	    e.printStackTrace();
	} catch (SAXException e) {
	     GNS.getLogger().severe("Exception: "+e);
	    e.printStackTrace();
	}
    }

    
    public static class AttributeParseException extends RuntimeException
    {
	String name;
	Element element;

	public AttributeParseException(String name, Element element) 
	{ 
	    this.name = name;
	    this.element = element;
	} 

	public String getName() {
	    return this.name;
	}

	public Element getElement() {
	    return this.element;
	}
    }
}
