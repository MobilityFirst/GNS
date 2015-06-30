/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.newApp.noopTest;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import edu.umass.cs.nio.InterfaceNodeConfig;

import java.util.HashSet;

import org.json.JSONArray;
import org.json.JSONException;

/**
 * @author Westy adapted from code originally by V. Arun
 * @param <NodeIDType>
 *
 */
public class TestNodeConfig<NodeIDType> implements
        InterfaceNodeConfig<NodeIDType> {

  /**
   * The default starting port number beyond which nodes automatically get
   * port numbers assigned.
   */
  public static final int DEFAULT_PORT = 20000;
  private HashMap<NodeIDType, InetAddress> nmap = new HashMap<NodeIDType, InetAddress>();
  private final int defaultPort;

  /**
   * @param defaultPort
   * Assigns port numbers to nodes starting from defaultPort
   */
  public TestNodeConfig(int defaultPort) {
    this.defaultPort = defaultPort;
  }

  /**
   *
   */
  public TestNodeConfig() {
    this(DEFAULT_PORT);
  }

  @Override
  public boolean nodeExists(NodeIDType ID) {
    return nmap.containsKey(ID);
  }

  @Override
  public Set<NodeIDType> getNodeIDs() {
    return this.nmap.keySet();
  }

  @Override
  public InetAddress getNodeAddress(NodeIDType ID) {
    return nmap.get(ID);
  }

  /**
   * Maps each node ID to a port number.
   * @param ID
   */
  @Override
  public int getNodePort(NodeIDType ID) {
    assert nmap.get(ID) != null;
    return defaultPort;
  }

  /**
   * @return Set of all nodes.
   */
  public Set<NodeIDType> getNodes() {
    return nmap.keySet();
  }

  /**
   * Add node with id mapped to IP and an auto-selected port number.
   *
   * @param id Node id.
   * @param IP IP address.
   */
  public void add(NodeIDType id, InetAddress IP) {
    nmap.put(id, IP);
  }

  /**
   * Pretty prints this node config information.
   */
  public String toString() {
    String s = "";
    for (NodeIDType i : nmap.keySet()) {
      s += i + " : " + getNodeAddress(i) + ":" + getNodePort(i) + "\n";
    }
    return s;
  }

  @SuppressWarnings("unchecked")
  @Override
  public NodeIDType valueOf(String nodeAsString) {
    NodeIDType node = null;
    Iterator<NodeIDType> nodeIter = this.nmap.keySet().iterator();
    if (nodeIter.hasNext() && (node = nodeIter.next()) != null) {
      if (node instanceof String) {
        return (NodeIDType) nodeAsString;
      } else if (node instanceof Integer) {
        return (NodeIDType) (Integer.valueOf(nodeAsString.trim()));
      } else if (node instanceof InetAddress) {
        try {
          return (NodeIDType) (InetAddress.getByName(nodeAsString.trim()));
        } catch (UnknownHostException e) {
          e.printStackTrace();
        }
      }
    }
    return null;
  }

  @Override
  public Set<NodeIDType> getValuesFromStringSet(Set<String> strNodes) {
    Set<NodeIDType> nodes = new HashSet<>();
    for (String strNode : strNodes) {
      nodes.add(valueOf(strNode));
    }
    return nodes;
  }

  @Override
  public Set<NodeIDType> getValuesFromJSONArray(JSONArray array)
          throws JSONException {
    Set<NodeIDType> nodes = new HashSet<>();
    for (int i = 0; i < array.length(); i++) {
      nodes.add(valueOf(array.getString(i)));
    }
    return nodes;
  }

  private InetAddress getLocalAddress() {
    InetAddress localAddr = null;
    try {
      localAddr = InetAddress.getByName("localhost");
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    return localAddr;
  }

  static class Main {

    public static void main(String[] args) {
      int defaultPort = (args.length > 0 ? Integer.valueOf(args[0]) : 2222);
      TestNodeConfig<String> snc = new TestNodeConfig<>(defaultPort);

      System.out.println("Adding useast1 node");
      try {
        snc.add("useast1", InetAddress.getByName("localhost"));
      } catch (UnknownHostException e) {
        e.printStackTrace();
      }
      System.out.println("0 : " + snc.getNodeAddress("useast1") + ":"
              + snc.getNodePort("useast1"));
    }
  }

}
