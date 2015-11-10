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
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.noopTest;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import edu.umass.cs.nio.interfaces.NodeConfig;

import java.util.HashSet;

import org.json.JSONArray;
import org.json.JSONException;

/**
 * @author Westy adapted from code originally by V. Arun
 * @param <NodeIDType>
 *
 */
public class TestNodeConfig<NodeIDType> implements
        NodeConfig<NodeIDType> {

  /**
   * The default starting port number beyond which nodes automatically get
   * port numbers assigned.
   */
  public static final int DEFAULT_PORT = 20000;
  private HashMap<NodeIDType, InetAddress> nmap = new HashMap<NodeIDType, InetAddress>();
  private final int defaultPort;

  /**
   * Creates a TestNodeConfig instance.
   * 
   * @param defaultPort
   * Assigns port numbers to nodes starting from defaultPort
   */
  public TestNodeConfig(int defaultPort) {
    this.defaultPort = defaultPort;
  }

  /**
   * Creates a TestNodeConfig instance with the default port.
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

  @Override
  public InetAddress getBindAddress(NodeIDType ID) {
    return nmap.get(ID);
  }

  /**
   * Maps each node ID to a port number.
   *
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
