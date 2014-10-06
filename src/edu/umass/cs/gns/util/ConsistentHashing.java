/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.util;

import edu.umass.cs.gns.main.GNS;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * This class contains the hash function used to find replica controllers (primary replicas)
 * of a name. The set of replica controllers is determined using consistent hashing.
 *
 * Before calling any method in this class, <code>reInitialize</code> method must be called. If <code>reInitialize</code>
 * method is called multiple times, the initialization is done based on the first call; later calls make no difference.
 *
 * The class is thread-safe.
 *
 * @author Abhigyan
 */
public class ConsistentHashing {

  private static int numReplicaControllers;

  /**
   * A treemap whose keys are hashes of ID of all name servers, and values are IDs of name servers.
   */
  private static TreeMap<String, Object> nsTreeMap;

  /**
   * Lock object for checking if <code>nsTreeMap</code> is initialized.
   */
  private static final Object lock = new Object();

  /**
   * ****************BEGIN: public methods in this class *****************************
   */
  /**
   * Initialize consistent hashing. Before calling any method in this class, <code>reInitialize</code> method must be
   * called.
   */
  public static void reInitialize(int numReplicaControllers, Set nameServerIDs) {
    if (numReplicaControllers > nameServerIDs.size()) {
      throw new IllegalArgumentException("ERROR: Number of replica controllers " + numReplicaControllers
              + " numNameServers = " + nameServerIDs.size());
    }
    synchronized (lock) {
      GNS.getLogger().info("Reinitializing consistent hashing with node Ids " +  Util.setOfNodeIdToString(nameServerIDs));
      ConsistentHashing.numReplicaControllers = numReplicaControllers;
      // Keys of treemap are hashes of ID of all name servers, values are IDs of name servers.
      nsTreeMap = new TreeMap<String, Object>();
      for (Object nodeID : nameServerIDs) {
        nsTreeMap.put(getMD5Hash(nodeID.toString()), nodeID);
      }
    }
  }

  /**
   * Returns the set of replica controllers for a name.
   * The set of replica controllers is determined using consistent hashing.
   *
   * @param name Name
   * @return a set with nodeIDs of replica controllers
   */
  public static Set<Object> getReplicaControllerSet(String name) {
    if (name == null || name.trim().equals("")) {
      return null;
    }
    return getPrimaryReplicasNoCache(name);
  }

  /**
   * Returns the group ID of replica controllers for this name.
   */
  public static String getReplicaControllerGroupID(String name) {

    String nameHash = getMD5Hash(name);
    String key = nsTreeMap.higherKey(nameHash);
    if (key == null) {
      return nsTreeMap.firstKey();
    }
    return key;
  }

  /**
   * Returns a list of replica controller groups which includes the given node.
   * We return a hash map whose keys are groupIDs of replica controller groups, and
   * values are list of members in that group.
   */
  public static HashMap<String, Set> getReplicaControllerGroupIDsForNode(Object nodeID) {

    HashMap<String, Set> groupIDsAndMembers = new HashMap<String, Set>();

    ArrayList<Object> nodesSorted = new ArrayList<Object>();
    for (String s1 : nsTreeMap.keySet()) {
      nodesSorted.add(nsTreeMap.get(s1));
    }

    for (int i = 0; i < nsTreeMap.size(); i++) {
      int paxosMemberIndex = i;
      String paxosID = getMD5Hash(nodesSorted.get(paxosMemberIndex).toString());
      HashSet<Object> nodes = new HashSet<Object>();
      boolean hasNode = false;
      while (nodes.size() < numReplicaControllers) {
        if (nodesSorted.get(paxosMemberIndex).equals(nodeID)) {
          hasNode = true;
        }
        nodes.add(nodesSorted.get(paxosMemberIndex));
        paxosMemberIndex += 1;
        if (paxosMemberIndex == nsTreeMap.size()) {
          paxosMemberIndex = 0;
        }
      }
      if (hasNode) {
        groupIDsAndMembers.put(paxosID, nodes);
      }
    }
    return groupIDsAndMembers;
  }

  public static String getConsistentHash(String name) {
    return getMD5Hash(name);
  }

  /**
   * Returns MD5 hash of given string. This is the hash function used for determining
   * replica controllers of a name.
   */
  private static String getMD5Hash(String name) {
    MessageDigest md = null;
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
      return null;
    }
    md.update(name.getBytes());
    String s = ByteUtils.toHex(md.digest());
    return s;
  }

  /**
   * Primary replicas for a name are obtained by consistent hashing.
   * First, all nodes are mapped on to key space by hashing. Then, the name is hashed on to the key space.
   * if 'k' primary replicas are to be selected, they are the higher
   */
  private static Set<Object> getPrimaryReplicasNoCache(String name) {
    String hashedName = getMD5Hash(name);
    HashSet<Object> result = new HashSet<Object>();
    while (true) {
      Map.Entry<String, Object> entry = nsTreeMap.higherEntry(hashedName);
      if (entry == null) {
        break;
      }
      result.add(entry.getValue());
      hashedName = (String) entry.getKey();
      if (result.size() == numReplicaControllers) {
        return result;
      }
    }

    Map.Entry<String, Object> entry = nsTreeMap.firstEntry();
    result.add(entry.getValue());
    hashedName = (String) entry.getKey();
    while (result.size() != numReplicaControllers) {
      entry = nsTreeMap.higherEntry(hashedName);
      result.add(entry.getValue());
      hashedName = (String) entry.getKey();
    }
    return result;
  }

  /**
   * Test *
   */
  public static void main(String[] args) {
    //try {
      int nameServers = 10;
      int numPrimaryReplicas = 3;
      int names = 10;
      Set<Object> nameServerIDs = new HashSet<Object>();
      for (int i = 0; i < nameServers; i++) {
        nameServerIDs.add(Util.randomString(4));
      }
      ConsistentHashing.reInitialize(numPrimaryReplicas, nameServerIDs);

      for (int i = 0; i < names; i++) {
        String name = Util.randomString(6);
        Set<Object> primaryNameServers = getPrimaryReplicasNoCache(name);
        System.out.println("Name: " + name + "\tPrimaries: " + primaryNameServers + "\tGroupID:"
                + getReplicaControllerGroupID(name) + "\t");
      }
      for (Object node :  nameServerIDs) {
        System.out.println("Groups for node: " + "\t" + node.toString() + "\t" + getReplicaControllerGroupIDsForNode(node));
      }

      int avg = names * numPrimaryReplicas / nameServers;
      System.out.println("Avg: " + avg);

      String name = "Frank";
      Set<Object> replicas = getReplicaControllerSet(name);
      System.out.println(name + ": " + replicas.toString());

      String s = "B04A8EBC86BC3BFDD1EF88377D55B8304088821C";
      replicas = getReplicaControllerSet(s);
      System.out.println("Primary replicas for name  " + s + ":" + replicas);

  }
}
