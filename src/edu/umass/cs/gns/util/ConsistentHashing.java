package edu.umass.cs.gns.util;


import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;


/**
 * This class contains the hash function used to find replica controllers (primary replicas)
 * of a name. The set of replica controllers is determined using consistent hashing.
 *
 * Before calling any method in this class, <code>initialize</code> method must be called. If <code>initialize</code>
 * method is called multiple times, the initialization is done based on the first call; later calls make no difference.
 *
 * The class is thread-safe.
 *
 * @author  Abhigyan
 */
public class ConsistentHashing {


  private static int numReplicaControllers;


  /**
   * A treemap whose keys are hashes of ID of all name servers, and values are IDs of name servers.
   */
  private static TreeMap<String, Integer> nsTreeMap;

  /**
   * Lock object for checking if <code>nsTreeMap</code> is initialized.
   */
  private static final Object lock = new Object();

  /******************BEGIN: public methods in this class ******************************/

  /**
   * Initialize consistent hashing. Before calling any method in this class, <code>initialize</code> method must be
   * called. If <code>initialize</code> method is called multiple times, the initialization is done based on the first
   * call; later calls make no difference.
   */
  public static void initialize(int numReplicaControllers, Set<Integer> nameServerIDs){
    if (numReplicaControllers > nameServerIDs.size()) {
      throw  new IllegalArgumentException("ERROR: Number of replica controllers " + numReplicaControllers +
              " numNameServers = " + nameServerIDs.size());
    }
    synchronized (lock) {   // lock so that we do not initialize nsTreeMap multiple times.
      if (nsTreeMap != null) return;
      ConsistentHashing.numReplicaControllers = numReplicaControllers;
      nsTreeMap = new TreeMap<String, Integer>();
      // Keys of treemap are hashes of ID of all name servers, values are IDs of name servers.
      nsTreeMap = new TreeMap<String, Integer>();
      for (int nodeID: nameServerIDs) {
        nsTreeMap.put(getMD5Hash(Integer.toString(nodeID)), nodeID);
      }
    }
  }

  /**
   * Returns the set of replica controllers for a name.
   * The set of replica controllers is determined using consistent hashing.
   * @param name Name
   * @return a set with nodeIDs of replica controllers
   */
  public static Set<Integer> getReplicaControllerSet(String name) {
    if (name == null || name.trim().equals("")) {
      return null;
    }

    Set<Integer> primaryReplicas;

    primaryReplicas = getPrimaryReplicasNoCache(name);
//    GNRS.getLogger().fine("Compute primaries and return: Name = " + name + " Primaries = " + primaryReplicas);
    return primaryReplicas;
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
   * values are list of members in that group. One of these members is the <code>nodeID</code>
   */
  public static HashMap<String, Set<Integer>> getReplicaControllerGroupIDsForNode(int nodeID) {

    HashMap<String, Set<Integer>> groupIDsAndMembers = new HashMap<String, Set<Integer>>();

    ArrayList<Integer> nodesSorted = new ArrayList<Integer>();
    for (String s1 : nsTreeMap.keySet()) {
      nodesSorted.add(nsTreeMap.get(s1));
    }

    for (int i = 0; i < nsTreeMap.size(); i++) {
      int paxosMemberIndex = i;
      String paxosID = getMD5Hash(Integer.toString(nodesSorted.get(paxosMemberIndex)));
      HashSet<Integer> nodes = new HashSet<Integer>();
      boolean hasNode = false;
      while (nodes.size() < numReplicaControllers) {
        if (nodesSorted.get(paxosMemberIndex) == nodeID) {
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

  /******************END: public methods in this class ******************************/



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
  private static Set<Integer> getPrimaryReplicasNoCache(String name) {
    String s = getMD5Hash(name);
    HashSet<Integer> primaryNameServers = new HashSet<Integer>();
    while(true) {
      Map.Entry entry = nsTreeMap.higherEntry(s);
      if (entry == null) break;
      primaryNameServers.add((Integer) entry.getValue());
      s = (String) entry.getKey();
//        System.out.println("x\t" + entry.getValue());
      if (primaryNameServers.size() == numReplicaControllers) return primaryNameServers;
    }

    Map.Entry entry = nsTreeMap.firstEntry();
    primaryNameServers.add((Integer) entry.getValue());
//      System.out.println("y\t" + entry.getValue());
    s = (String) entry.getKey();
    while(primaryNameServers.size() != numReplicaControllers) {
      entry = nsTreeMap.higherEntry(s);
      primaryNameServers.add((Integer) entry.getValue());
      s = (String) entry.getKey();
//        System.out.println("z\t" + entry.getValue());
    }
    return primaryNameServers;
  }



  /**
   * Computes the SHA of a string
   * @param text string
   * @param md Hashing algorithm
   * @return Hash of a string as byte array
   * @throws NoSuchAlgorithmException
   * @throws UnsupportedEncodingException
   */
  private static byte[] SHA(String text, MessageDigest md)
          throws NoSuchAlgorithmException, UnsupportedEncodingException {
    md.update(text.getBytes(), 0, text.length());
    return md.digest();
  }


  /** Test **/
  public static void main(String[] args) {

    try {
      int nameServers = 10;
      int numPrimaryReplicas = 3;
      int names = 10;
      Set<Integer> nameServerIDs = new HashSet<Integer>();
      for (int i = 0; i < nameServers; i++) {
        nameServerIDs.add(i);
      }
      ConsistentHashing.initialize(numPrimaryReplicas, nameServerIDs);

      HashMap<Integer, Integer> nodeNameCount = new HashMap<Integer, Integer>();
      for (int i = 0; i < names; i++) {
        String name = Integer.toString(i);
        Set<Integer> primaryNameServers = getPrimaryReplicasNoCache(name);
        System.out.println("Name: " + name + "\tPrimaries: " + primaryNameServers + "\tGroupID:" +
                getReplicaControllerGroupID(name) + "\t");
      }
      for (int i = 0; i < nameServers; i++) {
        System.out.println("Groups for node: "+ "\t" + i + "\t" + getReplicaControllerGroupIDsForNode(i));
      }

      System.exit(2);

      ArrayList<Integer> nodeCount = new ArrayList<Integer>();
      int avg = names * numPrimaryReplicas / nameServers;
      System.out.println("Avg: " + avg);
      for (int x: nodeNameCount.values()) {
        nodeCount.add(x);
      }
      Collections.sort(nodeCount);
      System.out.println(nodeCount);
      System.exit(2);


      System.out.println(ByteUtils.convertToHex(SHA("www.google.com", MessageDigest.getInstance("SHA-1"))));
      String s = new String(SHA("www.google.com", MessageDigest.getInstance("SHA-1")));
      System.out.println(s);
      byte[] t = s.getBytes();
      System.out.println(ByteUtils.convertToHex(t));


      System.out.println(SHA("www.google.com", MessageDigest.getInstance("SHA-1")).length);
      System.out.println(ByteUtils.convertToHex(SHA("www.google.com", MessageDigest.getInstance("SHA-256"))));
      System.out.println(SHA("www.google.com", MessageDigest.getInstance("SHA-256")).length);
      System.out.println(ByteUtils.convertToHex(SHA("www.google.com", MessageDigest.getInstance("SHA-512"))));
      System.out.println(SHA("www.google.com", MessageDigest.getInstance("SHA-512")).length);

      System.out.println("\n" + ByteUtils.ByteArrayToInt(SHA("www.google.com", MessageDigest.getInstance("SHA-1"))));
      System.out.println(ByteUtils.ByteArrayToInt(SHA("www.google.com", MessageDigest.getInstance("SHA-256"))));
      System.out.println(ByteUtils.ByteArrayToInt(SHA("www.google.com", MessageDigest.getInstance("SHA-512"))));


      String name = "13";
      Set<Integer> replicas = getReplicaControllerSet(name);
      System.out.println(name + ": " + replicas.toString());

      replicas = getReplicaControllerSet("B04A8EBC86BC3BFDD1EF88377D55B8304088821C");
      System.out.println(replicas.toString());

      replicas = getReplicaControllerSet("adsf");
      System.out.println(replicas);
      s = "B04A8EBC86BC3BFDD1EF88377D55B8304088821C";
      replicas = getReplicaControllerSet(s);
      System.out.println("Primary replicas for name  "+ s + ":" + replicas);

    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
//    catch (IOException e) {
//      e.printStackTrace();
//    }

  }
}
