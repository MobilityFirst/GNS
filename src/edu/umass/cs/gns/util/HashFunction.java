package edu.umass.cs.gns.util;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.NameServer;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/*************************************************************
 * This class contains the hash function used to find primary
 * replicas (name servers) of a host/domain name.
 *
 * @author Hardeep Uppal
 ************************************************************/
public class HashFunction {

  /** List of hash funtions used to compute the list of primary nameserver **/
//  private static List<MessageDigest> hashFunctions;
  /** Cache hash results **/
  private static Cache<String, Set<Integer>> cache;

  /**
   * Keys of treemap are hashes of ID of all name servers, values are IDs of name servers.
   */
  public static TreeMap<String, Integer> nsTreeMap = new TreeMap<String, Integer>();

  /*************************************************************
   * Initialize hash funtions
   * @throws NoSuchAlgorithmException
   ************************************************************/
  public static void initializeHashFunction(){
//    hashFunctions = new ArrayList<MessageDigest>();
//    hashFunctions.add(MessageDigest.getInstance("MD5"));
//    hashFunctions.add(MessageDigest.getInstance("SHA-1"));
//    hashFunctions.add(MessageDigest.getInstance("SHA-256"));
//    hashFunctions.add(MessageDigest.getInstance("SHA-512"));

    cache = CacheBuilder.newBuilder().concurrencyLevel(2).maximumSize(10000).build();
    // Keys of treemap are hashes of ID of all name servers, values are IDs of name servers.
    nsTreeMap = new TreeMap<String, Integer>();
    for (int i = 0; i < ConfigFileInfo.getNumberOfNameServers(); i++) {
      nsTreeMap.put(getMD5Hash(Integer.toString(i)), i);
    }
    System.out.println(nsTreeMap);
  }



  public static String getMD5Hash(String name) {
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


//  static HashSet<Integer> getPrimaryNameServers(String name){

//  }

  /*************************************************************
   * Computes the SHA of a string
   * @param text string
   * @param md Hashing algorithm
   * @return Hash of a string as byte array
   * @throws NoSuchAlgorithmException
   * @throws UnsupportedEncodingException
   ************************************************************/
  public static byte[] SHA(String text, MessageDigest md)
          throws NoSuchAlgorithmException, UnsupportedEncodingException {
    md.update(text.getBytes(), 0, text.length());
    return md.digest();
  }

  /*************************************************************
   * Computes the hash of a name and returns a set of primary
   * replicas (name server) ids for the name.
   * @param name Name
   * @return a set of primary replicas (name server) ids
   ************************************************************/
  public static Set<Integer> getPrimaryReplicas(String name) {

    if (name == null || name.trim().equals("")) {
      return null;
    }

    Set<Integer> primaryReplicas;
    if ((primaryReplicas = cache.getIfPresent(name)) != null) {
//    	GNRS.getLogger().fine("Return from Hash function cache: Name = " + name + " Primaries = " + primaryReplicas);
      return primaryReplicas;
    }

    primaryReplicas = getPrimaryReplicasNoCache(name);
//    GNRS.getLogger().fine("Compute primaries and return: Name = " + name + " Primaries = " + primaryReplicas);
    cache.put(name, primaryReplicas);
    return primaryReplicas;
  }

  /**
   * Primary replicas for a name are obtained by consistent hashing.
   * First, all nodes are mapped on to key space by hashing. Then, the name is hashed on to the key space.
   * if 'k' primary replicas are to be selected, they are the higher
   * @param name
   * @return
   */
  public static Set<Integer> getPrimaryReplicasNoCache(String name) {
    String s = getMD5Hash(name);
    HashSet<Integer> primaryNameServers = new HashSet<Integer>();
    while(true) {
      Map.Entry entry = nsTreeMap.higherEntry(s);
      if (entry == null) break;
      primaryNameServers.add((Integer) entry.getValue());
      s = (String) entry.getKey();
//        System.out.println("x\t" + entry.getValue());
      if (primaryNameServers.size() == GNS.numPrimaryReplicas) return primaryNameServers;
    }

    Map.Entry entry = nsTreeMap.firstEntry();
    primaryNameServers.add((Integer) entry.getValue());
//      System.out.println("y\t" + entry.getValue());
    s = (String) entry.getKey();
    while(primaryNameServers.size() != GNS.numPrimaryReplicas) {
      entry = nsTreeMap.higherEntry(s);
      primaryNameServers.add((Integer) entry.getValue());
      s = (String) entry.getKey();
//        System.out.println("z\t" + entry.getValue());
    }
    return primaryNameServers;

//    return getPrimaryNameServers(name);
  }

  public static Set<Integer> getPrimaryReplicasNoCache2(String name) {
    Set<Integer> primaryReplicas = new HashSet<Integer>();
//      primaryReplicas.add(0);
//      primaryReplicas.add(1);
//      primaryReplicas.add(2);
//      return primaryReplicas;
//      GNS.getLogger().fine("here 2.2.1");
    List<MessageDigest> hashFunctions = new ArrayList<MessageDigest>();
    try
    {
      hashFunctions.add(MessageDigest.getInstance("MD5"));
      hashFunctions.add(MessageDigest.getInstance("SHA-1"));
      hashFunctions.add(MessageDigest.getInstance("SHA-256"));
      hashFunctions.add(MessageDigest.getInstance("SHA-512"));

      for (int i = 0; i < GNS.numPrimaryReplicas; i++) {
        byte[] buff = SHA(name, hashFunctions.get(i%hashFunctions.size()));
        int id = ByteUtils.ByteArrayToInt(buff);
        id = (id < 0) ? id * -1 : id;
        Integer nameServerID = new Integer(id % ConfigFileInfo.getNumberOfNameServers());

        // westy added this so we actually get numPrimaryReplicas different numbers
        while (primaryReplicas.contains(nameServerID)) {
          nameServerID = (nameServerID + 1) % ConfigFileInfo.getNumberOfNameServers();
        }

        primaryReplicas.add(nameServerID);
      }
      GNS.getLogger().finer(" Primary replica: " + primaryReplicas);
    } catch (NoSuchAlgorithmException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (UnsupportedEncodingException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
//      GNS.getLogger().fine("here 2.2.2");
    return primaryReplicas;
  }

  /** Test **/
  public static void main(String[] args) {

    try {
      int nameServers = 5;
      int names = 0;
      ConfigFileInfo.setNumberOfNameServers(nameServers);
      HashFunction.initializeHashFunction();
      for (int i = 0; i < nameServers; i++) {
        NameServer.nodeID = i;
        System.out.println( "Set nodeID = " + i);
        NameServer.createPrimaryPaxosInstances();
      }
      HashMap<Integer, Integer> nodeNameCount = new HashMap<Integer, Integer>();
      for (int i = 0; i < names; i++) {
        String name = Integer.toString(i);
        Set<Integer> primaryNameServers = getPrimaryReplicasNoCache(name);
        for (int primary: primaryNameServers){
          if (nodeNameCount.containsKey(primary)) nodeNameCount.put(primary, nodeNameCount.get(primary) + 1);
          else nodeNameCount.put(primary, 1);
        }
//        System.out.println("Name: " + name + "\tPrimaries: " + primaryNameServers);
      }

      ArrayList<Integer> nodeCount = new ArrayList<Integer>();
      int avg = names * GNS.numPrimaryReplicas / nameServers;
      System.out.println("Avg: " + avg);
      for (int x: nodeNameCount.values()) {
        nodeCount.add(x);
      }
      Collections.sort(nodeCount);
      System.out.println(nodeCount);
      System.exit(2);

      HashFunction.initializeHashFunction();

      ConfigFileInfo.setNumberOfNameServers(3);
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

//      ConfigFileInfo.readHostInfo("ns1", 1);
      GNS.numPrimaryReplicas = 3;//GNRS.DEFAULTNUMPRIMARYREPLICAS;

      String name = "13";
      Set<Integer> replicas = getPrimaryReplicas(name);
      System.out.println(name + ": " + replicas.toString());

      replicas = getPrimaryReplicas("B04A8EBC86BC3BFDD1EF88377D55B8304088821C");
      System.out.println(replicas.toString());

      replicas = getPrimaryReplicas("adsf");
      System.out.println(replicas);
      s = "B04A8EBC86BC3BFDD1EF88377D55B8304088821C";
      replicas = getPrimaryReplicas(s);
      System.out.println("Primary replicas for name  "+ s + ":" + replicas);
//      System.out.println("Number of replicas:" + replicas.size());
//      replicas = getPrimaryReplicas("408");
//      System.out.println("Number of replicas:" + replicas.size());
//      System.out.println("CHECHECHEKHCKEHCLKEHCLKJE: " + replicas);
//      replicas = getPrimaryReplicas("408");
//      System.out.println(replicas);
//      System.out.println("Number of replicas:" + replicas.size());
//      replicas = getPrimaryReplicas("www.facebook.com");
//      System.out.println(replicas);
//      System.out.println("Number of replicas:" + replicas.size());
//      replicas = getPrimaryReplicas("www.google.com");
//      System.out.println(replicas.toString());
//      System.out.println("Number of replicas:" + replicas.size());
//      System.out.println("cache size:" + cache.size());
//      System.out.println(cache.stats().toString());
//      System.out.println(cache.asMap());

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
