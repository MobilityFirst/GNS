package edu.umass.cs.gns.util;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import edu.umass.cs.gns.main.GNS;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

  /*************************************************************
   * Initialize hash funtions
   * @throws NoSuchAlgorithmException
   ************************************************************/
  public static void initializeHashFunction() throws NoSuchAlgorithmException {
//    hashFunctions = new ArrayList<MessageDigest>();
//    hashFunctions.add(MessageDigest.getInstance("MD5"));
//    hashFunctions.add(MessageDigest.getInstance("SHA-1"));
//    hashFunctions.add(MessageDigest.getInstance("SHA-256"));
//    hashFunctions.add(MessageDigest.getInstance("SHA-512"));

    cache = CacheBuilder.newBuilder().concurrencyLevel(2).maximumSize(10000).build();
  }

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
  
  public static Set<Integer> getPrimaryReplicasNoCache(String name) {
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
		  int id = Util.ByteArrayToInt(buff);
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
      HashFunction.initializeHashFunction();

      ConfigFileInfo.setNumberOfNameServers(6);
      System.out.println(Util.convertToHex(SHA("www.google.com", MessageDigest.getInstance("SHA-1"))));
      String s = new String(SHA("www.google.com", MessageDigest.getInstance("SHA-1")));
      System.out.println(s);
      byte[] t = s.getBytes();
      System.out.println(Util.convertToHex(t));


      System.out.println(SHA("www.google.com", MessageDigest.getInstance("SHA-1")).length);
      System.out.println(Util.convertToHex(SHA("www.google.com", MessageDigest.getInstance("SHA-256"))));
      System.out.println(SHA("www.google.com", MessageDigest.getInstance("SHA-256")).length);
      System.out.println(Util.convertToHex(SHA("www.google.com", MessageDigest.getInstance("SHA-512"))));
      System.out.println(SHA("www.google.com", MessageDigest.getInstance("SHA-512")).length);

      System.out.println("\n" + Util.ByteArrayToInt(SHA("www.google.com", MessageDigest.getInstance("SHA-1"))));
      System.out.println(Util.ByteArrayToInt(SHA("www.google.com", MessageDigest.getInstance("SHA-256"))));
      System.out.println(Util.ByteArrayToInt(SHA("www.google.com", MessageDigest.getInstance("SHA-512"))));

//      ConfigFileInfo.readHostInfo("ns1", 1);
      GNS.numPrimaryReplicas = 3;//GNRS.DEFAULTNUMPRIMARYREPLICAS;
      
      Set<Integer> replicas = getPrimaryReplicas("0A7D28BDB37FFD25DBD9C956FAD675CA921B0F9C");
      System.out.println("0A7D28BDB37FFD25DBD9C956FAD675CA921B0F9C: " + replicas.toString());

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
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
