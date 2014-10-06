package edu.umass.cs.gns.nsdesign.replicationframework;

import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.util.Util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 *
 * This class does not implement {@link edu.umass.cs.gns.nsdesign.replicationframework.ReplicationFrameworkInterface}.
 *
 * To implement beehive, we first calculate the number of servers using this class and
 * then use RandomReplication to select replicas.
 *
 * This class assumes that names are positive integers represented as strings, and a name 'i' is the i-th most
 * popular name.
 *
 * Abhigyan: Keeping this code around here only because we may need to run some experiments with beehive replication.
 *
 * This was Hardeep's implementation of beehive DHT routing.
 * 
 * It assumes that Node Ids are Strings (because it uses compareTo).
 */
public class BeehiveReplication {

  private static int numNodes;
  private static double M;
  private static double base;

  private static Map<Integer, Double> replicationLevelMap = new HashMap<Integer, Double>();

  public static void generateReplicationLevel(int numNodes, double C, double M, double alpha, double base) {
    BeehiveReplication.numNodes = numNodes; //
    BeehiveReplication.M = M;
    BeehiveReplication.base = base;

    int kPrime = getKPrime(C, M, alpha, base);
    double xi = 0;
    int level = 0;

    while (xi < 1) {
      xi = xi(level, C, M, alpha, base, kPrime);
      if (xi > 1) {
        replicationLevelMap.put(level, 1.0);
      } else {
        replicationLevelMap.put(level, xi);
      }
      level++;
    }
  }

  public static int numActiveNameServers(String name) {
    double popularity = Integer.parseInt(name) / M;
    int level = 0;
    for (; level < replicationLevelMap.size(); level++) {
      if (popularity <= replicationLevelMap.get(level)) {
        break;
      }
    }

    int numActives = Util.roundToInt(numNodes / Math.pow(base, level));
    if (numActives < 1) {
      numActives = 1;
    }
//		System.out.println( name + "\t" + level + "\t" + numActives );
    return numActives;
  }

  private static double xi(int i, double C, double M, double alpha, double base, int kPrime) {
    double d_power = (1 - alpha) / alpha;
    double D = Math.pow(base, d_power);
    double CPrime = C * (1 - (1 / Math.pow(M, 1 - alpha)));

    double xi_num = (Math.pow(D, i) * (kPrime - CPrime));
    double xi_dem = 1;

    for (int j = 1; j <= (kPrime - 1); j++) {
      xi_dem += Math.pow(D, j);
    }

    double xi_power = 1 / (1 - alpha);
    return Math.pow((xi_num / xi_dem), xi_power);
  }

  private static int getKPrime(double C, double M, double alpha, double base) {
    double xkPrime = 0;
    int kPrime = 0;

    while (xkPrime < 1) {
      kPrime++;
      xkPrime = xi(kPrime - 1, C, M, alpha, base, kPrime);
//			System.out.println("k':" + kPrime + "\tx_" + (kPrime-1) + ":" + xkPrime );
    }

    return kPrime - 1;
  }

  /**
   * Abhigyan: Putting this code here because it is related to the beehive replication strategy.
   * Implements the algorithm that a local name server uses to select a name server when we experiment with beehive
   * replication. It chooses the closest name server if that name server is an active replica, otherwise picks a name
   * server whose ID is greater than current name server's ID. I think this approximates a replica that will be chosen
   * using DHT routing.
   */
  public static Object getBeehiveNameServer(GNSNodeConfig gnsNodeConfig, Set<String> activeNameServers, Set nameserverQueried) {
    ArrayList<String> allServers = new ArrayList<String>();
    if (activeNameServers != null) {
      for (String x : activeNameServers) {
        if (!allServers.contains(x) && nameserverQueried != null && !nameserverQueried.contains(x)) {
          allServers.add(x);
        }
      }
    }

    if (allServers.isEmpty()) {
      return GNSNodeConfig.INVALID_NAME_SERVER_ID;
    }

    if (allServers.contains((String) gnsNodeConfig.getClosestServer())) {
      return gnsNodeConfig.getClosestServer();
    }
    return beehiveNSChoose((String)gnsNodeConfig.getClosestServer(), allServers, nameserverQueried);

  }

  private static Object beehiveNSChoose(String closestNS, ArrayList<String> nameServers, Set<Object> nameServersQueried) {

    if (nameServers.contains(closestNS) && (nameServersQueried == null || !nameServersQueried.contains(closestNS))) {
      return closestNS;
    }
    Collections.sort(nameServers);
    for (String x : nameServers) {
      if (x.compareTo(closestNS) > 0 && (nameServersQueried == null || !nameServersQueried.contains(x))) {
        return x;
      }
    }
    for (String x : nameServers) {
      if (x.compareTo(closestNS) < 0 && (nameServersQueried == null || !nameServersQueried.contains(x))) {
        return x;
      }
    }
    return GNSNodeConfig.INVALID_NAME_SERVER_ID;
  }

  public static void main(String[] args) throws IOException {
    int nameServerCount = 97;
    int nameCount = 11000;
    double hopCount = 0.54;
    double zipfAlpha = 0.63;

//    BeehiveReplication.numNodes = nameServerCount;
    HashMap<Double, Integer> loadAuspiceReplicaCount = new HashMap<Double, Integer>();
    loadAuspiceReplicaCount.put(1.0, 213332);
//    loadAuspiceReplicaCount.put(2.0,144858);
//    loadAuspiceReplicaCount.put(3.0,121721);
//    loadAuspiceReplicaCount.put(4.0,110110);
//    loadAuspiceReplicaCount.put(5.0,102362);
//    loadAuspiceReplicaCount.put(6.0, 99000);
//    loadAuspiceReplicaCount.put(7.0, 99000);
//    loadAuspiceReplicaCount.put(8.0, 99000);
    for (Double load : loadAuspiceReplicaCount.keySet()) {

      int auspiceTotalReplicas = loadAuspiceReplicaCount.get(load);
      //		System.exit(2);
      double selectedHopCount = 2.0;

      int codonsTotalReplicas = 200000;
      for (double j = 0.3; j <= 2.0; j = j + 0.02) {
        hopCount = j;
        generateReplicationLevel(nameServerCount, hopCount, nameCount, 0.63, 16);
        //					System.out.println(replicationLevelMap.toString() );
        int sum = 0;
        for (int i = 1; i <= nameCount; i++) {
          sum += numActiveNameServers(Integer.toString(i));
        }
        System.out.println("sum = " + sum);
        if (sum < auspiceTotalReplicas) {
          break;
        }
        selectedHopCount = hopCount;
        codonsTotalReplicas = sum;
        System.out.println(j + "\t" + sum * 1.0);
      }
      System.out.println("Selected hop count\t" + selectedHopCount + "\tCodons total replicas\t" + codonsTotalReplicas);

      generateReplicationLevel(nameServerCount, selectedHopCount, nameCount, zipfAlpha, 16);
      //		System.out.println(replicationLevelMap.toString() );
      int NUM_RETRY = nameServerCount;
      FileWriter fw = new FileWriter(new File("nameActives-codons-load" + load.intValue()));
      //    System.out.println("ns count \t" + ConfigFileInfo.getNumberOfNameServers());

      for (int i = 0; i <= nameCount; i++) {

        int numReplica = numActiveNameServers(Integer.toString(i));

        Set<Integer> newActiveNameServerSet = new HashSet<Integer>();

        if (numReplica == numNodes) {
          for (int j = 0; j < numNodes; j++) {
            newActiveNameServerSet.add(j);
          }
        } else {
          for (int j = 1; j <= numReplica; j++) {
            Random random = new Random(i);
            boolean added;
            int numTries = 0;
            do {
              numTries += 1;
              int newActiveNameServerId = random.nextInt(numNodes);
              added = newActiveNameServerSet.add(newActiveNameServerId);
            } while (!added && numTries < NUM_RETRY);
          }
        }

        fw.write(i + " ");
        for (int ns : newActiveNameServerSet) {
          fw.write(" " + ns);
        }
        fw.write("\n");
        //      System.out.println(i + "\t" + numReplica + "\t" + newActiveNameServerSet.size());
      }
      fw.close();

    }

  }

}
