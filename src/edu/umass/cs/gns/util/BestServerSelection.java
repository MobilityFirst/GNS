package edu.umass.cs.gns.util;

import edu.umass.cs.gns.localnameserver.CacheEntry;
import edu.umass.cs.gns.localnameserver.LocalNameServer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nameserver.NameServer;

import java.util.*;

/**
 * @deprecated
 */
public class BestServerSelection {

  /**
   * ************************************************************
   * Returns closest server including ping-latency and server-load.
   *
   * @return Best name server among serverIDs given.
   * ***********************************************************
   */
  public static int simpleLatencyLoadHeuristic(Set<Integer> serverIDs) {

    if (serverIDs.size() == 0) {
      return -1;
    }

    int selectServer = -1;
    // select server whose latency + load is minimum
    double selectServerLatency = Double.MAX_VALUE;
    for (int x : serverIDs) {
      if (ConfigFileInfo.getPingLatency(x) > 0) {
        double totallatency = 5 * LocalNameServer.getNameServerLoads().get(x) + (double) ConfigFileInfo.getPingLatency(x);
        if (totallatency < selectServerLatency) {
          selectServer = x;
          selectServerLatency = totallatency;
        }
      }
    }
    return selectServer;

  }


  /**
   **
   * select best name server considering ping-latency + server-load including actives and primaries
   *
   *
   * @param name Host/Domain/Device Name
   * @param nameserverQueried A set of name servers queried and excluded.
   * @return id of best name server, -1 if none found.
   */
  public static int getBestActiveNameServerFromCache(CacheEntry cacheEntry,
                                                     Set<Integer> nameserverQueried) {

    if (cacheEntry == null || cacheEntry.getActiveNameServers() == null) {
      return -1;
    }

    HashSet<Integer> allServers = new HashSet<Integer>();
    if (cacheEntry.getActiveNameServers() != null) {
      for (int x : cacheEntry.getActiveNameServers()) {
        if (!allServers.contains(x) && nameserverQueried != null && !nameserverQueried.contains(x)) {
          allServers.add(x);
        }
      }
    }


    return BestServerSelection.simpleLatencyLoadHeuristic(allServers);
  }





  private static int beehiveNSChoose(int closestNS, ArrayList<Integer> nameServers, Set<Integer> nameServersQueried) {

    if (nameServers.contains(closestNS) && (nameServersQueried == null || !nameServersQueried.contains(closestNS))) {
      return closestNS;
    }

    Collections.sort(nameServers);
    for (int x : nameServers) {
      if (x > closestNS && (nameServersQueried == null || !nameServersQueried.contains(x))) {
        return x;
      }
    }

    for (int x : nameServers) {
      if (x < closestNS && (nameServersQueried == null || !nameServersQueried.contains(x))) {
        return x;
      }
    }

    return -1;
  }

  public static int getBeehiveNameServer(Set<Integer> nameserverQueried, CacheEntry cacheEntry) {
    ArrayList<Integer> allServers = new ArrayList<Integer>();
    if (cacheEntry.getActiveNameServers() != null) {
      for (int x : cacheEntry.getActiveNameServers()) {
        if (!allServers.contains(x) && nameserverQueried != null && !nameserverQueried.contains(x)) {
          allServers.add(x);
        }
      }
    }

//    for (int x : cacheEntry.getPrimaryNameServer()) {
//      if (!allServers.contains(x) && nameserverQueried != null && !nameserverQueried.contains(x)) {
//        allServers.add(x);
//      }
//    }
    if (allServers.size() == 0) {
      return -1;
    }

    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("BEEHIVE All Name Servers: " + allServers);
    }
    if (allServers.contains(ConfigFileInfo.getClosestNameServer())) {
      return ConfigFileInfo.getClosestNameServer();
    }
    return beehiveNSChoose(ConfigFileInfo.getClosestNameServer(), allServers, nameserverQueried);
//    return allServers.get(r.nextInt(allServers.size()));

    //		int x = beehiveDHTRouting.getDestNS(new Integer(name),
    //				ConfigFileInfo.getClosestNameServer(), allServers);
    //		if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("BEEHIVE Chosen Name Server: " + x);
    //		return x;
  }

  /**
   * ************************************************************
   * Returns closest server including ping-latency and server-load.
   *
   * @return Best name server among serverIDs given.
   * ***********************************************************
   */
  public static int thresholdHeuristic(Set<Integer> serverIDs) {

    if (serverIDs.size() == 0) {
      return -1;
    }
    int selectServer = -1;

    // select closest server: whose latency is below Threshold.
    long selectServerLatency = Long.MAX_VALUE;
    for (int x : serverIDs) {
//			  GNRS.getLogger().fine("Consider server " + x 
//					  + " Ping latency : " + ConfigFileInfo.getPingLatency(x)
//					  + " Name server load: " + nameServerLoads.get(x));
      if (ConfigFileInfo.getPingLatency(x) > 0
              && LocalNameServer.getNameServerLoads().containsKey(x)
              && LocalNameServer.getNameServerLoads().get(x) < StartLocalNameServer.serverLoadThreshold
              && ConfigFileInfo.getPingLatency(x) < selectServerLatency) {
//				  GNRS.getLogger().fine("Considered server " + x);
        selectServer = x;
        selectServerLatency = ConfigFileInfo.getPingLatency(x);
      }
    }
//		  GNRS.getLogger().fine("Select server " + selectServer + " latency: " + selectServerLatency);
    if (selectServer != -1) {
      return selectServer;
    }

    // All servers are loaded, choose least loaded server.
    double leastLoad = Double.MAX_VALUE;
    for (int x : serverIDs) {
      if (ConfigFileInfo.getPingLatency(x) > 0
              && LocalNameServer.getNameServerLoads().containsKey(x)
              && LocalNameServer.getNameServerLoads().get(x) < leastLoad) {
        selectServer = x;
        leastLoad = LocalNameServer.getNameServerLoads().get(x);
      }
    }
    return selectServer;

  }

  public static int getSmallestLatencyNS2(Set<Integer> activeNameServer, Set<Integer> primaryNameServer, Set<Integer> nameserverQueried) {
    if (activeNameServer == null || primaryNameServer == null) {
      return -1;
    }
    if ((activeNameServer.contains(ConfigFileInfo.getClosestNameServer()) || primaryNameServer.contains(ConfigFileInfo.getClosestNameServer())) && nameserverQueried != null && !nameserverQueried.contains(ConfigFileInfo.getClosestNameServer()) && ConfigFileInfo.getPingLatency(ConfigFileInfo.getClosestNameServer()) >= 0) {
      return ConfigFileInfo.getClosestNameServer();
    }
    long lowestLatency = Long.MAX_VALUE;
    int nameServerID = -1;
    long pingLatency = ConfigFileInfo.INVALID_PING_LATENCY;
    for (Integer nsID : activeNameServer) {
      if (nameserverQueried != null && nameserverQueried.contains(nsID)) {
        continue;
      }
      pingLatency = ConfigFileInfo.getPingLatency(nsID);
      if (pingLatency >= 0 && pingLatency < lowestLatency) {
        lowestLatency = pingLatency;
        nameServerID = nsID;
      }
    }
    for (Integer nsID : primaryNameServer) {
      if (nameserverQueried != null && nameserverQueried.contains(nsID)) {
        continue;
      }
      pingLatency = ConfigFileInfo.getPingLatency(nsID);
      if (pingLatency >= 0 && pingLatency < lowestLatency) {
        lowestLatency = pingLatency;
        nameServerID = nsID;
      }
    }
    return nameServerID;
  }

  /********
   * Returns a name server id with the smallest network latency among the set of name servers. If the name server set is empty or
   * all the name servers have already been queried then -1 is returned. Returns -1 of the name servers Set is null
   *
   * @param nameServers A set of name servers
   * @param nameserverQueried A set of name servers already queried
   * @return Name server id with the smallest latency or -1.****
   */
  public static int getSmallestLatencyNS(Set<Integer> nameServers, Set<Integer> nameserverQueried) {
    if (nameServers == null) {
      return -1;
    }

    if (nameServers.contains(ConfigFileInfo.getClosestNameServer())
            && nameserverQueried != null && !nameserverQueried.contains(ConfigFileInfo.getClosestNameServer())
            && ConfigFileInfo.getPingLatency(ConfigFileInfo.getClosestNameServer()) >= 0) {
      return ConfigFileInfo.getClosestNameServer();
    }
    long lowestLatency = Long.MAX_VALUE;
    int nameServerID = -1;
    long pingLatency;
    for (Integer nsID : nameServers) {
      if (nameserverQueried != null && nameserverQueried.contains(nsID)) {
        continue;
      }
      pingLatency = ConfigFileInfo.getPingLatency(nsID);
      if (pingLatency >= 0 && pingLatency < lowestLatency) {
        lowestLatency = pingLatency;
        nameServerID = nsID;
      }
    }
    return nameServerID;
  }

  /********
   * Returns a name server id with the smallest network latency among the set of name servers. If the name server set is empty or
   * all the name servers have already been queried then -1 is returned. Returns -1 of the name servers Set is null
   *
   * @param nameServers A set of name servers
   * @param nameServersQueried A set of name servers already queried
   * @return Name server id with the smallest latency or -1.****
   */
  public static int getSmallestLatencyNSNotFailed(Set<Integer> nameServers, Set<Integer> nameServersQueried) {
    if (nameServers == null) {
      return -1;
    }

    if (nameServers.contains(ConfigFileInfo.getClosestNameServer())
            && nameServersQueried != null && !nameServersQueried.contains(ConfigFileInfo.getClosestNameServer())
            && ConfigFileInfo.getPingLatency(ConfigFileInfo.getClosestNameServer()) >= 0
            && NameServer.getPaxosManager().isNodeUp(ConfigFileInfo.getClosestNameServer())) {
      return ConfigFileInfo.getClosestNameServer();
    }
    long lowestLatency = Long.MAX_VALUE;
    int nameServerID = -1;
    long pingLatency;
    for (Integer nsID : nameServers) {
      if (nameServersQueried != null && nameServersQueried.contains(nsID) || NameServer.getPaxosManager().isNodeUp(nsID) == false) {
        continue;
      }
      pingLatency = ConfigFileInfo.getPingLatency(nsID);
      if (pingLatency >= 0 && pingLatency < lowestLatency) {
        lowestLatency = pingLatency;
        nameServerID = nsID;
      }
    }
    return nameServerID;
  }


  private static Random random = new Random();

  public static int randomServer(Set<Integer> serverIDs) {

    int index = random.nextInt(serverIDs.size());
    int i = 0;
    for (int id : serverIDs) {
      if (i == index) {
        return id;
      }
      i = i + 1;
    }
    return -1;

  }
}
