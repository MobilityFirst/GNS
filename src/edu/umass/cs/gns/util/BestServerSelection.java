package edu.umass.cs.gns.util;

import edu.umass.cs.gns.localnameserver.original.LocalNameServer;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nameserver.NameServer;

import java.util.Random;
import java.util.Set;

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
        double totallatency = 5 * LocalNameServer.nameServerLoads.get(x) + ConfigFileInfo.getPingLatency(x);
        if (totallatency < selectServerLatency) {
          selectServer = x;
          selectServerLatency = totallatency;
        }
      }
    }
    return selectServer;

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
    double selectServerLatency = Double.MAX_VALUE;
    for (int x : serverIDs) {
//			  GNRS.getLogger().fine("Consider server " + x 
//					  + " Ping latency : " + ConfigFileInfo.getPingLatency(x)
//					  + " Name server load: " + nameServerLoads.get(x));
      if (ConfigFileInfo.getPingLatency(x) > 0
              && LocalNameServer.nameServerLoads.containsKey(x)
              && LocalNameServer.nameServerLoads.get(x) < StartLocalNameServer.serverLoadThreshold
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
              && LocalNameServer.nameServerLoads.containsKey(x)
              && LocalNameServer.nameServerLoads.get(x) < leastLoad) {
        selectServer = x;
        leastLoad = LocalNameServer.nameServerLoads.get(x);
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
    double lowestLatency = Double.MAX_VALUE;
    int nameServerID = -1;
    double pingLatency = -1;
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
    double lowestLatency = Double.MAX_VALUE;
    int nameServerID = -1;
    double pingLatency;
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
            && NameServer.paxosManager.isNodeUp(ConfigFileInfo.getClosestNameServer())) {
      return ConfigFileInfo.getClosestNameServer();
    }
    double lowestLatency = Double.MAX_VALUE;
    int nameServerID = -1;
    double pingLatency;
    for (Integer nsID : nameServers) {
      if (nameServersQueried != null && nameServersQueried.contains(nsID) || NameServer.paxosManager.isNodeUp(nsID) == false) {
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
