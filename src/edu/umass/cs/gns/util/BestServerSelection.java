package edu.umass.cs.gns.util;

import java.util.Set;

import edu.umass.cs.gns.localnameserver.LocalNameServer;
import edu.umass.cs.gns.main.StartLocalNameServer;

public class BestServerSelection
{
	/**
	   * ************************************************************
	   * Returns closest server including ping-latency and server-load.
	   * 
	   * @return Best name server among serverIDs given.
	   * ***********************************************************
	   */
	  public static int simpleLatencyLoadHeuristic(Set<Integer> serverIDs) {
		  
		  if (serverIDs.size() == 0) return -1;
		  
		  int selectServer = -1;
		  // select server whose latency + load is minimum
		  double selectServerLatency = Double.MAX_VALUE;
		  for (int x: serverIDs) {
			  if (ConfigFileInfo.getPingLatency(x) > 0) {
				  double totallatency = LocalNameServer.nameServerLoads.get(x) + 
						  ConfigFileInfo.getPingLatency(x);
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
		  
		  if (serverIDs.size() == 0) return -1;
		  int selectServer = -1;
		  
		  // select closest server: whose latency is below Threshold.
		  double selectServerLatency = Double.MAX_VALUE;
		  for (int x: serverIDs) {
//			  GNRS.getLogger().fine("Consider server " + x 
//					  + " Ping latency : " + ConfigFileInfo.getPingLatency(x)
//					  + " Name server load: " + nameServerLoads.get(x));
			  if (ConfigFileInfo.getPingLatency(x) > 0 && 
					  LocalNameServer.nameServerLoads.containsKey(x) && 
					  LocalNameServer.nameServerLoads.get(x) < StartLocalNameServer.serverLoadThreshold &&  
					  ConfigFileInfo.getPingLatency(x) < selectServerLatency) {
//				  GNRS.getLogger().fine("Considered server " + x);
				  selectServer = x;
				  selectServerLatency = ConfigFileInfo.getPingLatency(x);
			  }
		  }
//		  GNRS.getLogger().fine("Select server " + selectServer + " latency: " + selectServerLatency);
		  if (selectServer != -1) return selectServer;
		  
		  // All servers are loaded, choose least loaded server.
		  double leastLoad = Double.MAX_VALUE;
		  for (int x: serverIDs) {
			  if (ConfigFileInfo.getPingLatency(x) > 0 && 
					  LocalNameServer.nameServerLoads.containsKey(x) && 
					  LocalNameServer.nameServerLoads.get(x) < leastLoad) {
				  selectServer = x;
				  leastLoad = LocalNameServer.nameServerLoads.get(x);
			  }
		  }
		  return selectServer;
		  
	  }

}
