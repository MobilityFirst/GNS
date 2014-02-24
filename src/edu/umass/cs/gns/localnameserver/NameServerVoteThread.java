package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.packet.NameServerSelectionPacket;
import edu.umass.cs.gns.util.BestServerSelection;
import edu.umass.cs.gns.util.ConfigFileInfo;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

/**************************************************************
 * This class implements the thread that periodically multicast 
 * nameserver selection votes to primary nameservers of a name.
 * <br/>
 * The votes are collected at the primary nameservers and used
 * to select active nameservers with the highest votes during 
 * replication period. 
 * 
 * @author Hardeep Uppal
 *************************************************************/
public class NameServerVoteThread extends Thread {


	/** Time interval in ms between transmitting votes **/
	private long voteIntervalMillis;
	/** Start interval **/
	private long startInterval;
	
	
	public static ConcurrentHashMap<Integer,Integer> unackedVotes = new ConcurrentHashMap<Integer,Integer>();
	
	Random r = new Random();
	/**************************************************************
	 * Constructs a new NameServerVoteThread that periodically 
	 * multicast nameserver selection votes to primary nameservers 
	 * of a name.
	 * @param voteIntervalMillis Time interval in ms between transmitting
	 * votes
	 *************************************************************/
	public NameServerVoteThread(long voteIntervalMillis) {
		super("NameServerVoteThread");
		this.voteIntervalMillis = voteIntervalMillis;

	}

	/*************************************************************
	 * Starts executing this thread.
	 ************************************************************/
	@Override
	public void run() {
		long interval;
		int count = 0;

    Random r = new Random();
		
		try {
			long x = voteIntervalMillis / 2 + r.nextInt((int) voteIntervalMillis / 2);
			Thread.sleep(x);
			GNS.getLogger().fine("NameServerVoteThread: Sleeping for " + x + "ms");
		} catch (InterruptedException e) {
			if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Initial thread sleeping period.");
		}
		this.startInterval = System.currentTimeMillis();
		while (true) {
			// sleep between loops
			interval = System.currentTimeMillis() - startInterval;
			if (count >=1 && interval < voteIntervalMillis) {
				try {
//					long x = voteInterval - interval;
					Thread.sleep(voteIntervalMillis);
          GNS.getLogger().fine("NameServerVoteThread: Sleeping for " + voteIntervalMillis + "ms");
				} catch (InterruptedException e) {
					if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Thread sleeping interrupted.");
				}
			}
			count++;
			startInterval = System.currentTimeMillis();
			int vote;
      int update;
			NameServerSelectionPacket nsSelectionPacket;
//			JSONObject json;
      int nsToVoteFor = selectNSToVoteFor("0"); // name server selection does not depend on name
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" NameRecordStats Key Set: " + LocalNameServer.getNameRecordStatsKeySet());
      int nameCount = 0;
      int allNames = 0;
      long t0  = System.currentTimeMillis();
			for (String name : LocalNameServer.getNameRecordStatsKeySet()) {
        allNames ++;
        if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" BEGIN VOTING: " + name);
				//String name = nameAndType.getName();
				//NameRecordKey recordKey = nameAndType.getRecordKey();
				try {
          NameRecordStats stats = LocalNameServer.getStats(name);

					vote = stats.getVotes();
          update = stats.getUpdateVotes();
          if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" VOTE COUNT: " + vote);
					if (vote == 0 && update == 0) {
						continue;
					}
          nameCount++;
          if (StartLocalNameServer.debugMode) GNS.getLogger().fine("\tVoteSent\t" + name +"\t" + vote +"\t" + update+"\t");
//					int uniqueVoteID = r.nextInt();
					nsSelectionPacket = new NameServerSelectionPacket(name, vote, update, nsToVoteFor, LocalNameServer.nodeID, 0);

          // send to all primaries.
          Set<Integer> primaryNameServers = LocalNameServer.getPrimaryNameServers(name);
          if (StartLocalNameServer.debugMode) GNS.getLogger().info("Primary name servers = " + primaryNameServers + " name = " + name);
          for (int primary: primaryNameServers) {
            LocalNameServer.sendToNS(nsSelectionPacket.toJSONObject(), primary);
          }
          Thread.sleep(2);
//          LNSListener.tcpTransport.sendToIDs(, nsSelectionPacket.toJSONObject());

//					unackedVotes.put(uniqueVoteID, uniqueVoteID);

//          Thread.sleep(1);
					
//					// if not voted for by everyone,
//					Set<Integer> primaryNameServers = LocalNameServer.getPrimaryNameServers(name);
//
//					LocalNameServer.executorService.scheduleAtFixedRate(new CheckVoteStatus(json, uniqueVoteID, primaryNameServers), 0, TIMEOUT, TimeUnit.MILLISECONDS);
//					if (StartLocalNameServer.debugMode) GNS.getLogger().fine("VOTE THREAD: CheckVoteStatus Object created.  ID = " + uniqueVoteID);
					
					
//					ArrayList<Integer> destIDs = new ArrayList<Integer>();
//					ArrayList<Integer> portNumbers = new ArrayList<Integer>();
//					for (int x: primaryNameServers) {
//						destIDs.add(x);
//						portNumbers.add(ConfigFileInfo.getUpdatePort(x));
//					}
//					// LNS listening on Update port. Use its transport object to send packt
//					LNSListenerUpdate.transport.sendPacketToAll(json, destIDs, portNumbers);
//					//            Packet.multicastTCP(primaryNameServers, json, 2, GNRS.PortType.REPLICATION_PORT, -1);
//					StatusClient.sendTrafficStatus(LocalNameServer.nodeID, primaryNameServers, GNRS.PortType.UPDATE_PORT, nsSelectionPacket.getType(),
//							name, recordKey);
//					if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("nNameServerVoteThread: Vote send to :" + destIDs.toString() + "--> " + json.toString());
        } catch (Exception e) {
          e.printStackTrace();
        }
        //				LocalNameServer.printNameRecordStatsMap( debugMode );
			}
      long t1 = System.currentTimeMillis();
      GNS.getLogger().info("Round " + count +  ". Votes sent for " + nameCount + " names / " + allNames + " names. " +
              "Time = " + (t1 - t0) + " ms");
		}
	}

	
	private int selectNSToVoteFor(String name) {

    if (StartLocalNameServer.loadDependentRedirection) {
      Set<Integer> allNS = ConfigFileInfo.getAllNameServerIDs();
      return BestServerSelection.simpleLatencyLoadHeuristic(allNS);
    } else {
			return ConfigFileInfo.getClosestNameServer();
		}

//		else {// if (StartLocalNameServer.chooseFromClosestK > 1) {
//			int nameInt = Integer.parseInt(name);
//			int closestK = (nameInt % StartLocalNameServer.chooseFromClosestK) + 1;
//			if (closestK == 1) return ConfigFileInfo.getClosestNameServer();
//
//			Set<Integer> allNS = ConfigFileInfo.getAllNameServerIDs();
//
//			HashSet<Integer> excludeNS = new HashSet<Integer>();
//			excludeNS.add(ConfigFileInfo.getClosestNameServer());
//
//			while (excludeNS.size()  + 1 < closestK) {
//				int x = BestServerSelection.getSmallestLatencyNS(allNS, excludeNS);
//				excludeNS.add(x);
//			}
//			return BestServerSelection.getSmallestLatencyNS(allNS, excludeNS);
//		}
	}

	/**
	 * 
	 * @param json
	 * @throws JSONException 
	 */
	public static void handleNameServerSelection(JSONObject json) throws JSONException {
		
		NameServerSelectionPacket nameserverSelection = new NameServerSelectionPacket(json);
		removeVoteAcked(nameserverSelection.getUniqueID());
	}
	
	public static void removeVoteAcked(int ID) {
		unackedVotes.remove(ID);
	}
	
	public static boolean isVoteAcked(int ID) {
		return ! unackedVotes.containsKey(ID);
	}
}


class CheckVoteStatus extends TimerTask{

	JSONObject json;
	
	int voteID;
	Set<Integer> allPrimaries;
	Set<Integer> primariesQueried;
	
 	public CheckVoteStatus(JSONObject json, int voteID, Set<Integer> allPrimaries) {
 		this.json = json;
 		this.voteID = voteID;
 		this.allPrimaries = allPrimaries;
 		this.primariesQueried = new HashSet<Integer>();
	}
 	
	@Override
	public void run()
	{
		
		if (NameServerVoteThread.isVoteAcked(voteID)) {
			if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" VOTE ACKED: " + voteID);
			this.cancel();
			return;
		}
		
		int destID = -1;
		for (Integer x: allPrimaries) {
			if (primariesQueried.contains(x)) continue;
			else {
				destID = x;
				break;
			}
		}
		
		if (destID == -1) {
			this.cancel();
			NameServerVoteThread.removeVoteAcked(voteID);
			return;
		}
		
		primariesQueried.add(destID);
		
		if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" VOTE THREAD: sent vote to primary ID :" + destID + " VoteID = " + voteID);
		try
		{
			LNSListener.tcpTransport.sendToID(destID, json);
		} catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

  }
	
	
}

