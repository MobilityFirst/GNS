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
	
	/**
	 * Timeout interval for the ack of an vote before it is sent to second primary.
	 */
	public static int TIMEOUT = 5000;

	/** Time interval in ms between transmitting votes **/
	private long voteInterval;
	/** Start interval **/
	private long startInterval;
	
	
	public static ConcurrentHashMap<Integer,Integer> unackedVotes = new ConcurrentHashMap<Integer,Integer>();
	
	Random r = new Random();
	/**************************************************************
	 * Constructs a new NameServerVoteThread that periodically 
	 * multicast nameserver selection votes to primary nameservers 
	 * of a name.
	 * @param voteInterval Time interval in ms between transmitting
	 * votes
	 *************************************************************/
	public NameServerVoteThread(long voteInterval) {
		super("NameServerVoteThread");
		this.voteInterval = voteInterval;

	}

	/*************************************************************
	 * Starts executing this thread.
	 ************************************************************/
	@Override
	public void run() {
		long interval = 0;
		int count = 0;

        Random r = new Random();
		
		try {
			int x = r.nextInt((int)voteInterval);
			Thread.sleep(x);
			System.out.println("NameServerVoteThread: Sleeping for " + x + "ms");
		} catch (InterruptedException e) {
			if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Initial thread sleeping period.");
		}
		this.startInterval = System.currentTimeMillis();
		while (true) {
			// sleep between loops
			interval = System.currentTimeMillis() - startInterval;
			if (interval < voteInterval && count > 0) {
				try {
					long x = voteInterval - interval;
					Thread.sleep(x);
					System.out.println("NameServerVoteThread: Sleeping for " + x + "ms");
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
      int nsToVoteFor = selectNSToVoteFor("0"); // TODO is name server selection independent of name?
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" NameRecordStats Key Set: " + LocalNameServer.getNameRecordStatsKeySet());
			for (String name : LocalNameServer.getNameRecordStatsKeySet()) {
                GNS.getLogger().fine(" BEGIN VOTING: " + name);
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

//					int uniqueVoteID = r.nextInt();
					nsSelectionPacket = new NameServerSelectionPacket(name, vote, update, nsToVoteFor, LocalNameServer.nodeID, 0);

          // send to all primaries.
          LNSListener.tcpTransport.sendToIDs(LocalNameServer.getPrimaryNameServers(name), nsSelectionPacket.toJSONObject());

//					unackedVotes.put(uniqueVoteID, uniqueVoteID);

          Thread.sleep(1);
					
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
		}
	}

	
	private int selectNSToVoteFor(String name) {
		
		if (StartLocalNameServer.chooseFromClosestK == 1  
				&& StartLocalNameServer.loadDependentRedirection == false) {
			return ConfigFileInfo.getClosestNameServer();
		}
		else if (StartLocalNameServer.loadDependentRedirection) {
			Set<Integer> allNS = ConfigFileInfo.getAllNameServerIDs();
			return BestServerSelection.simpleLatencyLoadHeuristic(allNS);
		}
		else {// if (StartLocalNameServer.chooseFromClosestK > 1) {
			int nameInt = Integer.parseInt(name);
			int closestK = (nameInt % StartLocalNameServer.chooseFromClosestK) + 1;
			if (closestK == 1) return ConfigFileInfo.getClosestNameServer();
			
			Set<Integer> allNS = ConfigFileInfo.getAllNameServerIDs();
			
			HashSet<Integer> excludeNS = new HashSet<Integer>();
			excludeNS.add(ConfigFileInfo.getClosestNameServer());
			
			while (excludeNS.size()  + 1 < closestK) {
				int x = BestServerSelection.getSmallestLatencyNS(allNS, excludeNS);
				excludeNS.add(x);
			}
			return BestServerSelection.getSmallestLatencyNS(allNS, excludeNS);
		}
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

