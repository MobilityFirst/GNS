package edu.umass.cs.gns.nameserver.replicacontroller;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.TimerTask;


import edu.umass.cs.gns.main.StartNameServer;
import org.json.JSONException;

import edu.umass.cs.gns.main.GNS;

import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.packet.OldActiveSetStopPacket;
import edu.umass.cs.gns.packet.Packet.PacketType;

import edu.umass.cs.gns.paxos.FailureDetection;

/**
 * This class sends a message to current active replicas to stop an active replica
 * @author abhigyan
 *
 */
public class StopActiveSetTask extends TimerTask
{


	int MAX_ATTEMPTS = 3;		 // number of actives contacted to start replica
	
	String name;
	
	//NameRecordKey nameRecordKey;
	
	Set<Integer> oldActiveNameServers;
	
	Set<Integer> newActiveNameServers;
	
	Set<Integer> oldActivesQueried;
	
	String oldPaxosID;
	
	/**
	 * Constructor object
	 * @param name
	 * @param oldActiveNameServers
	 * @param newActiveNameServers
	 */
	public StopActiveSetTask(String name, //NameRecordKey nameRecordKey, 
			Set<Integer> oldActiveNameServers, Set<Integer> newActiveNameServers, String oldPaxosID) {
		this.name = name;
		//this.nameRecordKey = nameRecordKey;
		this.oldActiveNameServers = oldActiveNameServers;
		this.newActiveNameServers = newActiveNameServers;
		this.oldActivesQueried = new HashSet<Integer>();
		this.oldPaxosID = oldPaxosID;
	}
	

	@Override
	public void run()
	{
		
		ReplicaControllerRecord nameRecord = NameServer.replicaController.getNameRecordPrimary(name);

        if (nameRecord == null) {
            if (StartNameServer.debugMode) GNS.getLogger().severe(" Name Record Does not Exist. Name = " + name 
                   // + " Record Key = " + nameRecordKey
                    );
            this.cancel();
            return;
        }

        if (oldActivesQueried.size() == 0 && !ReplicaController.isSmallestPrimaryRunning(nameRecord.getPrimaryNameservers())) {
			if (StartNameServer.debugMode) GNS.getLogger().fine(" This node isnt the smallest primary active. will not proceed further.");
			this.cancel();
			return;
		}
		
		// is active with paxos ID have stopped return true
		if (nameRecord.isOldActiveStopped(oldPaxosID)) {
			if (StartNameServer.debugMode) GNS.getLogger().fine("Old active name servers stopped. Paxos ID: " + oldPaxosID
					+ " Old Actives : " + oldActiveNameServers);
			this.cancel();
			return;
		}
		
		if (oldActivesQueried.size() == MAX_ATTEMPTS) {
			if (StartNameServer.debugMode) GNS.getLogger().severe("ERROR: Old Actives failed to STOP after " + MAX_ATTEMPTS + ". " +
					"Old active name servers queried: " +  oldActivesQueried);
			this.cancel();
			return;
		}
		
		int selectedOldActive = selectNextActiveToQuery();
		
		if (selectedOldActive == -1) {
			if (StartNameServer.debugMode) GNS.getLogger().severe("ERROR: No more old active left to query. " +
					"Old Active name servers queried: " +  oldActivesQueried + ". Old Actives not STOPped yet..");
			this.cancel();
			return;
		}
		
		if (StartNameServer.debugMode) GNS.getLogger().fine(" Old Active Name Server Selected to Query: " + selectedOldActive);
		
		OldActiveSetStopPacket packet = new OldActiveSetStopPacket(name, //nameRecordKey, 
				NameServer.nodeID, selectedOldActive, oldPaxosID, PacketType.OLD_ACTIVE_STOP);
		if (StartNameServer.debugMode) GNS.getLogger().fine(" Old active stop Sent Packet: " + packet);
		try
		{
			NameServer.tcpTransport.sendToID(packet.toJSONObject(), selectedOldActive, GNS.PortType.STATS_PORT);
		} catch (IOException e)
		{
			if (StartNameServer.debugMode) GNS.getLogger().fine("IO Exception in sending OldActiveSetSTOPPacket: " + e.getMessage());
			e.printStackTrace();
		} catch (JSONException e)
		{
			if (StartNameServer.debugMode) GNS.getLogger().fine("JSON Exception in sending OldActiveSetSTOPPacket: " + e.getMessage());
			e.printStackTrace();
		}
		
		// if first time:  
		// 		Send message to one of the old actives to stop replica set. schedule next timer event.
		// else if active replica stopped:
		// 		cancel timer. return
		// else if no more active replicas to send message to:
		// 		log error message. cancel timer. return 
		// else:
		// 		send to next active replica. schedule next timer event.
		
	}


	/**
	 * the next active name server that will be queried to start active replicas 
	 * @return
	 */
	private int selectNextActiveToQuery() {
		int selectedActive = -1;
		for (int x: oldActiveNameServers) {
			if (oldActivesQueried.contains(x) || !FailureDetection.isNodeUp(x)) continue;
			selectedActive = x;
			break;
		}
		if (selectedActive != -1) {
			oldActivesQueried.add(selectedActive);
		}
		return selectedActive;
	}
}
