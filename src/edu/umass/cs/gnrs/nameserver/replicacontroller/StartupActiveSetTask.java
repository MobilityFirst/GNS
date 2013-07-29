package edu.umass.cs.gnrs.nameserver.replicacontroller;

import edu.umass.cs.gnrs.main.GNS;
import edu.umass.cs.gnrs.main.StartNameServer;
import edu.umass.cs.gnrs.nameserver.NameServer;
import edu.umass.cs.gnrs.nameserver.ValuesMap;
import edu.umass.cs.gnrs.packet.NewActiveSetStartupPacket;
import edu.umass.cs.gnrs.packet.Packet.PacketType;
import org.json.JSONException;
import paxos.FailureDetection;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.TimerTask;

/**
 * This class sends message to active replica to startup a new paxos instance
 * for a name record.
 * @author abhigyan
 *
 */
public class StartupActiveSetTask extends TimerTask
{

	int MAX_ATTEMPTS = 3;		 // number of actives contacted to start replica
	
	String name;
	
	//NameRecordKey nameRecordKey;
	
	Set<Integer> oldActiveNameServers;
	
	Set<Integer> newActiveNameServers;
	
	Set<Integer> newActivesQueried;
	
	String newActivePaxosID;
	
	String oldActivePaxosID;

    ValuesMap initialValue;
	/**
	 * Constructor object
	 * @param name
	 * @param oldActiveNameServers
	 * @param newActiveNameServers
	 */
	public StartupActiveSetTask(String name, //NameRecordKey nameRecordKey, 
			Set<Integer> oldActiveNameServers, Set<Integer> newActiveNameServers,
			String newActivePaxosID, String oldActivePaxosID, ValuesMap initialValue) {
		this.name = name;
		//this.nameRecordKey = nameRecordKey;
		this.oldActiveNameServers = oldActiveNameServers;
		this.newActiveNameServers = newActiveNameServers;
		this.newActivesQueried = new HashSet<Integer>();
		this.newActivePaxosID = newActivePaxosID;
		this.oldActivePaxosID = oldActivePaxosID;
        this.initialValue = initialValue;
	}
	

	@Override
	public void run()
	{


		ReplicaControllerRecord nameRecord = DBReplicaController.getNameRecordPrimary(name);
//        NameServer.getNameRecord(name// , nameRecordKey
//                        );

        if (nameRecord == null) {
            if (StartNameServer.debugMode) GNS.getLogger().severe(" Name Record Does not Exist. Name = " + name 
                    //+ " Record Key = " + nameRecordKey
                    );
            this.cancel();
            return;
        }
		
		if (newActivesQueried.size() == 0 && !ReplicaController.isSmallestPrimaryRunning(nameRecord.getPrimaryNameservers())) {
			if (StartNameServer.debugMode) GNS.getLogger().fine(" Node = " + NameServer.nodeID + " isn't the smallest primary active. will not proceed further.");
			this.cancel();
			return;
		}
		
		if (!nameRecord.getActivePaxosID().equals(newActivePaxosID)) {
			if (StartNameServer.debugMode) GNS.getLogger().fine(" Actives got accepted and replaced by new actives. Quitting. ");
			this.cancel();
			return;
		}
		
		if (nameRecord.isActiveRunning()) {
			if (StartNameServer.debugMode) GNS.getLogger().fine("New active name servers running. Startup done. All Actives: " +
					nameRecord.copyActiveNameServers()  + " Actives Queried: " + newActivesQueried);
			this.cancel();
			return;
		}
		
		if (newActivesQueried.size() == MAX_ATTEMPTS) {
			if (StartNameServer.debugMode) GNS.getLogger().severe("ERROR: New Actives failed to start after " + MAX_ATTEMPTS + ". " +
					"Active name servers queried: " +  newActivesQueried);
			this.cancel();
			return;
		}
		
		int selectedActive = selectNextActiveToQuery();
		
		if (selectedActive == -1) {
			if (StartNameServer.debugMode) GNS.getLogger().severe("ERROR: No more active left to query. " +
					"Active name servers queried: " +  newActivesQueried + " Actives not started.");
			this.cancel();
			return;
		}
 
		if (StartNameServer.debugMode) GNS.getLogger().fine(" Active Name Server Selected to Query: " + selectedActive);
		
		NewActiveSetStartupPacket packet = new NewActiveSetStartupPacket(name, //nameRecordKey,
				NameServer.nodeID, selectedActive, newActiveNameServers, oldActiveNameServers, 
				oldActivePaxosID,newActivePaxosID, PacketType.NEW_ACTIVE_START, initialValue, false);
		try
		{
			NameServer.tcpTransport.sendToID(packet.toJSONObject(), selectedActive, GNS.PortType.STATS_PORT);
		} catch (IOException e)
		{
			if (StartNameServer.debugMode) GNS.getLogger().fine("IO Exception in sending NewActiveSetStartupPacket: " + e.getMessage());
			e.printStackTrace();
		} catch (JSONException e)
		{
			if (StartNameServer.debugMode) GNS.getLogger().fine("JSON Exception in sending NewActiveSetStartupPacket: " + e.getMessage());
			e.printStackTrace();
		}
		if (StartNameServer.debugMode) GNS.getLogger().fine(" NEW ACTIVE STARTUP PACKET SENT: " + packet.toString());
		
		// 
		// if first time:  
		// 		send message to an active replica to get started. schedule next timer event.
		// else if active replica started:
		// 		cancel timer.
		// else if no more active replicas to send message to:
		// 		log error message. cancel timer. 
		// else: 
		// 		send to a next active replica. schedule next timer event.
	}

	/**
	 * the next active name server that will be queried to start active replicas 
	 * @return
	 */
	private int selectNextActiveToQuery() {
		int selectedActive = -1;
		for (int x: newActiveNameServers) {
			if (newActivesQueried.contains(x) || !FailureDetection.isNodeUp(x)) continue;
			selectedActive = x;
			break;
		}
		if (selectedActive != -1) {
			newActivesQueried.add(selectedActive);
		}
		return selectedActive;
	}
	
}
