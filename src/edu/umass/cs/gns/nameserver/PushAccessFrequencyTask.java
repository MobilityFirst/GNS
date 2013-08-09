package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.replicacontroller.ListenerNameRecordStats;
import edu.umass.cs.gns.packet.NameRecordStatsPacket;
import edu.umass.cs.gns.paxos.PaxosManager;
import edu.umass.cs.gns.statusdisplay.StatusClient;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.TimerTask;

/**
 * ***********************************************************
 * This class implements a thread that periodically pushes read and write stats for names in its lookup table.
 *
 * @author Hardeep Uppal
 * **********************************************************
 */
public class PushAccessFrequencyTask extends TimerTask {

  int count = 0;

  @Override
  public void run() {
    count++;
    StatusClient.sendStatus(NameServer.nodeID, "Pushing stats: " + count);

    //Iterate through the NameRecords and push access frequency stats
    for (NameRecord nameRecord : NameServer.getAllNameRecords()) {
    //for (NameRecord nameRecord : DBNameRecord.getAllNameRecords()) {


      if (nameRecord.getTotalLookupRequest() == 0 && nameRecord.getTotalUpdateRequest() == 0) {
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("Zero read write frequency. NO Frequency to report.");
        }
        continue;
      }

      NameRecordStatsPacket statsPacket = new NameRecordStatsPacket(
              // nameRecord.getRecordKey(), 
              nameRecord.getName(), nameRecord.getTotalLookupRequest(),
              nameRecord.getTotalUpdateRequest(), NameServer.nodeID);

      try {
        JSONObject json = statsPacket.toJSONObject();
        if (StartNameServer.debugMode) {
          GNS.getLogger().fine("PUSH_STATS: Round " + count + " Name " + nameRecord.getName()
                  + " To nodes: " + nameRecord.getPrimaryNameservers() + " --> " + json);
        }
        int selectedPrimaryNS = -1;
        for (int x : nameRecord.getPrimaryNameservers()) {
          if (PaxosManager.isNodeUp(x)) {
            selectedPrimaryNS = x;
            break;
          }
        }
        if (selectedPrimaryNS != -1 && selectedPrimaryNS != NameServer.nodeID) {
          NameServer.tcpTransport.sendToID(json, selectedPrimaryNS,
                  GNS.PortType.STATS_PORT);
        } else if (selectedPrimaryNS == NameServer.nodeID) {
          // if same node, then directly call the function
          ListenerNameRecordStats.handleIncomingPacket(json);
        }

//				NameServer.nio.sendToID(json, nameRecord.getMainPrimary(), GNRS.PortType.STATS_PORT);
      } catch (JSONException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }

      StatusClient.sendTrafficStatus(NameServer.nodeID, nameRecord.getPrimaryNameservers(),
              GNS.PortType.STATS_PORT, statsPacket.getType(), nameRecord.getName());

    }
  }
//	/**
//	 * Time interval in ms between push *
//	 */
//	private long aggregateInterval;
//	/**
//	 * Start interval *
//	 */
//	private long startInterval;
  //  private Transport transport;
//	/**
//	 * ***********************************************************
//	 * Constructs a new PushAccessFrequencyThread to push read and write stats of all names stored at this name server.
//	 *
//	 * @param aggregateInterval Time interval in ms between push.
//	 * **********************************************************
//	 */
//	public PushAccessFrequencyThread() {
//		super("PushAccessFrequencyThread");
//		this.aggregateInterval = aggregateInterval;
//		this.startInterval = System.currentTimeMillis();
  //    this.transport = new Transport(NameServer.nodeID);
//	}
//	/**
//	 * ***********************************************************
//	 * Starts executing this thread. **********************************************************
//	 */
//	@Override
//	public void run() {
//		long interval;
//		int count = 0;
//
//
//		Random r = new Random();
//		try {
//			int x = r.nextInt((int) aggregateInterval);
//			Thread.sleep(x);
//			System.out.println("PushAccessFrequencyThread: sleeping for " + x + " ms");
//		} catch (InterruptedException e) {
//			GNRS.getLogger().fine("Initial thread sleeping period.");
//		}
//
//		while (true) {
//			interval = System.currentTimeMillis() - startInterval;
//			if (interval < aggregateInterval && count > 0) {
//				try {
//					long x = aggregateInterval - interval;
//					Thread.sleep(x);
//					System.out.println("PushAccessFrequencyThread: Sleeping for " + x + " ms");
//				} catch (InterruptedException e) {
//					GNRS.getLogger().fine("Push Access Frequency Thread sleeping.");
//				}
//			}
//			count++;
//			startInterval = System.currentTimeMillis();
//
//			StatusClient.sendStatus(NameServer.nodeID, "Pushing stats: " + count);
//
//			//Iterate through the NameRecords and push access frequency stats       
//			for (NameRecord nameRecord : NameServer.getAllNameRecords()) {
//				if (nameRecord.getTotalReadFrequency() == 0 && nameRecord.getTotalWriteFrequency() == 0) {
//					GNRS.getLogger().fine("Zero read write freqeuncy. NO Frequency to report.");
//					continue;
//				}
//
//				// Abhigyan: Replication analysis is done only by "main" primary, 
//				// see method nameRecord.getMainPrimary().
//				if (nameRecord.getMainPrimary() == NameServer.nodeID) continue;
//
//				NameRecordStatsPacket statsPacket = new NameRecordStatsPacket(
//						nameRecord.getRecordKey(), nameRecord.getName(), nameRecord.getTotalReadFrequency(),
//						nameRecord.getTotalWriteFrequency(), NameServer.nodeID);
//				
//				try {
//					JSONObject json = statsPacket.toJSONObject();
//					GNRS.getLogger().fine("PUSH_STATS: Round " + count + " Name " + nameRecord.getName() + 
//							" To nodes: " + nameRecord.getMainPrimary() + " --> " + json);
//					NameServer.nio.sendToID(json, nameRecord.getMainPrimary(), GNRS.PortType.STATS_PORT);
//				} catch (JSONException e) {
//					e.printStackTrace();
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//				
//				StatusClient.sendTrafficStatus(NameServer.nodeID, nameRecord.getMainPrimary(),  
//						GNRS.PortType.STATS_PORT, statsPacket.getType(), nameRecord.getName(), nameRecord.getRecordKey());
//				
//				//Push access frequencies to primary name servers
//
//				//      ArrayList<Integer> destIDs = new ArrayList<Integer>();
//				//      ArrayList<Integer> portNumbers = new ArrayList<Integer>();
//				//      for (int x : primaryNameServers) {
//				//        if (x == NameServer.nodeID) {
//				//          continue;
//				//        }
//				//        destIDs.add(x);
//				//        portNumbers.add(ConfigFileInfo.getStatsPort(x));
//				//      }
//				//    try {
//				//    transport.sendPacketToAll(json, destIDs, portNumbers);
//				//  } catch (JSONException e) {
//				//    // TODO Auto-generated catch block
//				//    e.printStackTrace();
//				//  }
//
//
//			}
//		}
//
//	}
}
