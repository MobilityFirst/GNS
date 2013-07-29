package edu.umass.cs.gnrs.localnameserver;

import edu.umass.cs.gnrs.main.GNS;
import edu.umass.cs.gnrs.main.StartLocalNameServer;
import edu.umass.cs.gnrs.nameserver.NameRecordKey;
import edu.umass.cs.gnrs.packet.Packet;
import edu.umass.cs.gnrs.packet.UpdateAddressPacket;
import edu.umass.cs.gnrs.packet.UpdateOperation;
import edu.umass.cs.gnrs.util.Update;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.Random;
import java.util.TimerTask;

public class SendUpdatesViaIntercessor {

//	private static ExponentialDistribution exponentialDistribution;
	
	public static void schdeduleAllUpdates() {
		if (LocalNameServer.updateTrace == null) {
			if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Update trace is null. SendQueriesViaIntercessor thread quitting.");
			return;
		}
		if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Send update intercessor started. Number of queries. "
				+ LocalNameServer.updateTrace.size());
//		exponentialDistribution = new ExponentialDistribution(StartLocalNameServer.updateRateRegular);
		double delay = 0;

		double expectedDurationSec = (LocalNameServer.updateTrace.size()
				* StartLocalNameServer.updateRateRegular) / 1000;
		String msg = "SendUpdateStart StartTime " + delay
				+ " Expected-Duration " + expectedDurationSec
				+ " Number-Queries " + LocalNameServer.updateTrace.size();

		GNS.getStatLogger().fine(msg);
		if (StartLocalNameServer.debugMode) GNS.getLogger().fine(msg);
		int count = 0;
		for (Update u : LocalNameServer.updateTrace) {
			count++;
			LocalNameServer.experimentSendRequestTimer.schedule(
					new SendUpdateIntercessorTask(u.name, count), (long) delay);
			delay += StartLocalNameServer.updateRateRegular;// exponentialDistribution.exponential();
			if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" Send update scheduled: count " + count + " delay = " + delay);
		}
		if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Final delay = " + delay / 1000 + " Expected-duration " + expectedDurationSec);
	}

	/**
	 * Returns a 8-character string.
	 *
	 * @return
	 */
	public static String getRandomString() {
		Random rand = new Random();
		int intRange = 1000000;
		Integer x = intRange + rand.nextInt(1000000);
		return x.toString();
	}
}


class SendUpdateIntercessorTask extends TimerTask {

	int updateCount;
	String name;

	public SendUpdateIntercessorTask(String name, int updateCount) {

		this.updateCount = updateCount;
		this.name = name;
	}


	@Override
	public void run() {
        ArrayList<String> newValue = new ArrayList<String>();
        newValue.add(SendUpdatesViaIntercessor.getRandomString());

        UpdateAddressPacket updateAddressPacket = new UpdateAddressPacket(Packet.PacketType.UPDATE_ADDRESS_LNS,
                0, updateCount, updateCount, -1,
                name, NameRecordKey.EdgeRecord, newValue, null,
                UpdateOperation.REPLACE_ALL, LocalNameServer.nodeID, -1);
        try {
            LNSListener.demultiplexLNSPackets(updateAddressPacket.toJSONObject());
        } catch (JSONException e) {
            e.printStackTrace();
        }
	}
}

//class SendUpdateIntercessorTask extends TimerTask {
//
//  int updateCount;
//  String name;
//  int seqNumber;
//  ArrayList<String> oldValue;
//  ArrayList<String> newValue;
//
//  public SendUpdateIntercessorTask(int count, String name1,
//          int sequenceNumber, ArrayList<String> oldValue, ArrayList<String> newValue) {
//    this.name = name1;
//    this.updateCount = count;
//    this.seqNumber = sequenceNumber;
//    this.oldValue = oldValue;
//    this.newValue = newValue;
//  }
//
//  @Override
//  public void run() {
//  	UpdateAddressPacket updateAddressPacket = new UpdateAddressPacket(Packet.PacketType.UPDATE_ADDRESS_LNS,
//        0, updateCount,
//        name, NameRecordKey.EdgeRecord, newValue,
//        UpdateAddressPacket.UpdateOperation.REPLACE);
//
//  	LNSListenerUpdate.sendUpdatePacket(updateCount, updateAddressPacket);
//
////    LocalNameServer.myInter.sendUpdateWithSequenceNumber(name, 0, seqNumber, "edgeRecord", newValue);
////    if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("Intercessor sending update number " + updateCount + " for name " + name);
//  }
//}
//	public void run()
//	{
//		if (LocalNameServer.updateTrace == null)
//		{
//			if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("Update trace is null. SendUpdatesViaIntercessor thread quitting.");
//			return;
//		}
//		long startTime = System.currentTimeMillis();
//		
//		if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("Send updates intercessor started. Number of queries."
//						+ LocalNameServer.updateTrace.size() + 
//						" Update rate regular " + StartLocalNameServer.updateRateRegular);
//		int count = 0;
//		ArrayList<Long> queryTimes = new ArrayList<Long>();
//		long tStart = 0;
//		
//		for( Update u : LocalNameServer.updateTrace) {
//			String name = u.name;
//			if (queryTimes.size() == 0) {
//				tStart = System.currentTimeMillis();
//				long timeBetweenUpdate = 0;
//				for (int i = 0; i < 1000; i++) {
//					timeBetweenUpdate += Util.round(exponentialDistribution.exponential());
//					queryTimes.add(timeBetweenUpdate);
//				}
//			}
//			count++;
//			if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("Sending Update. count = "+ (count) +" name = "+ name);
//			
//			ArrayList<String> oldValue = new ArrayList<String>();
//			// Sending a random string value as new address.
//			ArrayList<String> newValue = new ArrayList<String>();
//			newValue.add(getRandomString());
//			LocalNameServer.myInter.sendUpdateWithSequenceNumber(u.name, u.seqNumber, "edgeRecord", oldValue, newValue);
////			if( !LocalNameServer.workloadContainsName( name ) ) continue;
//			
//			long tNow = System.currentTimeMillis();
//			long sleepTime = queryTimes.get(0) - (tNow - tStart);
//			long delay = (tNow - startTime) - (StartLocalNameServer.updateRateRegular * count);
//			if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("SendUpdateTimeDiff. count = "+ count +
//					" sleepTime = "+ sleepTime + " delay = " + delay);
//			
////			if (timeBetweenQuery < tUsed) 
////			delay += tUsed - timeBetweenQuery;			
////			timeBetweenQuery -= (tUsed);
////			if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("Sending Query. count = "+ (count) +
////					" waiting = "+ timeBetweenQuery);
//			
//			if (sleepTime  > 0)  
//			{
//				try {
//					Thread.sleep(sleepTime);
//				} catch (InterruptedException e)
//				{
//					e.printStackTrace();
//				}
//			}
//			queryTimes.remove(0);
//			//Time (ms) between events selected from an exponential distribution	
////			int timeBetweenUpdate = Util.round( exponentialDistribution.exponential() );
////			if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("Sending Update. count = "+ (count) +" waiting = "+ timeBetweenUpdate);
////			try
////			{
////				Thread.sleep(timeBetweenUpdate);
////			} catch (InterruptedException e)
////			{
////				// TODO Auto-generated catch block
////				e.printStackTrace();
////			}
//			
//			// Old value is not read at name server, so, just sending empty array list. 
////			ArrayList<String> oldValue = new ArrayList<String>();
////			// Sending a random string value as new address.
////			ArrayList<String> newValue = new ArrayList<String>();
////			newValue.add(getRandomString());
////			LocalNameServer.myInter.sendUpdate(u, "edgeRecord", oldValue, newValue);
//		}
//	}

