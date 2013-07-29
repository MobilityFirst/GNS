package edu.umass.cs.gnrs.nameserver;

import org.json.JSONObject;


import edu.umass.cs.gnrs.main.GNS;
import edu.umass.cs.gnrs.packet.Transport;
import edu.umass.cs.gnrs.util.ConfigFileInfo;

/**
 * This class listens on a UDP port for messages (Lookups, Updates, Add, Remove)
 * from clients and 
 * 
 * @author abhigyan
 * 
 */
public class NSListenerUDP extends Thread
{
	public static Transport udpTransport;

	public NSListenerUDP()
	{
		udpTransport = new Transport(NameServer.nodeID,
				ConfigFileInfo.getUpdatePort(NameServer.nodeID),
				NameServer.timer);
	}
	
	@Override
	public void run()
	{
		while (true)
		{
			try
			{
				JSONObject incomingJSON = udpTransport.readPacket();
				NameServer.nsDemultiplexer.handleJSONObject(incomingJSON);
			} catch (Exception e)
			{
				GNS.getLogger().fine("Exception in thread: " + e.getMessage());
				e.printStackTrace();
			}
		}
	}


}

