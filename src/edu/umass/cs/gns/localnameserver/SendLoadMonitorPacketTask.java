package edu.umass.cs.gns.localnameserver;

import java.io.IOException;
import java.util.TimerTask;

import org.json.JSONException;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.packet.NameServerLoadPacket;
import edu.umass.cs.gns.packet.Packet;

public class SendLoadMonitorPacketTask extends TimerTask
{

	int nameServerID;
	NameServerLoadPacket nsLoad;
	public SendLoadMonitorPacketTask(int nsID) {
		nameServerID = nsID;
		nsLoad = new NameServerLoadPacket(nsID,0);
	}
	
	@Override
	public void run()
	{
		try
		{
			Packet.sendUDPPacket(nameServerID, LocalNameServer.socket, nsLoad.toJSONObject(), GNS.PortType.UPDATE_PORT);
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		GNS.getLogger().fine("NameServerLoadMonitor: LoadMonitorPacketSent. NameServer:" + nsLoad.getNsID() + 
				" Load:" + nsLoad.getLoadValue());
	}

}
