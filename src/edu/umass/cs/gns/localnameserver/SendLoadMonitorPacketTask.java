package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.packet.NameServerLoadPacket;
import edu.umass.cs.gns.packet.Packet;
import org.json.JSONException;

import java.io.IOException;
import java.util.TimerTask;

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
			Packet.sendUDPPacket(nameServerID, LocalNameServer.socket, nsLoad.toJSONObject(), GNS.PortType.LNS_UDP_PORT);
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
