package nio;

import java.util.ArrayList;

import org.json.JSONException;
import org.json.JSONObject;

//import edu.umass.cs.gnrs.localnameserver.LNSListenerUpdate;
import edu.umass.cs.gns.packet.Packet;

public class LNSPacketDemultiplexer extends PacketDemultiplexer{

	@Override
	public void handleJSONObjects(ArrayList jsonObjects) {
		for (Object j: jsonObjects) {
			handleJSONObject((JSONObject) j);
		}
	}

	private void handleJSONObject(JSONObject json) {
		
		try {
			switch(Packet.getPacketType(json)) {
			case ADD_RECORD_LNS:
			case CONFIRM_ADD_LNS:
			case UPDATE_ADDRESS_LNS:
      case CONFIRM_UPDATE_LNS:
//				LNSListenerUpdate.handleJSONObject(json);
				break;
			
			}
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
