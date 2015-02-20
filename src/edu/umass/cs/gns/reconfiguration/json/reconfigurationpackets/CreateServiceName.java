package edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets;


import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.nio.JSONNIOTransport;
import edu.umass.cs.gns.reconfiguration.InterfaceReplicableRequest;
import edu.umass.cs.gns.reconfiguration.RequestParseException;
import edu.umass.cs.gns.util.Stringifiable;
import edu.umass.cs.gns.util.StringifiableDefault;

/**
@author V. Arun
 */
public class CreateServiceName extends BasicReconfigurationPacket<InetSocketAddress> implements InterfaceReplicableRequest {

	public static enum Keys {INITIAL_STATE};

	public static final Stringifiable<InetSocketAddress> unstringer = new StringifiableDefault<InetSocketAddress>(
			new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));

	//private boolean coordType = false;
        
        public final String initialState;
	
	public CreateServiceName(
			InetSocketAddress initiator,
			String name, int epochNumber, String state) {
		super(initiator, ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME, name, epochNumber);
                this.initialState = state;
	}
	public CreateServiceName(JSONObject json, Stringifiable<?> unstringer) throws JSONException {
		super(json, CreateServiceName.unstringer); // ignores argument unstringer
		this.setSender(JSONNIOTransport.getSenderAddress(json));
                this.initialState = json.optString(Keys.INITIAL_STATE.toString(), null);
	}
	public CreateServiceName(JSONObject json) throws JSONException {
		this(json, unstringer);
	}
        
        @Override
        public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
                if (initialState != null) {
                  json.put(Keys.INITIAL_STATE.toString(), initialState);
                }
                return json;
        }
        
	@Override
	public IntegerPacketType getRequestType() throws RequestParseException {
		return ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME;
	}
	@Override
	public boolean needsCoordination() {
		return false; //coordType;
	}
	@Override
	public void setNeedsCoordination(boolean b) {
		//coordType = b;
	}

        public String getInitialState() {
          return initialState;
        }
               
}
