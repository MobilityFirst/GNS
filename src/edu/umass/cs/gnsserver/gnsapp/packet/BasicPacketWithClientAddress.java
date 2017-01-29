
package edu.umass.cs.gnsserver.gnsapp.packet;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.utils.Util;

import java.net.InetSocketAddress;

import org.json.JSONException;
import org.json.JSONObject;


public abstract class BasicPacketWithClientAddress extends BasicPacket {

  private final static String CLIENT_ADDRESS = "clientAddress";

  private InetSocketAddress clientAddress = null;


  public BasicPacketWithClientAddress() {
    // will be filled in when it is next read by receiver after getting sent
    this.clientAddress = null;
  }


  public BasicPacketWithClientAddress(JSONObject json) throws JSONException {
    this.clientAddress = json.has(CLIENT_ADDRESS)
            ? Util.getInetSocketAddressFromString(json.getString(CLIENT_ADDRESS))
            : MessageNIOTransport.getSenderAddress(json);
  }
  

  protected ClientRequest response  =null;

public BasicPacketWithClientAddress setResponse(ClientRequest response) {
	synchronized(this) {
		if(this.response!=null) throw new RuntimeException("Can set response for ClientRequest at most once");
		this.response = response;
		return this;
	}
  }

  @Override
  public void addToJSONObject(JSONObject json) throws JSONException {
    if (clientAddress != null) {
      json.put(CLIENT_ADDRESS, clientAddress.toString());
    }
  }


  public InetSocketAddress getClientAddress() {
    return clientAddress;
  }

}
