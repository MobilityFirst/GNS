
package edu.umass.cs.gnsserver.gnsapp.packet;

import java.net.InetSocketAddress;
import org.json.JSONException;
import org.json.JSONObject;


public abstract class BasicPacketWithReturnAddress extends BasicPacketWithClientAddress {


  private final static String RETURN_ADDRESS = "returnAddress";


  private final static String RETURN_PORT = "returnPort";


  protected final static int INVALID_PORT = -1;
  //

  private InetSocketAddress returnAddress = null;


  public BasicPacketWithReturnAddress(JSONObject json) throws JSONException {
    super(json);
    String address = json.optString(RETURN_ADDRESS, null);
    int port = json.optInt(RETURN_PORT, INVALID_PORT);
    this.returnAddress = address != null && port != INVALID_PORT
            ? new InetSocketAddress(address, port) : null;
  }
  

  public BasicPacketWithReturnAddress() {
    this.returnAddress = null;
  }
  

  public BasicPacketWithReturnAddress(InetSocketAddress address) {
    this.returnAddress = address;
  }
  

  @Override
  public void addToJSONObject(JSONObject json) throws JSONException {
    super.addToJSONObject(json);
    if (returnAddress != null) {
      json.put(RETURN_ADDRESS, returnAddress.getHostString());
      json.put(RETURN_PORT, returnAddress.getPort());
    }
  }


  public InetSocketAddress getReturnAddress() {
    return returnAddress;
  }

//
//  public void setReturnAddress(InetSocketAddress returnAddress) {
//    this.returnAddress = returnAddress;
//  }

}
