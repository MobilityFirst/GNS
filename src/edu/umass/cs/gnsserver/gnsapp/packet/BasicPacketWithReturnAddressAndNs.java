
package edu.umass.cs.gnsserver.gnsapp.packet;

import edu.umass.cs.nio.interfaces.Stringifiable;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetSocketAddress;


public abstract class BasicPacketWithReturnAddressAndNs<NodeIDType> extends BasicPacketWithReturnAddress {


  private final static String NAMESERVER_ID = "ns_ID";


  private NodeIDType nameServerID;


  public BasicPacketWithReturnAddressAndNs(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
    super(json);
    this.nameServerID = json.has(NAMESERVER_ID) ? unstringer.valueOf(json.getString(NAMESERVER_ID)) : null;
  }
  

  public BasicPacketWithReturnAddressAndNs(NodeIDType nameServerID, InetSocketAddress address) {
    super(address);
    this.nameServerID = nameServerID;
  }
  

  public BasicPacketWithReturnAddressAndNs(NodeIDType nameServerID) {
    super();
    this.nameServerID = nameServerID;
  }

  @Override
  public void addToJSONObject(JSONObject json) throws JSONException {
    super.addToJSONObject(json);
    if (nameServerID != null) {
      json.put(NAMESERVER_ID, nameServerID);
    }
  }


  public NodeIDType getNameServerID() {
    return nameServerID;
  }


  public void setNameServerID(NodeIDType nameServerID) {
    this.nameServerID = nameServerID;
  }
}
