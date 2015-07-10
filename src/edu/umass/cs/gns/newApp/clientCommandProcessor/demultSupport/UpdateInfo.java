package edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.newApp.packet.AddRecordPacket;
import edu.umass.cs.gns.newApp.packet.BasicPacket;
import edu.umass.cs.gns.newApp.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.newApp.packet.Packet;
import static edu.umass.cs.gns.newApp.packet.Packet.PacketType.*;
import edu.umass.cs.gns.newApp.packet.RemoveRecordPacket;
import edu.umass.cs.gns.newApp.packet.UpdatePacket;
import edu.umass.cs.gns.util.NSResponseCode;
import java.net.InetSocketAddress;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This class stores information not just update requests, but also
 * add and remove requests transmitted by the local name
 * server. Extends {@link edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.RequestInfo}.
 *
 * @author abhigyan
 */
public class UpdateInfo<NodeIDType> extends RequestInfo {

  /**
   * ID of the name server where update was sent *
   */
  private NodeIDType nameserverID;

  private final BasicPacket basicPacket;
  
  private final ClientRequestHandlerInterface<NodeIDType> handler;

  public UpdateInfo(int lnsRequestID, String name, NodeIDType nameserverId, BasicPacket packet, ClientRequestHandlerInterface<NodeIDType> handler) {
    this.ccpReqID = lnsRequestID;
    this.name = name;
    this.startTime = System.currentTimeMillis();
    this.nameserverID = nameserverId;
    this.numLookupActives = 0;
    this.basicPacket = packet;
    this.requestType = packet.getType();
    this.handler = handler;
  }

  public synchronized void setNameserverID(NodeIDType nameserverID) {
    this.nameserverID = nameserverID;
  }

  @Override
  public synchronized JSONObject getErrorMessage() {
    return getErrorMessage(NSResponseCode.FAIL_ACTIVE_NAMESERVER);
  }
  
  public JSONObject getErrorMessage(NSResponseCode errorCode) {
    ConfirmUpdatePacket confirm = null;
    switch (basicPacket.getType()) {
      case ADD_RECORD:
        confirm = new ConfirmUpdatePacket(errorCode, (AddRecordPacket) basicPacket);
        break;
      case REMOVE_RECORD:
        confirm = new ConfirmUpdatePacket(errorCode, (RemoveRecordPacket) basicPacket);
        break;
      case UPDATE:
        confirm = ConfirmUpdatePacket.createFailPacket((UpdatePacket) basicPacket, errorCode);
        break;
    }
    try {
      return (confirm != null) ? confirm.toJSONObject() : null;
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem creating JSON error object:" + e);
    }
    return null;
  }

  /**
   * @return the basicPacket
   */
  public synchronized BasicPacket getUpdatePacket() {
    return basicPacket;
  }

}
