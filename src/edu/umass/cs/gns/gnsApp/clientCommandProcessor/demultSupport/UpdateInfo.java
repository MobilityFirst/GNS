package edu.umass.cs.gns.gnsApp.clientCommandProcessor.demultSupport;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.gnsApp.packet.AddRecordPacket;
import edu.umass.cs.gns.gnsApp.packet.BasicPacket;
import edu.umass.cs.gns.gnsApp.packet.ConfirmUpdatePacket;
import static edu.umass.cs.gns.gnsApp.packet.Packet.PacketType.*;
import edu.umass.cs.gns.gnsApp.packet.RemoveRecordPacket;
import edu.umass.cs.gns.gnsApp.packet.UpdatePacket;
import edu.umass.cs.gns.gnsApp.NSResponseCode;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This class stores information not just update requests, but also
 * add and remove requests transmitted by the local name
 * server. Extends {@link edu.umass.cs.gns.gnsApp.clientCommandProcessor.demultSupport.RequestInfo}.
 *
 * @author abhigyan
 * @param <NodeIDType>
 */
public class UpdateInfo<NodeIDType> extends RequestInfo {

  /**
   * ID of the name server where update was sent *
   */
  private NodeIDType nameserverID;

  private final BasicPacket basicPacket;
  
  private final ClientRequestHandlerInterface handler;

  /**
   *
   * @param lnsRequestID
   * @param name
   * @param nameserverId
   * @param packet
   * @param handler
   */
  public UpdateInfo(int lnsRequestID, String name, NodeIDType nameserverId, BasicPacket packet, ClientRequestHandlerInterface handler) {
    this.ccpReqID = lnsRequestID;
    this.name = name;
    this.startTime = System.currentTimeMillis();
    this.nameserverID = nameserverId;
    this.numLookupActives = 0;
    this.basicPacket = packet;
    this.requestType = packet.getType();
    this.handler = handler;
  }

  /**
   * Set the name server id.
   * 
   * @param nameserverID
   */
  public synchronized void setNameserverID(NodeIDType nameserverID) {
    this.nameserverID = nameserverID;
  }

  @Override
  public synchronized JSONObject getErrorMessage() {
    return getErrorMessage(NSResponseCode.FAIL_ACTIVE_NAMESERVER);
  }
  
  /**
   * Return the error packet.
   * 
   * @param errorCode
   * @return a JSON error packet
   */
  @SuppressWarnings("unchecked")
  public JSONObject getErrorMessage(NSResponseCode errorCode) {
    ConfirmUpdatePacket<String> confirm = null;
    switch (basicPacket.getType()) {
      case ADD_RECORD:
        confirm = new ConfirmUpdatePacket<String>(errorCode, (AddRecordPacket<String>) basicPacket);
        break;
      case REMOVE_RECORD:
        confirm = new ConfirmUpdatePacket<String>(errorCode, (RemoveRecordPacket<String>) basicPacket);
        break;
      case UPDATE:
        confirm = ConfirmUpdatePacket.createFailPacket((UpdatePacket<String>) basicPacket, errorCode);
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
   * Return the update packet.
   * 
   * @return the basicPacket
   */
  public synchronized BasicPacket getUpdatePacket() {
    return basicPacket;
  }

}
