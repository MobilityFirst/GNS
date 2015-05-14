package edu.umass.cs.gns.clientCommandProcessor;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.packet.AddRecordPacket;
import edu.umass.cs.gns.nsdesign.packet.BasicPacket;
import edu.umass.cs.gns.nsdesign.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import static edu.umass.cs.gns.nsdesign.packet.Packet.PacketType.*;
import edu.umass.cs.gns.nsdesign.packet.RemoveRecordPacket;
import edu.umass.cs.gns.nsdesign.packet.UpdatePacket;
import edu.umass.cs.gns.util.NSResponseCode;
import java.net.InetSocketAddress;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This class stores information not just update requests, but also
 * add and remove requests transmitted by the local name
 * server. Extends {@link edu.umass.cs.gns.clientCommandProcessor.RequestInfo}.
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
    this.cppReqID = lnsRequestID;
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
  public synchronized String getLogString() {
    String success = isSuccess() ? "Success" : "Failed";
    if (requestType.equals(Packet.PacketType.ADD_RECORD)) {
      success += "-Add";
    } else if (requestType.equals(Packet.PacketType.REMOVE_RECORD)) {
      success += "-Remove";
    } else if (requestType.equals(Packet.PacketType.UPDATE)) {
      success += "-Update";
    }
    return getFinalString(success, name, getResponseLatency(), 0, nameserverID, handler.getNodeAddress(),
            getCPPReqID(), numLookupActives, System.currentTimeMillis()
            //, getEventCodesString()
    );
  }

  private String getFinalString(String queryStatus, String name, long latency, int numTransmissions,
          NodeIDType nameServerID, InetSocketAddress address, int requestID, int numInvalidActiveError,
          long curTime 
          //,String eventCodes
  ) {
    return queryStatus + "\t" + name + "\t" + latency + "\t" + numTransmissions + "\t" 
            + (nameServerID != null ? nameServerID.toString() : "LNS") + "\t"
            + address.toString() + "\t" + requestID + "\t" + numInvalidActiveError + "\t" + curTime + "\t"
            //+ eventCodes
            ;
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
