package edu.umass.cs.gns.packet;

import edu.umass.cs.gns.client.Intercessor;
import edu.umass.cs.gns.packet.Packet.PacketType;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This class implements the packet that confirms update, add and remove transactions. The packet is transmitted from an
 * active name server to a local name server that had originally sent the address update. Also used to send
 * confirmation back to theoriginal client ({@link Intercessor} is one example).
 *
 * Name server sets the <code>requestID</code> and <code>LNSRequestID</code> field based on the original request,
 * which could be update, add and remove. If this fields are not set correctly, request will not be routed back to
 * client.
 *
 */
public class ConfirmUpdateLNSPacket extends BasicPacket {

  private final static String REQUESTID = "reqid";
  private final static String LNSREQUESTID = "lnreqsid";
  private final static String SUCCESS = "success";

  /**
   * Unique identifier used by the entity making the initial request to confirm
   */
  private int requestID;
  /**
   * The ID the LNS uses to for bookkeeping
   */
  private int LNSRequestID;

  /**
   * indicates success or failure of operation
   */
  private boolean success;


  /**
   * Constructs a new ConfirmUpdatePacket with the given parameters.
   *
   * @param type Type of this packet
   * @param requestID Id of local or name server
   */
  public ConfirmUpdateLNSPacket(PacketType type, int requestID, int LNSRequestID, boolean success) {
    this.type = type;
    this.success = success;
    this.requestID = requestID;
    this.LNSRequestID = LNSRequestID;
  }

  /**
   * Given an <code>UpdateAddressPacket</code>, create a <code>ConfirmUpdateLNSPacket</code> indicating that
   * the update is failed.
   *
   * @param updatePacket
   * @return
   */
  public static ConfirmUpdateLNSPacket createFailPacket(UpdateAddressPacket updatePacket) {
    return new ConfirmUpdateLNSPacket(PacketType.CONFIRM_UPDATE_LNS,
            updatePacket.getRequestID(), updatePacket.getLNSRequestID(), false);
  }

  /**
   * Given an <code>UpdateAddressPacket</code>, create a <code>ConfirmUpdateLNSPacket</code> indicating that
   * the update is success.
   *
   * @param updatePacket  <code>UpdateAddressPacket</code> received by name server.
   * @return <code>ConfirmUpdateLNSPacket</code> indicating request failure.
   */
  public static ConfirmUpdateLNSPacket createSuccessPacket(UpdateAddressPacket updatePacket) {
    return new ConfirmUpdateLNSPacket(PacketType.CONFIRM_UPDATE_LNS,
            updatePacket.getRequestID(), updatePacket.getLNSRequestID(), //updatePacket.getName(), updatePacket.getRecordKey(),
            true);
  }

  public void convertToFailPacket() {
    this.success = false;
  }

  public ConfirmUpdateLNSPacket(boolean success, AddRecordPacket packet) {
    this(Packet.PacketType.CONFIRM_ADD_LNS, packet.getRequestID(),packet.getLNSRequestID(), success);

  }

  public ConfirmUpdateLNSPacket(boolean success, RemoveRecordPacket packet) {
    this(Packet.PacketType.CONFIRM_REMOVE_LNS,  packet.getRequestID(), packet.getLNSRequestID(),success);
  }

  /**
   * Constructs a new ConfirmUpdatePacket from a JSONObject.
   *
   * @param json JSONObject that represents ConfirmUpdatePacket.
   * @throws JSONException
   */
  public ConfirmUpdateLNSPacket(JSONObject json) throws JSONException {
    this.type = Packet.getPacketType(json);
    this.requestID = json.getInt(REQUESTID);
    this.LNSRequestID = json.getInt(LNSREQUESTID);
    this.success = json.getBoolean(SUCCESS);
  }

  /**
   * Converts a ConfirmUpdatePacket to a JSONObject
   *
   * @return JSONObject that represents ConfirmUpdatePacket
   * @throws JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(REQUESTID, getRequestID());
    json.put(LNSREQUESTID, getLNSRequestID());
    json.put(SUCCESS, isSuccess());

    return json;
  }


  public int getRequestID() {
    return requestID;
  }

  public int getLNSRequestID() {
    return LNSRequestID;
  }

  public boolean isSuccess() {
    return success;
  }

}
