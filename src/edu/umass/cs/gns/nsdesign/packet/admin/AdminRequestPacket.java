package edu.umass.cs.gns.nsdesign.packet.admin;

import edu.umass.cs.gns.nsdesign.packet.BasicPacketWithLnsAddress;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.packet.Packet.PacketType;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * This class implements the packet transmitted to send an admin request.
 *
 * @author Westy
 */
public class AdminRequestPacket extends BasicPacketWithLnsAddress {

  public enum AdminOperation {

    DELETEALLRECORDS, // calls remove record on every record
    RESETDB, // clears the database and reinitializes all indices
    CLEARCACHE,
    DUMPCACHE,
    CHANGELOGLEVEL,
    PINGTABLE,
    PINGVALUE;
  };
  public final static String ID = "id";
  private final static String OPERATION = "operation";
  private final static String ARGUMENT = "arg";
  private final static String ARGUMENT2 = "arg2";
  //
  private int id;
  private AdminOperation operation;
  private String argument;
  private String argument2;

  /**
   *
   * Constructs new AdminRequestPacket with the given parameter.
   *
   * @param operation
   */
  public AdminRequestPacket(AdminOperation operation) {
    this(0, operation, null, null);
  }

  /**
   * Constructs new AdminRequestPacket.
   *
   * @param id
   * @param operation
   */
  public AdminRequestPacket(int id, AdminOperation operation) {
    this(id, operation, null, null);
  }

  /**
   * Constructs new AdminRequestPacket.
   *
   * @param id
   * @param operation
   * @param argument
   */
  public AdminRequestPacket(int id, AdminOperation operation, String argument) {
    this(id, operation, argument, null);
  }

  /**
   * Constructs new AdminRequestPacket.
   *
   * @param operation
   * @param argument
   */
  public AdminRequestPacket(AdminOperation operation, String argument) {
    this(0, operation, argument, null);
  }

  /**
   * Constructs new AdminRequestPacket.
   *
   * @param operation
   * @param argument
   * @param argument2
   */
  public AdminRequestPacket(AdminOperation operation, String argument, String argument2) {
    this(0, operation, argument, argument2);
  }

  /**
   * Constructs new AdminRequestPacket.
   *
   * @param id
   * @param operation
   * @param argument
   * @param argument2
   */
  public AdminRequestPacket(int id, AdminOperation operation, String argument, String argument2) {
    super(null); // implies that it came from a client, not an NS or LNS
    this.type = PacketType.ADMIN_REQUEST;
    this.id = id;
    this.operation = operation;
    this.argument = argument;
    this.argument2 = argument2;
  }

  /**
   *
   * Constructs new AdminRequestPacket from a JSONObject
   *
   * @param json JSONObject representing this packet
   * @throws org.json.JSONException
   */
  public AdminRequestPacket(JSONObject json) throws JSONException {
    super(json.optString(LNS_ADDRESS, null), json.optInt(LNS_PORT, INVALID_PORT));
    if (Packet.getPacketType(json) != PacketType.ADMIN_REQUEST) {
      Exception e = new Exception("AdminRequestPacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
      return;
    }

    this.type = Packet.getPacketType(json);
    this.id = json.getInt(ID);
//    this.lnsAddress = json.has(LNS_ADDRESS) && json.has(LNS_PORT)
//            ? new InetSocketAddress(json.getString(LNS_ADDRESS), json.getInt(LNS_PORT))
//            : null;
    //this.localNameServerId = json.getInt(LNSID);
    this.operation = AdminOperation.valueOf(json.getString(OPERATION));
    this.argument = json.optString(ARGUMENT, null);
    this.argument2 = json.optString(ARGUMENT2, null);
  }

  /**
   *
   * Converts a AdminRequestPacket to a JSONObject.
   *
   * @return JSONObject representing this packet.
   * @throws org.json.JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    super.addToJSONObject(json);
    json.put(ID, id);
//    if (lnsAddress != null) {
//      json.put(LNS_ADDRESS, lnsAddress.getHostString());
//      json.put(LNS_PORT, lnsAddress.getPort());
//    }
    //json.put(LNSID, localNameServerId);
    json.put(OPERATION, getOperation().name());
    if (this.argument != null) {
      json.put(ARGUMENT, argument);
    }
    if (this.argument2 != null) {
      json.put(ARGUMENT2, argument2);
    }
    return json;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

//  public int getLocalNameServerId() {
//    return localNameServerId;
//  }
//
//  public void setLocalNameServerId(int lnsid) {
//    this.localNameServerId = lnsid;
//  }
  
//  /** 
//   * Gets the lns return address. If thi
//   * @return 
//   */
//  public InetSocketAddress getLnsAddress() {
//    return lnsAddress;
//  }
//
//  public void setLnsAddress(InetSocketAddress lnsAddress) {
//    this.lnsAddress = lnsAddress;
//  }

  public AdminOperation getOperation() {
    return operation;
  }

  public String getArgument() {
    return argument;
  }

  public String getArgument2() {
    return argument2;
  }

}
