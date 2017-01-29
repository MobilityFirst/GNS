
package edu.umass.cs.gnsserver.gnsapp.packet.admin;

import edu.umass.cs.gnsserver.gnsapp.packet.BasicPacketWithReturnAddress;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet.PacketType;
import org.json.JSONException;
import org.json.JSONObject;


public class AdminRequestPacket extends BasicPacketWithReturnAddress {


  public enum AdminOperation {


    DELETEALLRECORDS, 

    RESETDB, // 

    CLEARCACHE,

    DUMPCACHE,

    CHANGELOGLEVEL,
  };

  private final static String ID = "id";
  private final static String OPERATION = "operation";
  private final static String ARGUMENT = "arg";
  private final static String ARGUMENT2 = "arg2";
  //
  private int id;
  private AdminOperation operation;
  private String argument;
  private String argument2;


  public AdminRequestPacket(AdminOperation operation) {
    this(0, operation, null, null);
  }


  public AdminRequestPacket(int id, AdminOperation operation) {
    this(id, operation, null, null);
  }


  public AdminRequestPacket(int id, AdminOperation operation, String argument) {
    this(id, operation, argument, null);
  }


  public AdminRequestPacket(AdminOperation operation, String argument) {
    this(0, operation, argument, null);
  }


  public AdminRequestPacket(AdminOperation operation, String argument, String argument2) {
    this(0, operation, argument, argument2);
  }


  public AdminRequestPacket(int id, AdminOperation operation, String argument, String argument2) {
    super(); // implies that it came from a client, not an NS or LNS
    this.type = PacketType.ADMIN_REQUEST;
    this.id = id;
    this.operation = operation;
    this.argument = argument;
    this.argument2 = argument2;
  }


  public AdminRequestPacket(JSONObject json) throws JSONException {
    super(json);
    //super(json.optString(CCP_ADDRESS, null), json.optInt(CCP_PORT, INVALID_PORT));
    if (Packet.getPacketType(json) != PacketType.ADMIN_REQUEST) {
      Exception e = new Exception("AdminRequestPacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
      return;
    }

    this.type = Packet.getPacketType(json);
    this.id = json.getInt(ID);
//    this.lnsAddress = json.has(CCP_ADDRESS) && json.has(CCP_PORT)
//            ? new InetSocketAddress(json.getString(CCP_ADDRESS), json.getInt(CCP_PORT))
//            : null;
    //this.localNameServerId = json.getInt(LNSID);
    this.operation = AdminOperation.valueOf(json.getString(OPERATION));
    this.argument = json.optString(ARGUMENT, null);
    this.argument2 = json.optString(ARGUMENT2, null);
  }


  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    super.addToJSONObject(json);
    json.put(ID, id);
//    if (lnsAddress != null) {
//      json.put(CCP_ADDRESS, lnsAddress.getHostString());
//      json.put(CCP_PORT, lnsAddress.getPort());
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
