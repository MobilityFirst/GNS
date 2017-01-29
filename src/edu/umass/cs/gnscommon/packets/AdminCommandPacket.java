package edu.umass.cs.gnscommon.packets;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsserver.gnsapp.packet.Packet.PacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;


public class AdminCommandPacket extends CommandPacket {


  public AdminCommandPacket(long requestId, JSONObject command) {
    super(requestId, command);
    this.setType(PacketType.ADMIN_COMMAND);
    assert (this.getCommandType().isMutualAuth());
  }


  public AdminCommandPacket(JSONObject json) throws JSONException {
    super(json);
    this.setType(PacketType.ADMIN_COMMAND);
    assert (this.getCommandType().isMutualAuth());
  }


  public AdminCommandPacket(byte[] bytes) throws RequestParseException {
    super(bytes);
    this.setType(PacketType.ADMIN_COMMAND);
    assert (this.getCommandType().isMutualAuth());
  }


  @Override
  protected void validateCommandType() {
    assert (this.getCommandType().isMutualAuth());
  }
}
