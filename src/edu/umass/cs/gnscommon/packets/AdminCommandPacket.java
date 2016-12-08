package edu.umass.cs.gnscommon.packets;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsserver.gnsapp.packet.Packet.PacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/**
 *
 * @author westy
 */
public class AdminCommandPacket extends CommandPacket {

  /**
   *
   * @param requestId
   * @param command
   */
  public AdminCommandPacket(long requestId, JSONObject command) {
    super(requestId, command);
    this.setType(PacketType.ADMIN_COMMAND);
    assert (this.getCommandType().isMutualAuth());
  }

  /**
   *
   * @param json
   * @throws JSONException
   */
  public AdminCommandPacket(JSONObject json) throws JSONException {
    super(json);
    this.setType(PacketType.ADMIN_COMMAND);
    assert (this.getCommandType().isMutualAuth());
  }

  /**
   *
   * @param bytes
   * @throws RequestParseException
   */
  public AdminCommandPacket(byte[] bytes) throws RequestParseException {
    super(bytes);
    this.setType(PacketType.ADMIN_COMMAND);
    assert (this.getCommandType().isMutualAuth());
  }

  /**
   * Checks that the command type of the packet is MUTUAL_AUTH as anything else should be a basic CommandPacket instead.
   * This being a separate method allows AdminCommandPacket to override it to change its validation while still reusing the constructor code here.
   */
  @Override
  protected void validateCommandType() {
    assert (this.getCommandType().isMutualAuth());
  }
}
