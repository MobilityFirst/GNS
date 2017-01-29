
package edu.umass.cs.gnsserver.gnsapp.packet;

import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.nio.interfaces.IntegerPacketType;

import java.util.logging.Level;
import org.json.JSONException;


public abstract class BasicPacket implements PacketInterface, ExtensiblePacketInterface {


  protected Packet.PacketType type;

  


  public Packet.PacketType getType() {
    return type;
  }

  // For InterfaceRequest

  public IntegerPacketType getRequestType() {
    return type;
  }


  public final void setType(Packet.PacketType type) {
    this.type = type;
  }
  
  @Override
  public String toString() {
    try {
      return this.toJSONObject().toString();
    } catch (JSONException e) {
      GNSConfig.getLogger().log(Level.SEVERE, "Problem converting packet to string:{0}", e);
      return "BasicPacket{" + "type=" + getType() + '}';
    }
  }


  public static boolean shortenLongToString = false; // shorten is unused right now


  public String toString(boolean shorten) {
    if (shortenLongToString && shorten) {
      return toReasonableString();
    } else {
      return toString();
    }
  }


  private String toReasonableString() {
    try {
      return this.toJSONObject().toReasonableString();
    } catch (JSONException e) {
      GNSConfig.getLogger().severe("Problem converting packet to string:" + e);
      return "BasicPacket{" + "type=" + getType() + '}';
    }
  }

}
