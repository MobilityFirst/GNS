/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.packet;

import edu.umass.cs.gns.main.GNS;
import org.json.JSONException;
/**************** FIXME Package deprecated by nsdesign/packet. this will soon be deleted. **/
/**
 * Provides the basics for Packets including a type field.
 * 
 * @author westy
 * @deprecated
 */
public abstract class BasicPacket implements PacketInterface {

  /**
   * The packet type *
   */
  protected Packet.PacketType type;

  @Override
  public String toString() {
    try {
      return this.toJSONObject().toString();
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem converting packet to string:" + e);
      return "BasicPacket{" + "type=" + getType() + '}';
    }
  }

  /**
   * @return the type
   */
  public Packet.PacketType getType() {
    return type;
  }

  /**
   * @param type the type to set
   */
  public void setType(Packet.PacketType type) {
    this.type = type;
  }
}
