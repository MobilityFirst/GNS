package edu.umass.cs.gns.protocoltask.examples;

import java.util.logging.Logger;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.MessagingTask;
import edu.umass.cs.gns.nio.NIOTransport;
import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.protocoltask.ProtocolEvent;
import edu.umass.cs.gns.protocoltask.ProtocolTask;
import edu.umass.cs.gns.protocoltask.TESTProtocolTaskConfig;

/**
 * @author V. Arun
 */
public class PingPongServer implements ProtocolTask<NodeId<String>, Packet.PacketType, Long> {

  private final Long key = null;

  protected final NodeId<String> myID;

  private Logger log
          = NIOTransport.LOCAL_LOGGER ? Logger.getLogger(getClass().getName())
          : GNS.getLogger();

  public PingPongServer(NodeId<String> id) {
    this.myID = id;
  }

  /**
   * ************************* Start of overridden methods ****************************************
   */
  @Override
  public Long getKey() {
    return this.key;
  }

  @Override
  public Long refreshKey() {
    // FIXME: WTF?
    return ((long) this.myID.hashCode() << 32)
            + (long) (Math.random() * Integer.MAX_VALUE);
  }

  @Override
  public MessagingTask[] handleEvent(
          ProtocolEvent<Packet.PacketType, Long> event,
          ProtocolTask<NodeId<String>, Packet.PacketType, Long>[] ptasks) {

    PingPongPacket ppp = ((PingPongPacket) event.getMessage());
    MessagingTask mtask = null;
    switch (ppp.getType()) {
      case TEST_PING:
        mtask = handlePingPong(ppp);
        break;
      default:
        throw new RuntimeException("Unrecognizable message");
    }
    return mtask != null ? mtask.toArray() : null;
  }

  @Override
  public MessagingTask[] start() {
    return null;
  }

  /**
   * ************************* End of overridden methods ****************************************
   */
  /**
   * ************************* Private or testing methods below ********************************
   */
  private MessagingTask handlePingPong(PingPongPacket ppp) {
    return handlePing(ppp);
  }

  private MessagingTask handlePing(PingPongPacket ping) {
    NodeId<String> sender = ping.flip(this.myID);
    if (TESTProtocolTaskConfig.shouldDrop(ping.getCounter())) {
      return null;
    }
    log.info("Node" + myID + " pingpong server ponging to " + sender
            + ": " + ping);
    return new MessagingTask(sender, ping);
  }

  /**
   * ********************************** Testing methods below *********************************
   */
  public static void main(String[] args) {
    // Not much to test here as this is an example of how to use protocol task
    PingPongServer pps = new PingPongServer(new NodeId<String>(25));
    long key1 = pps.refreshKey();
    System.out.println("Generated random key1 " + key1);
    long key2 = pps.refreshKey();
    System.out.println("Generated random key2 " + key2);
    assert ((key1 >> 32) == (key2 >> 32)) : "key1, key2 correspond to node IDs "
            + (key1 >> 32) + ", " + (key2 >> 32);
    System.out.println("SUCCESS: Not much to test here.");
  }
}
