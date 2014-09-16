package edu.umass.cs.gns.protocoltask.examples.pingpong;

import java.util.Set;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.MessagingTask;
import edu.umass.cs.gns.nio.NIOTransport;
import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.protocoltask.ProtocolEvent;
import edu.umass.cs.gns.protocoltask.ProtocolExecutor;
import edu.umass.cs.gns.protocoltask.ProtocolTask;
import edu.umass.cs.gns.protocoltask.examples.PingPongPacket;
import edu.umass.cs.gns.protocoltask.examples.PingPongServer;
import edu.umass.cs.gns.protocoltask.json.ProtocolPacket;
import edu.umass.cs.gns.util.Util;

/**
 * @author V. Arun
 */

/*
 * This example waits for numPings ping-pongs form any node in a
 * specified set of nodes. Note that it does not wait for numPings
 * ping-pongs from *all* nodes in the set.
 */
public class PingPongProtocolTask extends PingPongServer {

  public static final int MAX_PINGS = 10;

  private Long key = null;

  private final NodeId<String>[] nodes;
  private final int numPings;

  private Logger log
          = NIOTransport.LOCAL_LOGGER ? Logger.getLogger(getClass().getName())
          : GNS.getLogger();

  public PingPongProtocolTask(NodeId<String> id, Set<NodeId<String>> nodes, int numPings) {
    super(id);
    this.nodes = Util.setToNodeIdArray(nodes);
    this.numPings = numPings;
    log.info("Node" + myID + " constructing protocol task with nodeIDs "
            + nodes);
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
    // FIXME
    return (this.key = ((long) this.myID.hashCode()) << 32 + (int) (Math.random() * Integer.MAX_VALUE));
  }

  @Override
  public MessagingTask[] handleEvent(
          ProtocolEvent<Packet.PacketType, Long> event,
          ProtocolTask<NodeId<String>, Packet.PacketType, Long>[] ptasks) {

    JSONObject msg = null;
    try {
      msg = ((ProtocolPacket) event.getMessage()).toJSONObject();
    } catch (JSONException je) {
      je.printStackTrace();
      return null;
    }
    MessagingTask mtask = null;
    try {
      switch (Packet.getPacketType(msg)) {
        case TEST_PONG:
          mtask = handlePingPong(new PingPongPacket(msg));
          break;
        default:
          throw new RuntimeException("Unrecognizable message type: "
                  + Packet.getPacketType(msg));
      }
    } catch (JSONException je) {
      je.printStackTrace();
    }
    return mtask != null ? mtask.toArray() : null;
  }

  @Override
  public MessagingTask[] start() {
    PingPongPacket ppp
            = new PingPongPacket(this.myID, this.myID,
                    Packet.PacketType.TEST_PING);
    // ppp.setKey(getKey()); // automatically done by ProtocolExecutor
    log.info("Node" + myID + " starting protocol task with nodeIDs "
            + Util.arrayOfNodeIdsToString(nodes));
    return new MessagingTask(nodes, ppp).toArray();
  }

  /**
   * ************************* End of overridden methods ****************************************
   */
  /**
   * ************************* Private or testing methods below ********************************
   */
  private MessagingTask handlePingPong(PingPongPacket ppp) {
    assert (ppp.getInitiator().equals(this.myID));
    return handlePong(ppp);
  }

  private MessagingTask handlePong(PingPongPacket pong) {
    pong.incrCounter();
    NodeId<String> sender = pong.flip(this.myID);
    log.info("Node" + myID + " protocol task received pong from " + sender
            + ": " + pong);
    if (pong.getCounter() >= (this.numPings - 1)) {
      ProtocolExecutor.cancel(this); // throws exception
    }
    return new MessagingTask(sender, pong);
  }

  public static void main(String[] args) {
    System.out.println("No unit test. Run ExampleNode instead.");
  }
}
