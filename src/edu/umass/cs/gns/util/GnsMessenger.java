package edu.umass.cs.gns.util;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nio.InterfaceJSONNIOTransport;
import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Based on {@link edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.Messenger} class.
 *
 * This class adds packet retransmission with exponential back-off on top of GNSNIOTransport.
 * This is useful in a case of brief request burst when GNSNIOTransport drops packets due to
 * its message queue being full.
 *
 * Created by abhigyan on 5/3/14.
 */
public class GnsMessenger implements InterfaceJSONNIOTransport {

  private static final long RTX_DELAY = 1000; //ms
  private static final int BACKOFF_FACTOR = 2;

  private final InterfaceJSONNIOTransport<NodeId<String>> gnsnioTransport;
  private final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;
  private final NodeId<String> myID;

  public GnsMessenger(NodeId<String> myID, InterfaceJSONNIOTransport<NodeId<String>> gnsnioTransport, ScheduledThreadPoolExecutor scheduledThreadPoolExecutor) {
    this.myID = myID;
    this.scheduledThreadPoolExecutor = scheduledThreadPoolExecutor;
    this.gnsnioTransport = gnsnioTransport;
  }

  @Override
  public NodeId<String> getMyID() {
    return this.myID;
  }

  @Override
  public void stop() {
    gnsnioTransport.stop();
  }

  public int sendToID(NodeId<String> id, JSONObject jsonData) throws IOException {
    int sent = gnsnioTransport.sendToID(id, jsonData);
    if (sent < jsonData.length()) {
      Retransmitter rtxTask = new Retransmitter(id, jsonData, RTX_DELAY);
      scheduledThreadPoolExecutor.schedule(rtxTask, RTX_DELAY, TimeUnit.MILLISECONDS); // can't block, so ignore returned future
    }
    return jsonData.length();
  }

  @Override
  public int sendToAddress(InetSocketAddress isa, JSONObject jsonData) throws IOException {
    int sent = gnsnioTransport.sendToAddress(isa, jsonData);
    if (sent < jsonData.length()) {
      Retransmitter rtxTask = new Retransmitter(isa, jsonData, RTX_DELAY);
      scheduledThreadPoolExecutor.schedule(rtxTask, RTX_DELAY, TimeUnit.MILLISECONDS); // can't block, so ignore returned future
    }
    return jsonData.length();
  }

  @Override
  public int sendToID(Object id, JSONObject jsonData) throws IOException {
    // FIXME
    if (id instanceof NodeId) {
      return sendToID((NodeId) id, jsonData);
    } else {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  @Override
  public void addPacketDemultiplexer(AbstractPacketDemultiplexer pd) {
    //To change body of generated methods, choose Tools | Templates.
  }

  /* We need this because NIO may drop messages when congested. Thankfully,
   * it tells us when it does that. The task below exponentially backs
   * off with each retransmission. We are probably doomed anyway if this
   * class is invoked except rarely.
   */
  private class Retransmitter implements Runnable {

    // One of these next two will be non-null:
    private final NodeId<String> destID;
    private final InetSocketAddress destAddress;
    //
    private final JSONObject msg;
    private final long delay;

    Retransmitter(NodeId<String> destId, JSONObject m, long d) {
      this.destID = destId;
      this.destAddress = null;
      this.msg = m;
      this.delay = d;
    }

    Retransmitter(InetSocketAddress dest, JSONObject m, long d) {
      this.destID = null;
      this.destAddress = dest;
      this.msg = m;
      this.delay = d;
    }

    private Object getDest() {
      if (destID != null) {
        return destID;
      } else {
        return destAddress;
      }
    }

    @Override
    public void run() {
      int sent = 0;
      try {
        if (destID != null) {
          sent = gnsnioTransport.sendToID(destID, msg);
        } else {
          sent = gnsnioTransport.sendToAddress(destAddress, msg);
        }
      } catch (IOException ioe) {
        ioe.printStackTrace();
      } finally {
        if (sent < msg.length() && sent != -1) {
          GNS.getLogger().severe("Node " + myID + "->" + getDest() + " messenger backing off under severe congestion, Hail Mary!");
          Retransmitter rtx;
          if (destID != null) {
            rtx = new Retransmitter(destID, msg, delay * BACKOFF_FACTOR);
          } else {
            rtx = new Retransmitter(destAddress, msg, delay * BACKOFF_FACTOR);
          }
          scheduledThreadPoolExecutor.schedule(rtx, delay * BACKOFF_FACTOR, TimeUnit.MILLISECONDS);
        } else if (sent == -1) { // have to give up at this point
          GNS.getLogger().severe("Node " + myID + "->" + getDest() + " messenger dropping message as destination unreachable: " + msg);
        }
      }
    }
  }

}
