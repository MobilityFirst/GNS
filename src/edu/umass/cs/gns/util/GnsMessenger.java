package edu.umass.cs.gns.util;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.GNSNIOTransport;
import edu.umass.cs.gns.nio.GNSNIOTransportInterface;
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
public class GnsMessenger implements GNSNIOTransportInterface {
  private static final long RTX_DELAY = 1000; //ms
  private static final int BACKOFF_FACTOR=2;

  private final GNSNIOTransport gnsnioTransport;
  private final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;
  private final int myID;

  public GnsMessenger(int myID, GNSNIOTransport gnsnioTransport, ScheduledThreadPoolExecutor scheduledThreadPoolExecutor) {
    this.myID = myID;
    this.scheduledThreadPoolExecutor = scheduledThreadPoolExecutor;
    this.gnsnioTransport = gnsnioTransport;
  }

  @Override
  public int sendToID(int id, JSONObject jsonData) throws IOException {
    int sent = gnsnioTransport.sendToID(id, jsonData);
    if (sent < jsonData.length()) {
          Retransmitter rtxTask = new Retransmitter(id, jsonData, RTX_DELAY);
          scheduledThreadPoolExecutor.schedule(rtxTask, RTX_DELAY, TimeUnit.MILLISECONDS); // can't block, so ignore returned future
    }
    return jsonData.length();
  }

  @Override
  public int sendToAddress(InetSocketAddress isa, JSONObject jsonData) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  /* We need this because NIO may drop messages when congested. Thankfully,
   * it tells us when it does that. The task below exponentially backs
   * off with each retransmission. We are probably doomed anyway if this
   * class is invoked except rarely.
   */
  private class Retransmitter implements Runnable {
    private final int dest;
    private final JSONObject msg;
    private final long delay;
    Retransmitter(int id, JSONObject m, long d) {
      this.dest=id;
      this.msg=m;
      this.delay=d;
    }
    public void run() {
      int sent=0;
      try {
        sent = gnsnioTransport.sendToID(dest, msg);
      } catch(IOException ioe) {
        ioe.printStackTrace();
      } finally {
        if(sent < msg.length() && sent!=-1) {
          GNS.getLogger().severe("Node " + myID + "->" + dest + " messenger backing off under severe congestion, Hail Mary!");
          Retransmitter rtx = new Retransmitter(dest, msg, delay*BACKOFF_FACTOR);
          scheduledThreadPoolExecutor.schedule(rtx, delay*BACKOFF_FACTOR, TimeUnit.MILLISECONDS);
        } else if(sent==-1) { // have to give up at this point
          GNS.getLogger().severe("Node "+myID +"->"+dest+" messenger dropping message as destination unreachable: " + msg);
        }
      }
    }
  }

}
