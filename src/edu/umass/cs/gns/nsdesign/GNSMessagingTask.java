package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.nio.GNSNIOTransportInterface;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;



/**
 * Adapted MessagingTask in multipaxos pacakge for use in GNS.
 @author V. Arun
 */

/* This messaging task means that each msg needs
 * to be sent to all recipients. Note: it does
 * *NOT* mean that each msg needs to be sent to
 * its corresponding recipient; in fact, the
 * sizes of the two arrays, recipients and msgs,
 * may be different.
 */
public class GNSMessagingTask {

  public final int[] recipients;
  public final JSONObject[] msgs;

  // Unicast
  public GNSMessagingTask(int destID, JSONObject pkt) {
    assert(pkt!=null) : "Incorrect usage: MessagingTask can not be instantiated with no messages";
    this.recipients = new int[1];
    this.recipients[0] = destID;

    this.msgs = new JSONObject[1];
    msgs[0] = pkt;
  }
  // Multicast
  public GNSMessagingTask(int[] destIDs, JSONObject pkt) {
    assert(pkt!=null) : "Incorrect usage: MessagingTask can not be instantiated with no messages";
    this.recipients = destIDs;
    this.msgs = new JSONObject[1];
    msgs[0] = pkt;
  }
  // Unicast multiple packets
  public GNSMessagingTask(int destID, JSONObject[] pkts) {
    assert(pkts.length>0 && pkts[0]!=null) : "Incorrect usage: MessagingTask can not be instantiated with no messages";

    this.recipients = new int[1];
    this.recipients[0] = destID;
    this.msgs = pkts;
  }
  // Multicast multiple packets
  public GNSMessagingTask(int[] destIDs, JSONObject[] pkts) {
    assert(pkts.length>0 && pkts[0]!=null) : "Incorrect usage: MessagingTask can not be instantiated with no messages";

    this.recipients = destIDs;
    this.msgs = pkts;
  }

  public GNSMessagingTask(int[] destIDs, ArrayList<JSONObject> pkts) {
    assert(pkts.size()>0 && pkts.get(0)!=null) : "Incorrect usage: MessagingTask can not be instantiated with no messages";

    this.recipients = destIDs;
    msgs = new JSONObject[pkts.size()];
    for(int i=0; i<msgs.length; i++) {
      msgs[i++] = pkts.get(i);
    }
  }


  public String toString() {
    if(msgs.length==0) return "NULL";
    String s = msgs[0].toString();
    s+= ": Recipients: [";
    for(int i=0;i<this.recipients.length; i++) {
      s += this.recipients[i]+" ";
    }
    s += "] , ";
    for(int i=0; i<msgs.length; i++) {
      s += msgs[i];
    }
    return s;
  }

  /**
   * Send GNSMessagingTask using GNSNIOTransport.
   */
  public static void send(GNSMessagingTask mtask, GNSNIOTransportInterface nioTransport) throws JSONException, IOException {
    if(mtask==null) return;
    for(int m=0; m<mtask.msgs.length; m++) {
      for(int r=0; r<mtask.recipients.length; r++) {
        JSONObject jsonMsg = mtask.msgs[m];
        nioTransport.sendToID(mtask.recipients[r], jsonMsg);
      }
    }
  }


}
