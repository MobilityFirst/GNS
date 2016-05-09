package edu.umass.cs.gnsclient.client.testing;

import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.GNSClientCommands;

/**
 * @author gaozy
 *
 */
public class SingleClient implements Runnable {
  // the total number of requests need to be sent

  private final int numReq;
  private final GuidEntry entry;
  private final GNSClientCommands client;
  private final boolean malicious;

  /**
   * @param client
   * @param entry
   * @param malicious
   */
  public SingleClient(GNSClientCommands client, GuidEntry entry, boolean malicious) {
    this.client = client;
    this.entry = entry;
    this.malicious = malicious;
    if (malicious) {
      this.numReq = MessageStats.DURATION * 1000 / MessageStats.MAL_INTERVAL / MessageStats.DEPTH;
    } else {
      this.numReq = MessageStats.DURATION * 1000 / MessageStats.INTERVAL;
    }

    System.out.println("Start Sending " + numReq + " requests with entry " + entry);
  }

  @Override
  public void run() {
    for (int i = 0; i < numReq; i++) {
      long start = System.nanoTime();
      try {
        client.fieldRead(entry, "nextGuid");
        //System.out.println("query result is "+result);
      } catch (Exception e) {
        //swallow it 
        //e.printStackTrace();
      }
      long eclapsed = System.nanoTime() - start;

      if (malicious) {
        MessageStats.mal_request.add(eclapsed);
        System.out.println(entry.getGuid());
      } else {
        MessageStats.latency.add(eclapsed);
      }

    }
  }
}
