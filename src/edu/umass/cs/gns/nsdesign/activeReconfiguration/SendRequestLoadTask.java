package edu.umass.cs.gns.nsdesign.activeReconfiguration;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.packet.NameServerLoadPacket;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * Sends request load at this active replica to replica controllers (RCs). RCs consider this
 * load in deciding whether to choose this node as an active replica.
 *
 * Created by abhigyan on 5/20/14.
 */
public class SendRequestLoadTask extends TimerTask{

  // the average outgoing traffic rate (in msgs/sec) sent by this task
  private static final double SEND_RATE = 1.0;

  private final ActiveReplicaApp activeApp;
  private final ActiveReplica activeReplica;

  // number of requests
  private int prevReqCount = 0;

  // number of times task is run
  private int taskRunCount = 0;

  // last time the task was run
  private long prevRunTime = 0;

  public SendRequestLoadTask(ActiveReplicaApp activeApp, ActiveReplica activeReplica) {
    this.activeApp = activeApp;
    this.activeReplica = activeReplica;
    this.taskRunCount += 1;
  }

  public SendRequestLoadTask(ActiveReplicaApp activeApp, ActiveReplica activeReplica,
                             int prevReqCount, int taskRunCount, long prevRunTime) {
    this.activeApp = activeApp;
    this.activeReplica = activeReplica;
    this.prevReqCount = prevReqCount;
    this.taskRunCount = taskRunCount;
    this.prevRunTime = prevRunTime;
  }

  @Override
  public void run() {

    long curTime = System.currentTimeMillis();
    int curReqCount = activeApp.getRequestCount();

    if (taskRunCount > 0) {
      double reqRate = (curReqCount - prevReqCount) * 1000.0 / (curTime - prevRunTime);
      GNS.getStatLogger().info("\tRequestRate\tnode\t" + activeReplica.getNodeID() + "\treqRate\t"
              + reqRate + "\t");
      NameServerLoadPacket nsLoad = new NameServerLoadPacket(activeReplica.getNodeID(), -1, reqRate);

      try {
        JSONObject sendJson = nsLoad.toJSONObject();
        for (int nsID: activeReplica.getGnsNodeConfig().getNodeIDs()) {
          try {
            activeReplica.getNioServer().sendToID(nsID, sendJson);
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      } catch (JSONException e) {
        e.printStackTrace();
      }
    }

    this.prevRunTime = curTime;
    this.prevReqCount = curReqCount;
    taskRunCount += 1;

    double nextRunDelaySec = activeReplica.getGnsNodeConfig().getNumberOfNodes()/SEND_RATE;
    if (Config.debuggingEnabled) GNS.getLogger().fine("Next run delay " + nextRunDelaySec + " sec");
    SendRequestLoadTask task = new SendRequestLoadTask(activeApp, activeReplica, prevReqCount, taskRunCount, prevRunTime);
    activeReplica.getScheduledThreadPoolExecutor().schedule(task, (int) nextRunDelaySec + 1, TimeUnit.SECONDS);
  }
}
