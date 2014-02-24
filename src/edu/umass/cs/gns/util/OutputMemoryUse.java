package edu.umass.cs.gns.util;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.NameServer;

import java.util.TimerTask;

/**
 * Created by abhigyan on 2/23/14.
 */
public class OutputMemoryUse extends TimerTask {

  int count = 0;

  @Override
  public void run() {
    count++;
    outputMemoryUse(Integer.toString(count) + "sec ");

    GNS.getLogger().info("\tTasksSubmitted\t" + NameServer.executorService.getTaskCount() + "\tTasksCompleted\t"
            + NameServer.executorService.getCompletedTaskCount());

    // this code was used
  }

  public static void outputMemoryUse(String desc) {
    long maxHeapSize = Runtime.getRuntime().maxMemory()/1024/1024;
    long heapSize = Runtime.getRuntime().totalMemory()/1024/1024;
    long freeMem = Runtime.getRuntime().freeMemory()/1024/1024;
    long usedHeap = heapSize - freeMem;
    GNS.getLogger().severe(desc + "\tHeap = " + heapSize + " MB\tMaxHeap = " + maxHeapSize + " MB\tFree = " + freeMem +
            " MB\tUsed = " + usedHeap +" MB");
  }

}