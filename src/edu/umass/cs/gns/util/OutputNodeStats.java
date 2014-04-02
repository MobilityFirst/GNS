package edu.umass.cs.gns.util;

import edu.umass.cs.gns.main.GNS;

import java.util.TimerTask;

/**
 * Created by abhigyan on 2/23/14.
 */
public class OutputNodeStats extends TimerTask {

  int count = 0;

  @Override
  public void run() {
    count++;
    outputMemoryUse(Integer.toString(count) + "sec ");

//    GNS.getStatLogger().info(new NIOInstrumenter().toString());
//    GNS.getStatLogger().info("\tTasksSubmitted\t" + NameServer.getExecutorService().getTaskCount() + "\tTasksCompleted\t"
//            + NameServer.getExecutorService().getCompletedTaskCount());

  }

  public static void outputMemoryUse(String desc) {
    long maxHeapSize = Runtime.getRuntime().maxMemory()/1024/1024;
    long heapSize = Runtime.getRuntime().totalMemory()/1024/1024;
    long freeMem = Runtime.getRuntime().freeMemory()/1024/1024;
    long usedHeap = heapSize - freeMem;
    GNS.getStatLogger().info(desc + "\tHeap = " + heapSize + " MB\tMaxHeap = " + maxHeapSize + " MB\tFree = " + freeMem +
            " MB\tUsed = " + usedHeap +" MB");
  }

}