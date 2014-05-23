package edu.umass.cs.gns.test.connecttime;

import edu.umass.cs.gns.clientsupport.Intercessor;
import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.util.NameRecordKey;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.Util;
import edu.umass.cs.gns.workloads.ExponentialDistribution;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.TimerTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Represents a mobile device which sends address updates to GNS. It listens on a given port to send its current
 * address to correspondent.
 * Created by abhigyan on 5/13/14.
 */
public class Mobile implements Runnable{

  private ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(10);

  private double updateIntervalSec = 10.0;  // this should be configurable

  private double maxExpDuration = 3600.0;  // this should be configurable

  private int port = 23498;

  private String name = "test_name";

  private String key = NameRecordKey.EdgeRecord.getName();

  private String currentValue;

  public Mobile(double updateIntervalSec){
    this.updateIntervalSec = updateIntervalSec;
  }

  private synchronized String getCurrentValue() {
    return currentValue;
  }

  private synchronized void setCurrentValue(String newValue) {
    this.currentValue = newValue;
  }

  private ResultValue getResultValue(String s){
    ResultValue value = new ResultValue();
    value.add(s);
    return value;
  }

  @Override
  public void run() {
    long t0 = System.currentTimeMillis();
    String initialValue = "ABCD";
    setCurrentValue(initialValue);
    NSResponseCode response = Intercessor.sendAddRecord(name, key, getResultValue(getCurrentValue()));
    assert response == NSResponseCode.NO_ERROR: "Error in adding record";

    startListeningThread();

    // Main loop:
    // do an add
    // wait
    // send update at a rate (1/updateIntervalSec)
    ExponentialDistribution exp = new ExponentialDistribution(updateIntervalSec);
    while (true) {
      if (System.currentTimeMillis() - t0 > maxExpDuration * 1000) {
        GNS.getLogger().info("Experiment over: duration = " + (System.currentTimeMillis() - t0)/1000 + " sec");
        break;
      }
      long delay = (int)(exp.getNextArrivalDelay()*1000);
      try {
        Thread.sleep(delay);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      executor.submit(new UpdateTask());
    }
  }

  class UpdateTask extends TimerTask{

    @Override
    public void run() {
      String value = Util.randomString(10);
      setCurrentValue(value);
      NSResponseCode response = Intercessor.sendUpdateRecordBypassingAuthentication(name, key,
              value, null, UpdateOperation.REPLACE_ALL);
      assert response == NSResponseCode.NO_ERROR: "Error in adding record";
    }
  }

  private void startListeningThread() {
    new Thread(new Runnable() {
      @Override
      public void run() {
        ServerSocket welcomeSocket;
        while (true) {
          try {
            welcomeSocket = new ServerSocket(port);
            Thread.sleep(1000); // sleep between retries
            break;
          } catch (IOException e) {
            e.printStackTrace();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
        while(true)
        {
          try {
            Socket connectionSocket = welcomeSocket.accept();
//          BufferedReader inFromClient =
//                  new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
            DataOutputStream outToClient = new DataOutputStream(connectionSocket.getOutputStream());
//          String clientSentence = inFromClient.readLine();
//          System.out.println("Received: Line " + lineCount + "Sentence: " + clientSentence);
            outToClient.writeBytes(getCurrentValue() + "\n");
            connectionSocket.close();
          } catch (IOException e) {
            GNS.getLogger().severe("Mobile: IO exception while processing incoming request");
            e.printStackTrace();
          }
        }

      }
    }).start();
  }
}
