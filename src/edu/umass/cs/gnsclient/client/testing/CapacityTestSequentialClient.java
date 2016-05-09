package edu.umass.cs.gnsclient.client.testing;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import java.util.concurrent.ThreadFactory;

/**
 * @author gaozy
 *
 */
public class CapacityTestSequentialClient {

  private final static String ACCOUNT_ALIAS = "@gigapaxos.net";
  private final static int GNS_CLIENT_NUM = 10;

  private static int NUM_THREAD = 100;
  private static int NUM_CLIENT = 0;
  private static SingleClient[] clients;
  private static ThreadPoolExecutor executorPool;

  static class MyThreadFactory implements ThreadFactory {

    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r);
      return t;
    }
  }

  /**
   * @param args
   * @throws IOException
   * @throws InvalidKeySpecException
   * @throws NoSuchAlgorithmException
   * @throws InvalidKeyException
   * @throws SignatureException
   * @throws Exception
   */
  public static void main(String[] args) throws IOException,
          InvalidKeySpecException, NoSuchAlgorithmException,
          InvalidKeyException, SignatureException, Exception {
    int node = Integer.parseInt(args[0]);
    int BENIGN = Integer.parseInt(args[1]);
    NUM_CLIENT = Integer.parseInt(args[2]);
    System.out.println("There are " + BENIGN + "/" + NUM_CLIENT + " clients.");

    clients = new SingleClient[NUM_CLIENT];
    //UniversalTcpClient client = new UniversalTcpClient(address, 24398, false);
    GNSClientCommands[] gnsClients = new GNSClientCommands[GNS_CLIENT_NUM];
    for (int i = 0; i < GNS_CLIENT_NUM; i++) {
      gnsClients[i] = new GNSClientCommands();
    }

    executorPool = new ThreadPoolExecutor(NUM_THREAD, NUM_THREAD, 0, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(), new MyThreadFactory());
    executorPool.prestartAllCoreThreads();

    // round-robin gnsClient
    int k = 0;
    for (int index = 0; index < NUM_CLIENT; index++) {
      String account = "test" + (node * 1000 + index) + ACCOUNT_ALIAS;

      GuidEntry accountGuid = KeyPairUtils.getGuidEntry("server.gns.name", account);
      GNSClientCommands client = gnsClients[k];
      k = (k + 1) % GNS_CLIENT_NUM;

      if (index < BENIGN) {
        clients[index] = new SingleClient(client, accountGuid, false);
      } else {
        clients[index] = new SingleClient(client, accountGuid, true);
      }

    }

    System.out.println("1st run");
    long start = System.currentTimeMillis();

    for (int i = 0; i < NUM_CLIENT; i++) {
      executorPool.execute(clients[i]);
    }

    int t = 0;
    int received = 0;
    int max = 0;
    int thruput = 0;
    while (executorPool.getCompletedTaskCount() < NUM_CLIENT) {
      thruput = (MessageStats.latency.size() + MessageStats.mal_request.size()) - received;
      if (max < thruput) {
        max = thruput;
      }
      System.out.println(t + " Throuput:" + thruput + " reqs/sec");
      received = MessageStats.latency.size() + MessageStats.mal_request.size();
      t++;
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    double eclapsed = System.currentTimeMillis() - start;
    System.out.println("It takes " + eclapsed + "ms to send all the requests");
    System.out.println("The maximum throuput is " + max + " reqs/sec, and the average throughput is "
            + (1000 * (MessageStats.latency.size() + MessageStats.mal_request.size()) / eclapsed) + " req/sec.");
    System.exit(0);
  }

}
