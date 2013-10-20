package edu.umass.cs.gns.client;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 9/24/13
 * Time: 1:15 PM
 * To change this template use File | Settings | File Templates.
 */
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Random;

public class NsLookup
{

  public long resolve(String host)
  {
    long t0  = System.currentTimeMillis();
    try
    {
      host = randomString(7) + host;
      InetAddress inetAddress = InetAddress.getByName(host);

//      System.out.println("Host: " +
//              inetAddress.getHostName());
//      System.out.println("IP Address: " +
//              inetAddress.getHostAddress());
    }
    catch (UnknownHostException e)
    {
//	    e.printStackTrace();
    }
    long t1  = System.currentTimeMillis();
    long delay = t1 - t0;
//    System.out.println("Delay = " + delay);
    return delay;
  }


  static final String CHARACTERS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  static Random rnd = new Random(System.currentTimeMillis());

  static String randomString(int len) {
    StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      sb.append(CHARACTERS.charAt(rnd.nextInt(CHARACTERS.length())));
    }
    return sb.toString();
  }

  public static void main(String[] args) throws InterruptedException, IOException {
    String outputFile = "dnsOutput";
    int numQuery = 30;
    ArrayList<Long> delays = new ArrayList<Long>();
    for (int i = 0; i < numQuery; i++) {
      long delay = new NsLookup().resolve(args[0]);
      delays.add(delay);
      Thread.sleep(1000);
      rnd.nextInt(2000);
    }
    FileWriter fw = new FileWriter(outputFile);
    for (Long delay: delays) {
      fw.write(delay + "\n");
    }
    fw.close();
  }

}
