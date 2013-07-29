package edu.umass.cs.gnrs.util;

import edu.umass.cs.gnrs.main.GNS;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

/**
 * Various static utility methods.
 *
 * @author Hardeep Uppal, Westy
 */
public class Util {

  //---------------------------------------------------------
  // Address utilities
  //---------------------------------------------------------
  /**
   * ********************************************************
   * Returns the IP address in textual representation.
   *
   * @param addr InetSocketAddress
   ********************************************************
   */
  public static String InetAddrToHost(InetSocketAddress addr) throws UnknownHostException {
    String host = addr.getAddress().getHostAddress();
    host = host.toLowerCase();
    if (host.equals("localhost")) {
      host = InetAddress.getLocalHost().getHostAddress();
    }
    return host;
  }

  /**
   * ********************************************************
   * Convert byte[offset]...byte[offset+3] to integer.
   *
   * @param buf Byte array
   * @param offset Offset into the byte array
	 ********************************************************
   */
  public static int BAToInt(byte[] buf, int offset) {
    int val = ((buf[offset] & 0xff) << 24)
            | ((buf[offset + 1] & 0xff) << 16)
            | ((buf[offset + 2] & 0xff) << 8)
            | (buf[offset + 3] & 0xff);
    return val;
  }

  /**
   * ********************************************************
   * Convert byte[0]...buf[3] to integer.
	 ********************************************************
   */
  public static int BAToInt(byte[] buf) {
    return BAToInt(buf, 0);
  }

  //---------------------------------------------------------
  // Integer / byte[] utilities
  //---------------------------------------------------------
  /**
   * ********************************************************
   * Write integer into buf[offset]...buf[offset+3]
   *
   * @param val Integer value
   * @param buf Byte array
   * @param offset Offset into the byte array
   ********************************************************
   */
  public static void IntToByteArray(int val, byte[] buf, int offset) {
    buf[0 + offset] = (byte) ((val >> 24) & 0xff);
    buf[1 + offset] = (byte) ((val >> 16) & 0xff);
    buf[2 + offset] = (byte) ((val >> 8) & 0xff);
    buf[3 + offset] = (byte) (val & 0xff);
  }

  /**
   * ********************************************************
   * Write integer into buf[0]...buf[3]
   *
   * @param val Integer value
   * @param buf Byte array
   ********************************************************
   */
  public static void IntToByteArray(int val, byte[] buf) {
    IntToByteArray(val, buf, 0);
  }

  /**
   * ********************************************************
   * Convert byte[offset]...byte[offset+3] to integer.
   *
   * @param buf Byte array
   * @param offset Offset into the byte array
   ********************************************************
   */
  public static int ByteArrayToInt(byte[] buf, int offset) {
    int val = ((buf[offset] & 0xff) << 24)
            | ((buf[offset + 1] & 0xff) << 16)
            | ((buf[offset + 2] & 0xff) << 8)
            | (buf[offset + 3] & 0xff);
    return val;
  }

  /**
   * ********************************************************
   * Convert byte[0]...buf[3] to integer.
   ********************************************************
   */
  public static int ByteArrayToInt(byte[] buf) {
    return ByteArrayToInt(buf, 0);
  }

  //---------------------------------------------------------
  // short / byte[] utilities
  //---------------------------------------------------------
  /**
   * ********************************************************
   * Write low-order 16-bits of val into buf[offset] buf[offset+1]
   ********************************************************
   */
  public static void ShortToByteArray(int val, byte[] buf, int offset) {
    buf[0 + offset] = (byte) ((val >> 8) & 0xff);
    buf[1 + offset] = (byte) (val & 0xff);
  }

  /**
   * ********************************************************
   * Write low order 16-bits of val into buf[0] buf[1] 
   ********************************************************
   */
  public static void ShortToByteArray(int val, byte[] buf) {
    ShortToByteArray(val, buf, 0);
  }

  /**
   * ********************************************************
   * Convert buf[offset] buf[offset+1] to int in range 0..65535
   ********************************************************
   */
  public static int ByteArrayToShort(byte[] buf, int offset) {
    int val = ((buf[offset] & 0xff) << 8) | (buf[offset + 1] & 0xff);
    return val;
  }

  /**
   * ********************************************************
   * Convert buf[0] buf[1] to int in range 0..65535 
   ********************************************************
   */
  public static int ByteArrayToShort(byte[] buf) {
    return ByteArrayToShort(buf, 0);
  }

  public static String convertToHex(byte[] data) {
    StringBuilder buf = new StringBuilder();

    for (int i = 0; i < data.length; i++) {
      int halfbyte = (data[i] >>> 4) & 0x0F;
      int two_halfs = 0;

      do {
        if ((0 <= halfbyte) && (halfbyte <= 9)) {
          buf.append((char) ('0' + halfbyte));
        } else {
          buf.append((char) ('a' + (halfbyte - 10)));
        }
        halfbyte = data[i] & 0x0F;
      } while (two_halfs++ < 1);

    }

    return buf.toString();
  }

  public static void println(String string, boolean print) {
    if (print) {
      GNS.getLogger().fine(string);
      //System.out.println(string);
    }
  }

  /**
   * ********************************************************
   * Loops around for the give time
   *
   * @param waitingTime Time in ms.
   ********************************************************
   */
  public static void waitForTime(int waitingTime) {
    long t0 = System.currentTimeMillis();
    long t1 = 0;
    do {
      t1 = System.currentTimeMillis();
    } while (t1 - t0 < waitingTime);
  }

  public static int round(double d) {
    return (int) Math.round(d);
  }

  /**
   * ************************************************************
   * Returns a name server id with the smallest network latency among the set of name servers. If the name server set is
   * empty or all the name servers have already been queried then -1 is returned. Returns -1 of the name servers Set is
   * null
   *
   * @param nameservers A set of name servers
   * @param nameserverQueried A set of name servers already queried
   * @return Name server id with the smallest latency or -1.
   ************************************************************
   */
  public static int getSmallestLatencyNS(Set<Integer> nameservers, Set<Integer> nameserverQueried) {
    if (nameservers == null) {
      return -1;
    }

    if (nameservers.contains(ConfigFileInfo.getClosestNameServer())
            && nameserverQueried != null
            && !nameserverQueried.contains(ConfigFileInfo.getClosestNameServer())
            && ConfigFileInfo.getPingLatency(ConfigFileInfo.getClosestNameServer()) >= 0) {
      return ConfigFileInfo.getClosestNameServer();
    }

    double lowestLatency = Double.MAX_VALUE;
    int nameServerID = -1;
    double pingLatency;

    //Find a name server with the smallest network latency
    for (Integer nsID : nameservers) {
      //Exclude name server that have already been queried
      if (nameserverQueried != null && nameserverQueried.contains(nsID)) {
        continue;
      }

      pingLatency = ConfigFileInfo.getPingLatency(nsID);
      if (pingLatency >= 0 && pingLatency < lowestLatency) {
        lowestLatency = pingLatency;
        nameServerID = nsID;
      }
    }

    return nameServerID;
  }

  public static int getSmallestLatencyNS2(Set<Integer> activeNameServer, Set<Integer> primaryNameServer,
          Set<Integer> nameserverQueried) {
    if (activeNameServer == null || primaryNameServer == null) {
      return -1;
    }

    if ((activeNameServer.contains(ConfigFileInfo.getClosestNameServer())
            || primaryNameServer.contains(ConfigFileInfo.getClosestNameServer()))
            && nameserverQueried != null
            && !nameserverQueried.contains(ConfigFileInfo.getClosestNameServer())
            && ConfigFileInfo.getPingLatency(ConfigFileInfo.getClosestNameServer()) >= 0) {
      return ConfigFileInfo.getClosestNameServer();
    }

    double lowestLatency = Double.MAX_VALUE;
    int nameServerID = -1;
    double pingLatency = -1;

    //Find a name server with the smallest network latency
    for (Integer nsID : activeNameServer) {
      //Exclude name server that have already been queried
      if (nameserverQueried != null && nameserverQueried.contains(nsID)) {
        continue;
      }

      pingLatency = ConfigFileInfo.getPingLatency(nsID);
      if (pingLatency >= 0 && pingLatency < lowestLatency) {
        lowestLatency = pingLatency;
        nameServerID = nsID;
      }
    }

    for (Integer nsID : primaryNameServer) {
      //Exclude name server that have already been queried
      if (nameserverQueried != null && nameserverQueried.contains(nsID)) {
        continue;
      }

      pingLatency = ConfigFileInfo.getPingLatency(nsID);
      if (pingLatency >= 0 && pingLatency < lowestLatency) {
        lowestLatency = pingLatency;
        nameServerID = nsID;
      }
    }

    return nameServerID;
  }

  /**
   * ********************************************************
   * Unit test.
   *
   * @throws IOException 
   ********************************************************
   */
  public static void main(String[] args) throws IOException {
    ConfigFileInfo.readHostInfo("ns2", 1);
    Set<Integer> primary = new HashSet<Integer>();
    primary.add(0);
    primary.add(3);
    primary.add(5);
    Set<Integer> active = new HashSet<Integer>();
    active.add(1);
    active.add(2);
    active.add(4);
    System.out.println(getSmallestLatencyNS2(active, primary, new HashSet<Integer>()));
  }
}
