
package edu.umass.cs.gnscommon.utils;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;


public class NetworkUtils {


  public static InetAddress getLocalHostLANAddress() throws UnknownHostException {
    try {
      InetAddress candidateAddress = null;
      // Iterate all NICs (network interface cards)...
      for (Enumeration<?> ifaces = NetworkInterface.getNetworkInterfaces(); ifaces.hasMoreElements();) {
        NetworkInterface iface = (NetworkInterface) ifaces.nextElement();
        // Iterate all IP addresses assigned to each card...
        for (Enumeration<?>  inetAddrs = iface.getInetAddresses(); inetAddrs.hasMoreElements();) {
          InetAddress inetAddr = (InetAddress) inetAddrs.nextElement();
          // filter out IP6 addresses for now
          if (!inetAddr.isLoopbackAddress() && isIp4Address(inetAddr.getHostAddress())) {
            if (inetAddr.isSiteLocalAddress()) {
              // Found non-loopback site-local address. Return it immediately...
              return inetAddr;
            } else if (candidateAddress == null) {
              // Found non-loopback address, but not necessarily site-local.
              // Store it as a candidate to be returned if site-local address is not subsequently found...
              candidateAddress = inetAddr;
              // Note that we don't repeatedly assign non-loopback non-site-local addresses as candidates,
              // only the first. For subsequent iterations, candidate will be non-null.
            }
          }
        }
      }
      if (candidateAddress != null) {
        // We did not find a site-local address, but we found some other non-loopback address.
        // Server might have a non-site-local address assigned to its NIC (or it might be running
        // IPv6 which deprecates the "site-local" concept).
        // Return this non-loopback candidate address...
        return candidateAddress;
      }
      // At this point, we did not find a non-loopback address.
      // Fall back to returning whatever InetAddress.getLocalHost() returns...
      InetAddress jdkSuppliedAddress = InetAddress.getLocalHost();
      if (jdkSuppliedAddress == null) {
        throw new UnknownHostException("The JDK InetAddress.getLocalHost() method unexpectedly returned null.");
      }
      return jdkSuppliedAddress;
    } catch (SocketException | UnknownHostException e) {
      UnknownHostException unknownHostException = new UnknownHostException("Failed to determine LAN address: " + e);
      unknownHostException.initCause(e);
      throw unknownHostException;
    }
  }

  private static Pattern VALID_IPV4_PATTERN = null;
  private static Pattern VALID_IPV6_PATTERN = null;
  private static final String IP_V4_PATTERN = "(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])";
  private static final String IP_V6_PATTERN = "([0-9a-f]{1,4}:){7}([0-9a-f]){1,4}";

  static {
    try {
      VALID_IPV4_PATTERN = Pattern.compile(IP_V4_PATTERN, Pattern.CASE_INSENSITIVE);
      VALID_IPV6_PATTERN = Pattern.compile(IP_V6_PATTERN, Pattern.CASE_INSENSITIVE);
    } catch (PatternSyntaxException e) {
      //logger.severe("Unable to compile pattern", e);
    }
  }


  public static boolean isIpAddress(String ipAddress) {

    Matcher m1 = VALID_IPV4_PATTERN.matcher(ipAddress);
    if (m1.matches()) {
      return true;
    }
    Matcher m2 = VALID_IPV6_PATTERN.matcher(ipAddress);
    return m2.matches();
  }


  public static boolean isIp4Address(String ipAddress) {
    return VALID_IPV4_PATTERN.matcher(ipAddress).matches();
  }


  public static boolean isIp6Address(String ipAddress) {
    return VALID_IPV6_PATTERN.matcher(ipAddress).matches();
  }

}
