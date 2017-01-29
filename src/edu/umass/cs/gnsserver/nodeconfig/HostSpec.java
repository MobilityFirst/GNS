
package edu.umass.cs.gnsserver.nodeconfig;

import edu.umass.cs.gnscommon.utils.NetworkUtils;

import java.net.UnknownHostException;


public class HostSpec {

  private final String id;
  private final String name;
  private final String externalIP;
  private final Integer startPort;

  private boolean enablePublicIPConversion = false;


  public HostSpec(String id, String name, String externalIP, Integer startPort) {
    if (enablePublicIPConversion) {
      if ("127.0.0.1".equals(name) || "localhost".equals(name)) {
        try {
          name = NetworkUtils.getLocalHostLANAddress().getHostAddress();
        } catch (UnknownHostException e) {
          // punt if we can't get it
        }
      }
    }

    this.id = id;
    this.name = name;
    this.externalIP = externalIP;
    this.startPort = startPort;
  }


  public String getId() {
    return id;
  }


  public String getName() {
    return name;
  }


  public String getExternalIP() {
    return externalIP;
  }


  public Integer getStartPort() {
    return startPort;
  }

}
