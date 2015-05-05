/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.localnameserver;

/**
 *
 * @author westy
 */
public class LNSCommandInfo {
  private String clientHost;
  private int clientPort;

  public LNSCommandInfo(String host, int port) {
    this.clientHost = host;
    this.clientPort = port;
  }

  public String getHost() {
    return clientHost;
  }

  public int getPort() {
    return clientPort;
  }
  
}
