/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport;

import java.net.InetSocketAddress;

//

// Code for handling commands at the app
//
// public because the app uses this
/**
 * Encapsulates the info needed for a command request.
 */
public class CommandRequestInfo {

  private final String host;
  private final int port;
  // For debugging
  private final String command;
  private final String guid;
  // arun: Need this for correct receiver messaging
  final InetSocketAddress myListeningAddress;

  /**
   *
   * @param host
   * @param port
   * @param command
   * @param guid
   */
  public CommandRequestInfo(String host, int port, String command, String guid, InetSocketAddress myListeningAddress) {
    this.host = host;
    this.port = port;
    this.command = command;
    this.guid = guid;
    this.myListeningAddress = myListeningAddress;
  }

  /**
   * Returns the host.
   *
   * @return a string
   */
  public String getHost() {
    return host;
  }

  /**
   * Returns the port.
   *
   * @return an int
   */
  public int getPort() {
    return port;
  }

  /**
   * Returns the command.
   *
   * @return a string
   */
  public String getCommand() {
    return command;
  }

  /**
   * Returns the guid.
   *
   * @return a string
   */
  public String getGuid() {
    return guid;
  }
  
}
