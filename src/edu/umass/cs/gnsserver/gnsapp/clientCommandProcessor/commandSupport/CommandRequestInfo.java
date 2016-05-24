/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnscommon.CommandType;
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
  private final CommandType commandType;
  private final String guid;
  // arun: Need this for correct receiver messaging
  final InetSocketAddress myListeningAddress;

  /**
   *
   * @param host
   * @param port
   * @param commandType
   * @param guid
   * @param myListeningAddress
   */
  public CommandRequestInfo(String host, int port,
          CommandType commandType,
          String guid, InetSocketAddress myListeningAddress) {
    this.host = host;
    this.port = port;
    this.commandType = commandType;
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
   * @return the command type
   */
  public CommandType getCommandType() {
    return commandType;
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
