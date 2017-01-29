
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnscommon.CommandType;

import java.net.InetSocketAddress;

//

// Code for handling commands at the app
//
// public because the app uses this

public class CommandRequestInfo {

  private final String host;
  private final int port;
  // For debugging
  private final CommandType commandType;
  private final String guid;
  // arun: Need this for correct receiver messaging
  final InetSocketAddress myListeningAddress;


  public CommandRequestInfo(String host, int port,
          CommandType commandType,
          String guid, InetSocketAddress myListeningAddress) {
    this.host = host;
    this.port = port;
    this.commandType = commandType;
    this.guid = guid;
    this.myListeningAddress = myListeningAddress;
  }


  public String getHost() {
    return host;
  }


  public int getPort() {
    return port;
  }


  public CommandType getCommandType() {
    return commandType;
  }


  public String getGuid() {
    return guid;
  }
  
}
