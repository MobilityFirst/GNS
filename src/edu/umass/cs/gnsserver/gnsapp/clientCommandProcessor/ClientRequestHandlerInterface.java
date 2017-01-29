
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor;

import edu.umass.cs.gnsserver.gnsapp.GNSApp;
import edu.umass.cs.gnsserver.gnsapp.GNSClientInternal;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.Admintercessor;
import edu.umass.cs.gnsserver.nodeconfig.GNSNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;


public interface ClientRequestHandlerInterface {


  public GNSClientInternal getInternalClient();


  public GNSNodeConfig<String> getGnsNodeConfig();

  // FIXME: During transition we have both this and the above.

  public ConsistentReconfigurableNodeConfig<String> getNodeConfig();


  public InetSocketAddress getNodeAddress();


  public String getActiveReplicaID();


  public Admintercessor getAdmintercessor();


  public GNSApp getApp();


  public int getHttpServerPort();


  public void setHttpServerPort(int port);


  public String getHttpServerHostPortString() throws UnknownHostException;


  public int getHttpsServerPort();


  public void setHttpsServerPort(int port);


  public String getHttpsServerHostPortString() throws UnknownHostException;

}
