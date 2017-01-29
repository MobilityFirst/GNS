
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor;

import edu.umass.cs.gnsserver.gnsapp.GNSApp;
import edu.umass.cs.gnsserver.gnsapp.GNSClientInternal;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.Admintercessor;
import edu.umass.cs.gnsserver.nodeconfig.GNSNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import static edu.umass.cs.gnscommon.utils.NetworkUtils.getLocalHostLANAddress;


public class ClientRequestHandler implements ClientRequestHandlerInterface {

  private final GNSClientInternal internalClient;
  private final Admintercessor admintercessor;


  private final GNSNodeConfig<String> gnsNodeConfig;

  private final ConsistentReconfigurableNodeConfig<String> nodeConfig;


  private final InetSocketAddress nodeAddress;
  //
  private final String activeReplicaID;
  private final GNSApp app;
  private int httpServerPort;
  private int httpsServerPort;


  public ClientRequestHandler(Admintercessor admintercessor,
          InetSocketAddress nodeAddress,
          String activeReplicaID,
          GNSApp app,
          GNSNodeConfig<String> gnsNodeConfig) throws IOException {

    assert (activeReplicaID != null);
    this.admintercessor = admintercessor;
    this.nodeAddress = nodeAddress;
    // a little hair to convert fred to fred-activeReplica if we just get fred
    this.activeReplicaID = gnsNodeConfig.isActiveReplica(activeReplicaID) ? activeReplicaID
            : gnsNodeConfig.getReplicaNodeIdForTopLevelNode(activeReplicaID);
    this.internalClient = (GNSClientInternal) new GNSClientInternal(app.toString()).setForceCoordinatedReads(true).setNumRetriesUponTimeout(1);
    this.app = app;
    // FOR NOW WE KEEP BOTH
    this.nodeConfig = new ConsistentReconfigurableNodeConfig<>(gnsNodeConfig);
    this.gnsNodeConfig = gnsNodeConfig;
  }
  
  @Override
  public GNSNodeConfig<String> getGnsNodeConfig() {
    return gnsNodeConfig;
  }

  @Override
  public ConsistentReconfigurableNodeConfig<String> getNodeConfig() {
    return nodeConfig;
  }

  @Override
  public InetSocketAddress getNodeAddress() {
    return nodeAddress;
  }

  @Override
  public String getActiveReplicaID() {
    return activeReplicaID;
  }

  @Override
  public Admintercessor getAdmintercessor() {
    return admintercessor;
  }

  @Override
  public GNSApp getApp() {
    return app;
  }

  @Override
  public int getHttpServerPort() {
    return httpServerPort;
  }

  @Override
  public void setHttpServerPort(int httpServerPort) {
    this.httpServerPort = httpServerPort;
  }

  @Override
  public String getHttpServerHostPortString() throws UnknownHostException {
    // this isn't going to work behind NATs but for public servers it should get the
    // job done
    return getLocalHostLANAddress().getHostAddress() + ":" + httpServerPort;
  }

  @Override
  public int getHttpsServerPort() {
    return httpsServerPort;
  }

  @Override
  public void setHttpsServerPort(int httpServerPort) {
    this.httpsServerPort = httpServerPort;
  }

  @Override
  public String getHttpsServerHostPortString() throws UnknownHostException {
    // this isn't going to work behind NATs but for public servers it should get the
    // job done
    return getLocalHostLANAddress().getHostAddress() + ":" + httpsServerPort;
  }

  @Override
  public GNSClientInternal getInternalClient() {
    return this.internalClient;
  }

}
