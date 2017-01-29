
package edu.umass.cs.gnsserver.nodeconfig;

import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;

import java.util.Set;




public interface GNSInterfaceNodeConfig<NodeIDType> extends
        ReconfigurableNodeConfig<NodeIDType> {


  @Override
  public Set<NodeIDType> getReconfigurators();


  @Override
  public Set<NodeIDType> getActiveReplicas();


  public abstract int getAdminPort(NodeIDType id);


  public abstract int getCcpPort(NodeIDType id);
  

  public abstract int getCcpAdminPort(NodeIDType id);


  public long getPingLatency(NodeIDType id);


  public void updatePingLatency(NodeIDType id, long responseTime);


  public abstract long getVersion();

}
