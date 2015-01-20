package edu.umass.cs.gns.reconfiguration.examples;

import java.util.Set;

import edu.umass.cs.gns.nio.nioutils.SampleNodeConfig;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurableNodeConfig;

/**
 * @author V. Arun
 */
public class ReconfigurableSampleNodeConfig extends SampleNodeConfig<Integer> implements InterfaceReconfigurableNodeConfig<Integer> {

  public ReconfigurableSampleNodeConfig() {
    super();
  }

  public ReconfigurableSampleNodeConfig(int defaultPort) {
    super(defaultPort);
  }

  @Override
  public Set<Integer> getActiveReplicas() {
    return this.getNodeIDs();
  }

  @Override
  public Set<Integer> getReconfigurators() {
    return this.getNodeIDs();
  }

  @Override
  public void localSetup(int nNodes) {
    super.localSetup(nNodes);
  }

}
