package edu.umass.cs.gns.reconfiguration.examples;

import java.util.Set;

import edu.umass.cs.gns.nio.nioutils.SampleNodeConfig;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurableNodeConfig;
import org.json.JSONArray;
import org.json.JSONException;

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

  @Override
  public Set<Integer> getValuesFromStringSet(Set<String> strNodes) {
    throw new UnsupportedOperationException("Not supported yet."); 
  }

  @Override
  public Set<Integer> getValuesFromJSONArray(JSONArray array) throws JSONException {
    throw new UnsupportedOperationException("Not supported yet."); 
  }
}
