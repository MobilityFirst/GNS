package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.gigapaxos.deprecated.Replicable;
import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.reconfiguration.InterfaceRequest;
import edu.umass.cs.gns.reconfiguration.RequestParseException;
import java.util.Set;

/**
 * @author V. Arun
 */
@Deprecated
public class ReplicableApp implements Replicable {

  Application app;

  public ReplicableApp(Application app) {
    this.app = app;
  }

  private void assertReplicable() {
    if (!(this.app instanceof Replicable)) {
      throw new RuntimeException("Attempting to replicate an application that is not replicable");
    }
  }

  @Override
  public boolean handleDecision(String name, String value, boolean recovery) {
    assertReplicable();
    return ((Replicable) this.app).handleDecision(name, value, recovery);
  }

  @Override
  public String getState(String name) {
    assertReplicable();
    return ((Replicable) this.app).getState(name);
  }

  @Override
  public boolean updateState(String name, String state) {
    assertReplicable();
    return ((Replicable) this.app).updateState(name, state);
  }

  // For InterfaceReplicable
  @Override
  public boolean handleRequest(InterfaceRequest request) {
    assertReplicable();
    return ((Replicable) this.app).handleRequest(request);
  }

  @Override
  public boolean handleRequest(InterfaceRequest request, boolean handleRequest) {
    assertReplicable();
    return ((Replicable) this.app).handleRequest(request, handleRequest);
  }

//  @Override
//  public String getState(String name, int epoch) {
//    assertReplicable();
//    return ((Replicable) this.app).getState(name, epoch);
//  }

  @Override
  public InterfaceRequest getRequest(String stringified) throws RequestParseException {
    assertReplicable();
    return ((Replicable) this.app).getRequest(stringified);
  }

  @Override
  public Set<IntegerPacketType> getRequestTypes() {
    assertReplicable();
    return ((Replicable) this.app).getRequestTypes();
  }
}
