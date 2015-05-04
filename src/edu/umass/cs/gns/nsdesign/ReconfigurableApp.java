package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurableRequest;
import edu.umass.cs.gns.reconfiguration.InterfaceRequest;
import edu.umass.cs.gns.reconfiguration.RequestParseException;
import java.util.Set;

/**
 * @author V. Arun
 */
@Deprecated
public class ReconfigurableApp implements Reconfigurable {

  Application app = null;

  public ReconfigurableApp(Application app) {
    this.app = app;
  }

  private void assertReconfigurable() {
    if (!(this.app instanceof Reconfigurable)) {
      throw new RuntimeException("Attempting to reconfigure an application that is not reconfigurable");
    }
  }

  private boolean isStopRequest(String value) {
    /* logic to determine if it is a stop request */
    return false;
  }

  @Override
  public boolean handleDecision(String name, String value, boolean recovery) {
    boolean executed = this.app.handleDecision(name, value, recovery);
    if (isStopRequest(value)) {
      executed &= stopVersion(name, (short) -1);
    }
    return executed;
  }

  @Override
  public boolean stopVersion(String name, short version) {
    assertReconfigurable();
    return ((Reconfigurable) (this.app)).stopVersion(name, version);
  }

  @Override
  public String getFinalState(String name, short version) {
    assertReconfigurable();
    return ((Reconfigurable) this.app).getFinalState(name, version);
  }

  @Override
  public void putInitialState(String name, short version, String state) {
    assertReconfigurable();
    ((Reconfigurable) this.app).putInitialState(name, version, state);
  }

  @Override
  public int deleteFinalState(String name, short version) {
    assertReconfigurable();
    return ((Reconfigurable) (this.app)).deleteFinalState(name, version);
  }

  // For InterfaceRconfigurable
  @Override
  public InterfaceReconfigurableRequest getStopRequest(String name, int epoch) {
    assertReconfigurable();
    return ((Reconfigurable) (this.app)).getStopRequest(name, epoch);
  }

  @Override
  public String getFinalState(String name, int epoch) {
    assertReconfigurable();
    return ((Reconfigurable) (this.app)).getFinalState(name, epoch);
  }

  @Override
  public void putInitialState(String name, int epoch, String state) {
    assertReconfigurable();
    ((Reconfigurable) (this.app)).putInitialState(name, epoch, state);
  }

  @Override
  public boolean deleteFinalState(String name, int epoch) {
    assertReconfigurable();
    return ((Reconfigurable) (this.app)).deleteFinalState(name, epoch);
  }

  @Override
  public Integer getEpoch(String name) {
    assertReconfigurable();
    return ((Reconfigurable) (this.app)).getEpoch(name);
  }

  @Override
  public boolean handleRequest(InterfaceRequest request) {
    assertReconfigurable();
    return ((Reconfigurable) (this.app)).handleRequest(request);
  }

  @Override
  public InterfaceRequest getRequest(String stringified) throws RequestParseException {
    assertReconfigurable();
    return ((Reconfigurable) (this.app)).getRequest(stringified);
  }

  @Override
  public Set<IntegerPacketType> getRequestTypes() {
    assertReconfigurable();
    return ((Reconfigurable) (this.app)).getRequestTypes();
  }

}
