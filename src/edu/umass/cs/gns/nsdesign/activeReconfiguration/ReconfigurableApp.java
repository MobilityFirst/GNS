package edu.umass.cs.gns.nsdesign.activeReconfiguration;


import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Reconfigurable;
import edu.umass.cs.gns.nsdesign.Replicable;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.replicaController.Application;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Set;

/**
@author V. Arun
 */
public class ReconfigurableApp implements Reconfigurable, Replicable {

	Application app=null;
	ActiveReplica activeReplica = null;

	public ReconfigurableApp(Application app, ActiveReplica activeReplica) {
		this.app = app;
    this.activeReplica = activeReplica;
	}

  private void assertReconfigurable() {
		if(!(this.app instanceof Reconfigurable)) 
			throw new RuntimeException("Attempting to reconfigure an application that is not reconfigurable");
	}

	private boolean isStopRequest(String value) {
		/* logic to determine if it is a stop request */
    try {
      JSONObject json = new JSONObject(value);
      if (Packet.getPacketType(json).equals(Packet.PacketType.OLD_ACTIVE_STOP)) {
        return true;
      }
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return false;
  }

	@Override
	public boolean handleDecision(String name, String value, boolean recovery, boolean noCoordinatorState) {
		boolean executed;
		if(isStopRequest(value)) {
      if (noCoordinatorState) {
        // ignore
        GNS.getLogger().severe("No coordinator state found for stop request: " + value);
        executed = false;
      } else {
        executed = stopVersion(name, (short) -1);
        if (executed) {
          // send confirmation to primary
        }
      }
    }
    else executed = this.app.handleDecision(name, value, recovery, noCoordinatorState);
		return executed;
	}

	@Override
	public boolean stopVersion(String name, short version) {
		assertReconfigurable();
		return ((Reconfigurable)(this.app)).stopVersion(name, version);
	}

	@Override
	public String getFinalState(String name, short version) {
		assertReconfigurable();
		return ((Reconfigurable)this.app).getFinalState(name, version);
	}

	@Override
	public void putInitialState(String name, short version, String state, Set<Integer> activeServers) {
		assertReconfigurable();
		((Reconfigurable)this.app).putInitialState(name, version, state, activeServers);
	}

	@Override
	public int deleteFinalState(String name, short version) {
		assertReconfigurable();
		return ((Reconfigurable)(this.app)).deleteFinalState(name, version);
	}

  @Override
  public String getState(String name) {
    return null;
  }

  @Override
  public boolean updateState(String name, String state) {
    return false;
  }
}
