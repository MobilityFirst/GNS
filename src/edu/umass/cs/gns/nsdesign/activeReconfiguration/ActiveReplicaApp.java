package edu.umass.cs.gns.nsdesign.activeReconfiguration;


import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.Reconfigurable;
import edu.umass.cs.gns.nsdesign.Replicable;
import edu.umass.cs.gns.nsdesign.packet.OldActiveSetStopPacket;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.Application;
import org.json.JSONException;
import org.json.JSONObject;

/**
@author V. Arun, abhigyan
 */
public class ActiveReplicaApp implements Reconfigurable, Replicable {

	Application app=null;
	ActiveReplica activeReplica = null;

	public ActiveReplicaApp(Application app, ActiveReplica activeReplica) {
		this.app = app;
    this.activeReplica = activeReplica;
	}

  private void assertReconfigurable() {
		if(!(this.app instanceof Reconfigurable)) 
			throw new RuntimeException("Attempting to reconfigure an application that is not reconfigurable");
	}

	@Override
	public boolean handleDecision(String name, String value, boolean recovery) {
    boolean executed = false;
    try {
      JSONObject json = new JSONObject(value);
      if (Packet.getPacketType(json).equals(Packet.PacketType.OLD_ACTIVE_STOP)) {
        OldActiveSetStopPacket stopPkt = new OldActiveSetStopPacket(json);
        if (Config.debugMode) GNS.getLogger().fine("Executing stop request: " + value);
        boolean noCoordinationState = json.has(Config.NO_COORDINATOR_STATE_MARKER);
        if (noCoordinationState) {
          // probably stop has already been executed, so send confirmation to replica controller
          GNS.getLogger().warning("No coordinator state found for stop request: " + value);
          executed = true;
        } else {
          executed = stopVersion(stopPkt.getName(), (short) -1);
          if (!executed) {
            GNS.getLogger().severe("Stop request not executed: name = " + stopPkt.getName() + " request = " + value);
          }
        }
        if (executed) {
          try {
            activeReplica.stopProcessed(new OldActiveSetStopPacket(new JSONObject(value)));
          } catch (JSONException e) {
            e.printStackTrace();
          }
        }
      }
      else executed = this.app.handleDecision(name, value, recovery);
    } catch (JSONException e) {
      e.printStackTrace();
    }

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
	public void putInitialState(String name, short version, String state) {
		assertReconfigurable();
		((Reconfigurable)this.app).putInitialState(name, version, state);
	}

	@Override
	public int deleteFinalState(String name, short version) {
		assertReconfigurable();
		return ((Reconfigurable)(this.app)).deleteFinalState(name, version);
	}

  @Override
  public String getState(String name) {
    return ((Replicable)app).getState(name);
  }

  @Override
  public boolean updateState(String name, String state) {
    return ((Replicable)app).updateState(name, state);
  }
}
