package edu.umass.cs.gns.nsdesign.gnsReconfigurable;

import edu.umass.cs.gns.nsdesign.Reconfigurable;
import org.json.JSONObject;

/**
 * This interface exists so that packet demultiplexer can forward packets to active replica.
 * Created by abhigyan on 2/27/14.
 */
public interface GnsReconfigurableInterface extends Reconfigurable {

  public void handleIncomingPacket(JSONObject json);

  void resetDB();

}
