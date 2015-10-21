package edu.umass.cs.gns.gnsApp;

import edu.umass.cs.gigapaxos.InterfaceRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.reconfiguration.reconfigurationutils.InterfaceGetActiveIPs;

import java.net.InetAddress;
import java.util.ArrayList;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author Westy
 */
/*
 * This class maintains a demand profile that neither wants reports or reconfigurations.
 *
 * FIXME: Created this in part because the default profile was resulting in a reconfiguration that
 * was either not working or just exacerbating issues that we were trying to fix while debugging.
 * So we need to verify that the default DemandProfile works ok with our stuff instead of this one.
 */
public class NullDemandProfile extends AbstractDemandProfile {

  /**
   * The keys used in NullDemandProfile.
   */
  public enum Keys {

    /**
     * SERVICE_NAME
     */
    SERVICE_NAME
  };

  /**
   * Creates a NullDemandProfile instance.
   * 
   * @param name
   */
  public NullDemandProfile(String name) {
    super(name);
  }

  /**
   * Creates a NullDemandProfile by doing a deep copy of another instance.
   * 
   * @param dp
   */
    public NullDemandProfile(NullDemandProfile dp) {
    super(dp.name);
  }

  /**
   * Creates a NullDemandProfile from a JSON packet.
   * 
   * @param json
   * @throws JSONException
   */
  public NullDemandProfile(JSONObject json) throws JSONException {
    super(json.getString(Keys.SERVICE_NAME.toString()));
  }

  /**
   * Creates an empty NullDemandProfile.
   * 
   * @param name
   * @return
   */
  public static NullDemandProfile createDemandProfile(String name) {
    return new NullDemandProfile(name);
  }

  @Override
  public void register(InterfaceRequest request, InetAddress sender, InterfaceGetActiveIPs nodeConfig) {
  }

  @Override
  public boolean shouldReport() {
    return false;
  }

  @Override
  public JSONObject getStats() {
    JSONObject json = new JSONObject();
    try {
      json.put(Keys.SERVICE_NAME.toString(), this.name);
    } catch (JSONException je) {
      je.printStackTrace();
    }
    return json;
  }

  @Override
  public void reset() {
  }

  @Override
  public NullDemandProfile clone() {
    return new NullDemandProfile(this);
  }

  @Override
  public void combine(AbstractDemandProfile dp) {
  }

  
  @Override
  public ArrayList<InetAddress> shouldReconfigure(ArrayList<InetAddress> curActives, 
          InterfaceGetActiveIPs nodeConfig) {
    return null;
  }

  @Override
  public void justReconfigured() {
  }
}
