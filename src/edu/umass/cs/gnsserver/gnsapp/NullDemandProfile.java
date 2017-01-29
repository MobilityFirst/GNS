
package edu.umass.cs.gnsserver.gnsapp;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.reconfiguration.reconfigurationutils.InterfaceGetActiveIPs;

import java.net.InetAddress;
import java.util.ArrayList;

import org.json.JSONException;
import org.json.JSONObject;



public class NullDemandProfile extends AbstractDemandProfile {


  public enum Keys {


    SERVICE_NAME
  };


  public NullDemandProfile(String name) {
    super(name);
  }


    public NullDemandProfile(NullDemandProfile dp) {
    super(dp.name);
  }


  public NullDemandProfile(JSONObject json) throws JSONException {
    super(json.getString(Keys.SERVICE_NAME.toString()));
  }


  public static NullDemandProfile createDemandProfile(String name) {
    return new NullDemandProfile(name);
  }


  @Override
  public void register(Request request, InetAddress sender, InterfaceGetActiveIPs nodeConfig) {
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
