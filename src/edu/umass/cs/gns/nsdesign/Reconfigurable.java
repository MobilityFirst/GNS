package edu.umass.cs.gns.nsdesign;

/**
 * Created by abhigyan on 3/27/14.
 */
public interface Reconfigurable extends Replicable{

  public abstract String getFinalState(String name, int version);

  public abstract void putInitialState(String name, int version, String state);

}
