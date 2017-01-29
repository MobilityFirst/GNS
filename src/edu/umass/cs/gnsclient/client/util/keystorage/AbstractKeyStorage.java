package edu.umass.cs.gnsclient.client.util.keystorage;


public abstract class AbstractKeyStorage {


  public abstract String get(String key, String def);


  public abstract void remove(String key);


  public abstract void put(String key, String value);


  public abstract String[] keys();


  public abstract void clear();


  @Override
  public abstract String toString();
}
