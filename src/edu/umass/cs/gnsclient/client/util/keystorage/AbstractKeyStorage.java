package edu.umass.cs.gnsclient.client.util.keystorage;

/**
 * Defines functions for abstract key storage.
 *
 * @author ayadav
 *
 */
public abstract class AbstractKeyStorage {

  /**
   * Get method of the key storage.
   *
   * @param key
   * @param def
   * @return the value as a string
   */
  public abstract String get(String key, String def);

  /**
   * Remove method of the key storage.
   *
   * @param key
   */
  public abstract void remove(String key);

  /**
   * Put method of the key storage.
   *
   * @param key
   * @param value
   */
  public abstract void put(String key, String value);

  /**
   * Returns all the keys stored in the key storage as a string array.
   *
   * @return all the keys
   */
  public abstract String[] keys();

  /**
   * Clears all the keys stored in the key storage.
   */
  public abstract void clear();

  /**
   * Converts the key storage to String.
   *
   * @return a string
   */
  @Override
  public abstract String toString();
}
