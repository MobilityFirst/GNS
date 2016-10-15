package edu.umass.cs.gnsclient.client.util.keystorage;

import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;

/**
 *
 * @author westy
 */
public class JavaPreferencesKeyStore extends AbstractKeyStorage {

  private final Preferences KEYSTORE;

  /**
   * A keystore using JavaPreferences.
   */
  public JavaPreferencesKeyStore() {
    KEYSTORE
            = Preferences.userRoot().node(JavaPreferencesKeyStore.class.getName());
  }

  @Override
  public String get(String key, String def) {
    return KEYSTORE.get(key, def);
  }

  @Override
  public void remove(String key) {
    KEYSTORE.remove(key);
  }

  @Override
  public void put(String key, String value) {
    KEYSTORE.put(key, value);
  }

  @Override
  public String[] keys() {
    try {
      return KEYSTORE.keys();
    } catch (BackingStoreException e) {
      e.printStackTrace();
    }
    assert (false);
    return null;
  }

  @Override
  public void clear() {
    try {
      KEYSTORE.clear();
    } catch (BackingStoreException e) {
      e.printStackTrace();
    }
  }

  @Override
  public String toString() {
    return KEYSTORE.toString();
  }
}
