package edu.umass.cs.gnsclient.client.util.keystorage;

/**
 * Created by kanantharamu on 2/1/17.
 */
public class IOSKeyStorage extends AbstractKeyStorage {

    public IOSKeyStorage() {
        //Empty constructor
    }

    @Override
    public String get(String key, String def) {
        return null;
    }

    @Override
    public void remove(String key) {

    }

    @Override
    public void put(String key, String value) {

    }

    @Override
    public String[] keys() {
        return new String[0];
    }

    @Override
    public void clear() {

    }

    @Override
    public String toString() {
        return null;
    }
}
