package edu.umass.cs.gns.nsdesign.replicaController;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Wrapper class around concurrent hash map. Its key feature is a put(Object) method which returns a unique key for that
 * object.
 * Created by abhigyan on 3/22/14.
 */
public class UniqueIDHashMap {

  ConcurrentHashMap<Integer, Object> objectStore = new ConcurrentHashMap<Integer, Object>();

  private Random r = new Random();

  public synchronized int put(Object o) {
    int ID = r.nextInt();
    while (objectStore.containsKey(ID)) ID = r.nextInt();
    objectStore.put(ID, o);
    return ID;
  }

  public Object get(int i) {
    return objectStore.get(i);
  }

  public Object remove(int i) {
    return objectStore.remove(i);
  }

}
