package edu.umass.cs.gns.nameserver.replicacontroller;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Methods and object to track all group changes in progress
 *
 * Created by abhigyan on 2/23/14.
 */
public class GroupChangeProgress {

  public static ConcurrentHashMap<String, Integer> groupChangeProgress = new ConcurrentHashMap<String, Integer>();

  public static final int STOP_SENT = 1;

  public static final int OLD_ACTIVE_STOP = 2;

  public static final int NEW_ACTIVE_START = 3;


  public static boolean updateGroupChangeProgress(String name, int status) {
    synchronized (groupChangeProgress) {
      if (groupChangeProgress.containsKey(name) == false && status > STOP_SENT) {
        return false;
      }
      if (groupChangeProgress.containsKey(name) == false) {
        groupChangeProgress.put(name, status);
        return true;
      }
      if (groupChangeProgress.get(name) >= status) {
        return false;
      }
      groupChangeProgress.put(name, status);
      return true;
    }
  }


  public static void groupChangeComplete(String name) {
    synchronized (groupChangeProgress) {
      groupChangeProgress.remove(name);
    }
  }

}
