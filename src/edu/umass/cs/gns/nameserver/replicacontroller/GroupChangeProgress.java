package edu.umass.cs.gns.nameserver.replicacontroller;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Methods and object to track all group changes in progress.
 *
 * <p>
 * After replica controllers have agreed to change the set of active replicas for a name, there are three stages
 * to complete this change: (1) stopping previous active replicas from functioning (2) starting new active replicas
 * (3) replica controllers updating database to indicate group change completion. Methods in this class are called
 * before step 1, and on completion of each of the next steps.
 *
 * <p>
 * <b>Future work:</b> Package this class so that externally member do not need to use either the
 * hashmap <code>groupChangeProgress</code> or internal static constants such as <code>GROUP_CHANGE_START</code>.
 *
 * @see edu.umass.cs.gns.nameserver.replicacontroller.ComputeNewActivesTask
 * @see edu.umass.cs.gns.nameserver.replicacontroller.StartActiveSetTask
 * @see edu.umass.cs.gns.nameserver.replicacontroller.ReplicaController
 * @see edu.umass.cs.gns.nameserver.replicacontroller.StopActiveSetTask
 * @see edu.umass.cs.gns.nameserver.replicacontroller.WriteActiveNameServersRunningTask
 * Created by abhigyan on 2/23/14.
 * @deprecated
 */
public class GroupChangeProgress {

  public static ConcurrentHashMap<String, Integer> groupChangeProgress = new ConcurrentHashMap<String, Integer>();

  public static final int GROUP_CHANGE_START = 1;

  public static final int OLD_ACTIVE_STOP = 2;

  public static final int NEW_ACTIVE_START = 3;


  public static boolean updateGroupChangeProgress(String name, int status) {
    synchronized (groupChangeProgress) {
      if (groupChangeProgress.containsKey(name) == false && status > GROUP_CHANGE_START) {
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
