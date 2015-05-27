/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.newApp;

import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.reconfiguration.InterfaceReplicable;
import edu.umass.cs.gns.reconfiguration.PaxosReplicaCoordinator;
import edu.umass.cs.gns.util.Stringifiable;

/**
 * @author Westy
 * @param <String>
 */
public class NewAppCoordinator<String> extends PaxosReplicaCoordinator<String> {

  NewAppCoordinator(InterfaceReplicable app, Stringifiable<String> unstringer, 
          JSONMessenger<String> messenger) {
    super(app, messenger.getMyID(), unstringer, messenger);
  }
}
