/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp;

import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.PaxosReplicaCoordinator;

/**
 * @author Westy
 * @param <String>
 */
public class GnsAppCoordinator<String> extends PaxosReplicaCoordinator<String> {

  GnsAppCoordinator(Replicable app, Stringifiable<String> unstringer, 
          JSONMessenger<String> messenger) {
    super(app, messenger.getMyID(), unstringer, messenger);
  }
}
