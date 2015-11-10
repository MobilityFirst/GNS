/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 */
package edu.umass.cs.gnsserver.deprecated.nio;

import java.nio.channels.SocketChannel;

/* Used by NIOTrasnport. Currently used only for
 * registering channels and for connect events. Read
 * events are monitored by default. Write events are
 * set through pendingWrites.
 */
@Deprecated
@SuppressWarnings("unchecked")
class ChangeRequest {

  public static final int REGISTER = 1;
  public static final int CHANGEOPS = 2;

  public final SocketChannel socket;
  public final int type;
  public final int ops;

  public ChangeRequest(SocketChannel socket, int type, int ops) {
    this.socket = socket;
    this.type = type;
    this.ops = ops;
  }

  public String toString() {
    return "" + socket + ":" + type + ":" + ops;
  }
}
