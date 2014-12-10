package edu.umass.cs.gns.nio.deprecated;

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
