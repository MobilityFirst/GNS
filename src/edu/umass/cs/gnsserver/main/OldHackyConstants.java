package edu.umass.cs.gnsserver.main;

public class OldHackyConstants {

  // This is designed so we can run multiple NSs on the same host if needed

  @Deprecated
  public enum PortType {

    ACTIVE_REPLICA_PORT(0),

    RECONFIGURATOR_PORT(1),
    // Reordered these so they work with the new GNSApp

    NS_TCP_PORT(3), // TCP port at name servers

    NS_ADMIN_PORT(4),
    // sub ports

    CCP_PORT(6),

    CCP_ADMIN_PORT(7);

    //
    int offset;

    PortType(int offset) {
      this.offset = offset;
    }


    public static int maxOffset() {
      int result = 0;
      for (PortType p : values()) {
        if (p.offset > result) {
          result = p.offset;
        }
      }
      return result;
    }


    public int getOffset() {
      return offset;
    }
  }

  //FIXME: Remove this.

  @Deprecated
  public static final int DEFAULT_STARTING_PORT = 24400;

}
