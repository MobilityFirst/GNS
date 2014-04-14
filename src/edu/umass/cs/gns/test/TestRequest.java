package edu.umass.cs.gns.test;

/**
 * A request we send from local name server to name server during experiments, and in testing.
 */
public class TestRequest {
  // request type
  public static final int LOOKUP = 1;
  public static final int UPDATE = 2;
  public static final int ADD = 3;
  public static final int REMOVE = 4;
  public static final int GROUP_CHANGE = 5;

  public static final int DELAY = 6; // this is not a request. it introduces delay between the preceding and the next
                                      // request. the name field for DELAY entry is an integer that specifies the delay.

  public final int type;
  public final String name;

  public TestRequest(String name, int type) {
    this.name = name;
    if (type == LOOKUP || type == UPDATE || type == ADD || type == REMOVE || type == DELAY
            || type == GROUP_CHANGE)
      this.type = type;
    else
      throw new IllegalArgumentException("Request type not found: " + type);
  }

  public String toString() {
    return name + ":" + type;
  }


}
