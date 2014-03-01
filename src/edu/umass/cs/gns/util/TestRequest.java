package edu.umass.cs.gns.util;

/**
 * A request we send from local name server to name server during experiments, and in testing.
 */
public class TestRequest {

  public static final int LOOKUP = 1;
  public static final int UPDATE = 2;
  public static final int ADD = 3;
  public static final int REMOVE = 4;


  public int type;
  public String name;

  public TestRequest(String name, int type) {
    this.name = name;
    if (type == LOOKUP || type == UPDATE || type == ADD || type == REMOVE)
      this.type = type;
    else
      throw new IllegalArgumentException("Request type not found: " + type);
  }

  public String toString() {
    return name + ":" + type;
  }


}
