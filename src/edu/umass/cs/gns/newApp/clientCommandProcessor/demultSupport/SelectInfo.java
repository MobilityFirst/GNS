package edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport;

/**************************************************************
 * This class represents a data structure to store information
 * about queries (SELECT like lookup) transmitted by the local name
 * server.
 *************************************************************/
public class SelectInfo {

  private final int id;

  /**************************************************************
   * Constructs a SelectInfo object with the following parameters
   * @param id Query id
   **************************************************************/
  public SelectInfo(int id) {
    this.id = id;
  }

  public int getId() {
    return id;
  }
  
}
