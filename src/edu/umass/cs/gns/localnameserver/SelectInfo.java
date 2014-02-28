package edu.umass.cs.gns.localnameserver;

/**************************************************************
 * This class represents a data structure to store information
 * about queries (SELECT like lookup) transmitted by the local name
 * server.
 *************************************************************/
public class SelectInfo {

  private int id;

  /**************************************************************
   * Constructs a SelectInfo object with the following parameters
   * @param id Query id
   * @param name Host/Domain name
   * @param time System time when query was transmitted
   * @param nameserverID Response name server ID
   * @param queryStatus Query Status
   **************************************************************/
  public SelectInfo(int id) {
    this.id = id;
  }

  public int getId() {
    return id;
  }
  
}
