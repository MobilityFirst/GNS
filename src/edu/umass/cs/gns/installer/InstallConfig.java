/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.installer;

import edu.umass.cs.gns.database.DataStoreType;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 *
 * @author westy
 */
public class InstallConfig {

  private static final String USER_NAME = "userName";
  private static final String KEY_FILE = "keyFile";
  private static final String HOST_TYPE = "hostType";
  private static final String DATA_STORE_NAME = "dataStoreName";
  private static final String INSTALL_PATH = "installPath";

  private String username;
  private String keyname;
  private String hostType;
  private DataStoreType dataStoreType;
  private String installPath;

  public String getUsername() {
    return username;
  }

  public String getKeyname() {
    return keyname;
  }

  public String getHostType() {
    return hostType;
  }

  public DataStoreType getDataStoreType() {
    return dataStoreType;
  }

  public String getInstallPath() {
    return installPath;
  }

  public InstallConfig(String filename) {
    try {
      loadPropertiesFile(filename);
    } catch (IOException e) {
      GNS.getLogger().severe("Problem loading installer config file: " + e);
    }
  }

  private void loadPropertiesFile(String filename) throws IOException {
    Properties properties = new Properties();

    File f = new File(filename);
    if (f.exists() == false) {
      throw new FileNotFoundException("Install config file not found:" + filename);
    }
    InputStream input = new FileInputStream(filename);
    properties.load(input);
    // username=ec2-user
    // keyfile=aws.pem
    // hosttype=linux
    // datastorename=MONGO
    //#installpath=/home/westy
    this.username = properties.getProperty(USER_NAME, System.getProperty("user.name"));
    this.keyname = properties.getProperty(KEY_FILE);
    this.hostType = properties.getProperty(HOST_TYPE, "linux");
    this.dataStoreType = DataStoreType.valueOf(properties.getProperty(DATA_STORE_NAME, GNSInstaller.DEFAULT_DATA_STORE_TYPE.name()).toUpperCase());
    this.installPath = properties.getProperty(INSTALL_PATH);
  }

  @Override
  public String toString() {
    return "InstallConfigParser{" + "username=" + username + ", keyname=" + keyname + ", hostType=" + hostType + ", dataStoreType=" + dataStoreType + ", installPath=" + installPath + '}';
  }

  public static void main(String[] args) {
    String filename = Config.WESTY_GNS_DIR_PATH + "/conf/ec2_release/installer_config";
    InstallConfig config = new InstallConfig(filename);
    System.out.println(config.toString());
  }
}
