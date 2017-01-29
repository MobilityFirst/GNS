
package edu.umass.cs.gnsserver.installer;

import edu.umass.cs.gnsserver.main.GNSConfig;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class InstallConfig {

  private static final String USER_NAME = "userName";
  private static final String KEY_FILE = "keyFile";
  private static final String HOST_TYPE = "hostType";
  private static final String DATA_STORE_NAME = "dataStoreName";
  private static final String INSTALL_PATH = "installPath";
  private static final String JAVA_COMMAND = "javaCommand";

  private String username;
  private String keyFile;
  private String hostType;
  private DataStoreType dataStoreType;
  private String installPath;
  private String javaCommand;


  public String getUsername() {
    return username;
  }


  public String getKeyFile() {
    return keyFile;
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


  public String getJavaCommand() {
    return javaCommand;
  }


  public InstallConfig(String filename) {
    try {
      loadPropertiesFile(filename);
    } catch (IOException e) {
      GNSConfig.getLogger().severe("Problem loading installer config file: " + e);
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
    this.keyFile = properties.getProperty(KEY_FILE);
    this.hostType = properties.getProperty(HOST_TYPE, "linux");
    this.dataStoreType = DataStoreType.valueOf(properties.getProperty(DATA_STORE_NAME, GNSInstaller.DEFAULT_DATA_STORE_TYPE.name()).toUpperCase());
    this.installPath = properties.getProperty(INSTALL_PATH);
    this.javaCommand = properties.getProperty(JAVA_COMMAND);
  }

  @Override
  public String toString() {
    return "InstallConfig{" + "username=" + username + ", keyFile=" + keyFile + ", hostType=" + hostType + ", dataStoreType=" + dataStoreType + ", installPath=" + installPath + ", javaCommand=" + javaCommand + '}';
  }


  public static void main(String[] args) {
    String filename = "/Users/westy/Documents/Code/GNS/conf/ec2_small/installer_config";
    InstallConfig config = new InstallConfig(filename);
    System.out.println(config.toString());
  }
}
