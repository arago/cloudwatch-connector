package de.arago.connector.cloudwatch;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.yaml.snakeyaml.Yaml;

public class CloudWatchMain {

  private static final Logger LOG = Logger.getLogger(CloudWatchMain.class.getName());

  private final CountDownLatch latch = new CountDownLatch(1);

  private CloudWatchMain() {
  }

  private void run() throws Exception {
    final Map c;
    try (InputStream in = new FileInputStream(new File("/opt/arago/conf/cloudwatch-connector.yaml"));)
    {
      c = new Yaml().load(in);
    }
    
    final YamlConfig config = new YamlConfig(c);
    
    final CloudWatchSQSWorker sqs = new CloudWatchSQSWorker();

    sqs.configure(config);
    sqs.start();

    final CloudWatchMonitorWorker monitoring = new CloudWatchMonitorWorker();

    monitoring.configure(config);
    monitoring.start();

    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        LOG.info("[WORKER] stopping ...");
        try {
          sqs.close();
          monitoring.close();
        } catch (IOException ex) {
          LOG.log(Level.SEVERE, null, ex);
        }
        LOG.info("[WORKER] stopped");
      }
    }));

    LOG.log(Level.INFO, "[WORKER] started");

    latch.await();
  }

  public static void main(String[] args) throws Exception {
    String prop = System.getProperty("log4j.configuration");
    if (prop == null) {
      prop = System.getProperty("log4j.properties", "/opt/arago/conf/cloudwatch-connector-log4j.properties");
    }

    File configFile = new File(prop);
    if (configFile.canRead()) {
      PropertyConfigurator.configure(configFile.getAbsolutePath());
    } else {
      LOG.log(Level.SEVERE, "can not configure log4j. Can not read config file: {0}", prop);
    }

    final CloudWatchMain main = new CloudWatchMain();
    try {
      main.run();
    } catch (Throwable t) {
      LOG.log(Level.SEVERE, "error while starting", t);
      System.exit(-1);
    }
  }
}
