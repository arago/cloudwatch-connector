package de.arago.connector.cloudwatch;

import co.arago.hiro.client.api.HiroClient;
import co.arago.hiro.client.builder.ClientBuilder;
import co.arago.hiro.client.builder.TokenBuilder;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import de.arago.commons.configuration.Config;
import de.arago.commons.configuration.ConfigFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CloudWatchWorker implements Closeable, Runnable {

  private static final Logger LOG = Logger.getLogger(CloudWatchWorker.class.getName());

  private String authUrl;
  private String authUser;
  private String authPasswd;
  private String authClientId;
  private String authClientSecret;

  private String awsKey;
  private String awsSecret;
  private String queueUrl;
  private String graphitUrl;
  private AmazonSQSBufferedAsyncClient bufferedSQS;
  private HiroClient hiro;
  private Thread worker;
  private int sqsWaitTimeout;
  private int sqsMessages;
  private boolean skipCertsCheck;

  public CloudWatchWorker() {
  }

  public void configure() {
    final Config c = ConfigFactory.open("cloudwatch-connector");

    awsKey = c.get("aws.AWS_ACCESS_KEY");
    awsSecret = c.get("aws.AWS_SECRET_KEY");
    queueUrl = c.get("aws.sqs-url");

    if (awsKey.isEmpty() || awsSecret.isEmpty() || queueUrl.isEmpty()) {
      LOG.log(Level.SEVERE, "config does not contain aws access options");
      throw new IllegalArgumentException();
    }

    graphitUrl = c.get("graphit.url");

    if (graphitUrl.isEmpty()) {
      LOG.log(Level.SEVERE, "config does not contain graphit options");
      throw new IllegalArgumentException();
    }

    authUrl = c.get("auth.url");
    authUser = c.get("auth.username");
    authPasswd = c.get("auth.passwd");
    authClientId = c.get("auth.clientId");
    authClientSecret = c.get("auth.clientSecret");

    sqsWaitTimeout = Integer.parseInt(c.getOr("aws.sqs-timeout", "30"));
    sqsMessages = Integer.parseInt(c.getOr("aws.sqs-messages", "100"));

    skipCertsCheck = Boolean.parseBoolean(c.getOr("skip-certs-validation", "false"));
  }

  public void start() {
    // Create the basic Amazon SQS async client
    final AmazonSQSAsync sqsAsync;
    if (awsKey.isEmpty() || awsSecret.isEmpty()) {
      sqsAsync = new AmazonSQSAsyncClient();
    } else {
      sqsAsync = new AmazonSQSAsyncClient(new BasicAWSCredentials(awsKey, awsSecret));
    }

    ClientBuilder builder = new ClientBuilder()
      .setRestApiUrl(graphitUrl)
      .setTrustAllCerts(skipCertsCheck);

    if (authUser.isEmpty()) {
      builder.setTokenProvider(new TokenBuilder().makeClientCredentials(authUrl, authClientId, authClientSecret));
    } else {
      builder.setTokenProvider(new TokenBuilder().makePassword(authUrl, authClientId, authClientSecret, authUser, authPasswd));
    }

    hiro = builder.makeHiroClient();

    hiro.info();

    // Create the buffered client
    bufferedSQS = new AmazonSQSBufferedAsyncClient(sqsAsync);

    worker = new Thread(this);
    worker.start();
  }

  @Override
  public void close() throws IOException {
    try {
      worker.interrupt();
      worker.wait(60000);
    } catch (Throwable t) {
      LOG.log(Level.SEVERE, null, t);
    }
  }

  @Override
  public void run() {
    while (!Thread.currentThread().isInterrupted()) {
      ReceiveMessageRequest receiveRq = new ReceiveMessageRequest()
        .withMaxNumberOfMessages(sqsMessages)
        .withWaitTimeSeconds(sqsWaitTimeout)
        .withQueueUrl(queueUrl);
      ReceiveMessageResult rx = bufferedSQS.receiveMessage(receiveRq);

      for (Message m : rx.getMessages()) {
        LOG.log(Level.FINEST, "received message : {0} : {1} : {2}", new Object[]{m.getMessageId(), m.getAttributes(), m.getBody()});

        try {
          process(m);
          //TODO
          //bufferedSQS.deleteMessage(queueUrl, m.getReceiptHandle());
        } catch (Exception ex) {
          LOG.log(Level.WARNING, "could not process message: " + m.getBody(), ex);
        }
      }
    }
  }

  private void process(final Message m) throws Exception {

    /*
     HashMap map = new HashMap();
     map.put("NodeID", marsNode);
     map.put("xmlns", "https://graphit.co/schemas/v2/IssueSchema");
     map.put("CloudWatchEvent", m.getBody());
     */
  }
}
