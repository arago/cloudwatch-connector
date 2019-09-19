package de.arago.connector.cloudwatch;

import co.arago.hiro.client.api.HiroClient;
import co.arago.hiro.client.builder.ClientBuilder;
import co.arago.hiro.client.builder.TokenBuilder;
import co.arago.hiro.client.util.HiroException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.minidev.json.JSONValue;

public class CloudWatchSQSWorker implements Closeable, Runnable {

  private static final Logger LOG = Logger.getLogger(CloudWatchSQSWorker.class.getName());

  private static final String VARIABLE_PROCESS_CLOUDWATCH_EVENT = "ProcessCloudWatchEvent";
  private static final String VARIABLE_AWSSQS_ATRIBUTES = "AWSSQSAttributes";
  private static final String VARIABLE_AWSSQS_BODY = "AWSSQSBody";

  private boolean isEnabled;

  private String authUrl;
  private String authUser;
  private String authPasswd;
  private String authClientId;
  private String authClientSecret;

  private String graphitUrl;

  private String awsKey;
  private String awsSecret;
  private String queueUrl;
  private int sqsWaitTimeout;
  private int sqsMessages;
  private int sqsMaxConnections;

  private String modelMachineNodePrefix;
  private String modelDefaultNodeId;

  private final Map<String, Set<String>> skipTransitions = new ConcurrentHashMap();

  private AmazonSQSBufferedAsyncClient bufferedSQS;
  private HiroClient hiro;
  private Thread worker;

  public void configure(final YamlConfig c) {
    isEnabled = c.get("sqs.enabled", true);
    if (!isEnabled) {
      return;
    }

    awsKey = c.get("aws.AWS_ACCESS_KEY", "");
    awsSecret = c.get("aws.AWS_SECRET_KEY", "");

    queueUrl = c.get("sqs.url", "");

    if (queueUrl.isEmpty()) {
      throw new IllegalArgumentException("config does not contain sqs queueUrl");
    }

    graphitUrl = c.get("graphit.url", "");

    if (graphitUrl.isEmpty()) {
      throw new IllegalArgumentException("config does not contain graphit options");
    }

    authUrl = c.get("auth.url", "");
    authUser = c.get("auth.username", "");
    authPasswd = c.get("auth.passwd", "");
    authClientId = c.get("auth.clientId", "");
    authClientSecret = c.get("auth.clientSecret", "");

    sqsWaitTimeout = c.get("sqs.timeout", 10);
    sqsMessages = c.get("sqs.messages", 10);
    sqsMaxConnections = c.get("sqs.connections", 50);

    modelMachineNodePrefix = c.get("model.machine-node-prefix", "");
    modelDefaultNodeId = c.get("model.default-node-id", "");

    List<Map> transforms = (List) c.get("sqs.skip-status-transitions");
    if (transforms != null) {
      for (Map<String, String> sub : transforms) {
        String from = sub.get("from");
        String to = sub.get("to");
        if (from != null && to != null) {
          Set<String> l = skipTransitions.get(from);
          if (l != null) {
            l.add(to);
          } else {
            l = new HashSet();
            l.add(to);
            skipTransitions.put(from, l);
          }
        }
      }
    }
    LOG.log(Level.FINE, "skip-status-transitions={0}", skipTransitions);
  }

  public void start() {
    if (!isEnabled) {
      return;
    }

    ClientBuilder builder = new ClientBuilder()
      .setRestApiUrl(graphitUrl);

    builder.setTokenProvider(new TokenBuilder().makePassword(authUrl, authClientId, authClientSecret, authUser, authPasswd));

    hiro = builder.makeHiroClient();

    checkDefaultNode();
    initializeVariables();

    final ClientConfiguration clientConfiguration = new ClientConfiguration();
    clientConfiguration.withMaxConnections(sqsMaxConnections);

    final AmazonSQSAsync sqsAsync;
    if (awsKey.isEmpty() || awsSecret.isEmpty()) {
      sqsAsync = new AmazonSQSAsyncClient(clientConfiguration);
    } else {
      sqsAsync = new AmazonSQSAsyncClient(new BasicAWSCredentials(awsKey, awsSecret));
    }

    bufferedSQS = new AmazonSQSBufferedAsyncClient(sqsAsync);

    try {
      GetQueueAttributesRequest req = new GetQueueAttributesRequest()
        .withQueueUrl(queueUrl);
      GetQueueAttributesResult queueAttributes = bufferedSQS.getQueueAttributes(req);
      LOG.log(Level.FINE, "aws: {0}", queueAttributes.getAttributes());
    } catch (Throwable t) {
      throw new IllegalStateException("could not connect to aws", t);
    }

    worker = new Thread(this);
    worker.start();
  }

  private void checkDefaultNode() {
    try {
      Map v = hiro.getVertex(modelDefaultNodeId, new HashMap());
      LOG.log(Level.FINE, "default node: {0}", v);
    } catch (HiroException t) {
      throw new IllegalStateException("default model node not found, id=[" + modelDefaultNodeId + "]", t);
    }
  }

  private void initializeVariables() {
    try {
      Map variable = hiro.getVariable(VARIABLE_PROCESS_CLOUDWATCH_EVENT);
      LOG.log(Level.FINE, "varaible: {0}", variable);
    } catch (Throwable t) {
      LOG.log(Level.FINE, "can not get varaible", t);

      hiro.setVariable(VARIABLE_PROCESS_CLOUDWATCH_EVENT, "Trigger processing of CloudWatchEvent", true);
      hiro.setVariable(VARIABLE_AWSSQS_ATRIBUTES, "Attributes of SQS message", false);
      hiro.setVariable(VARIABLE_AWSSQS_BODY, "Body of SQS message", false);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      worker.interrupt();
    } catch (Throwable t) {
      LOG.log(Level.SEVERE, null, t);
    }
  }

  @Override
  public void run() {
    while (!Thread.currentThread().isInterrupted()) {

      ReceiveMessageResult rx;
      try {
        ReceiveMessageRequest receiveRq = new ReceiveMessageRequest()
          .withMaxNumberOfMessages(sqsMessages)
          .withWaitTimeSeconds(sqsWaitTimeout)
          .withMessageAttributeNames("All")
          .withQueueUrl(queueUrl);
        rx = bufferedSQS.receiveMessage(receiveRq);
      } catch (Throwable t) {
        LOG.log(Level.SEVERE, "error while receiving messages", t);
        continue;
      }

      for (Message m : rx.getMessages()) {
        if (Thread.currentThread().isInterrupted()) {
          break;
        }

        try {
          if (process(m)) {
            bufferedSQS.deleteMessage(queueUrl, m.getReceiptHandle());
          }
        } catch (Exception ex) {
          LOG.log(Level.WARNING, "could not process message: " + m.getBody(), ex);
        }
      }
    }
  }

  private boolean process(final Message m) throws Exception {
    LOG.log(Level.FINEST, "processing message : {0} : {1} : {2}", new Object[]{m.getMessageId(), m.getMessageAttributes(), m.getBody()});

    final CloudWatchAlarmMessage msg = new CloudWatchAlarmMessage(m);

    LOG.log(Level.FINEST, "parsed message: {0}", msg.toString());

    if (skipTransitions.containsKey(msg.getOldStateValue()) && skipTransitions.get(msg.getOldStateValue()).contains(msg.getNewStateValue())) {
      LOG.log(Level.FINE, "skipping event bcs of defined transition type: {0}", m.toString());
      return true;
    }

    return createIssue(msg);
  }

  private boolean createIssue(CloudWatchAlarmMessage msg) throws Exception {

    String nodeId = modelDefaultNodeId;
    if (msg.getInstanceId() == null || msg.getInstanceId().isEmpty()) {
      if (modelDefaultNodeId.isEmpty()) {
        LOG.log(Level.WARNING, "skipping issue creation: missing instanceId in attributes");
        return true;
      }
    } else if (!modelMachineNodePrefix.isEmpty()) {
      nodeId = modelMachineNodePrefix + msg.getInstanceId();
    }

    try {
      hiro.getVertex(nodeId, new HashMap());
    } catch (HiroException t) {
      if (t.getCode() == 404) {
        LOG.log(Level.WARNING, "node for issue does not exists: {0}, using default: {1}", new Object[]{nodeId, modelDefaultNodeId});
        nodeId = modelDefaultNodeId;
      }
    }

    final Map v = new HashMap();
    v.put(VARIABLE_PROCESS_CLOUDWATCH_EVENT, "");
    v.put(VARIABLE_AWSSQS_ATRIBUTES, JSONValue.toJSONString(msg.getAttributes()));
    v.put(VARIABLE_AWSSQS_BODY, msg.getBody());
    v.put("ogit/Automation/originNode", nodeId);
    v.put("ogit/subject", msg.getSubject());
    
    if (LOG.isLoggable(Level.FINE)) {
      LOG.log(Level.FINE, "issue: {0}", v);
    }
    
    try {
      Map createVertexResp = hiro.createVertex("ogit/Automation/AutomationIssue", v, new HashMap());
      LOG.log(Level.FINE, "issue vertex: {0}", createVertexResp);
      LOG.log(Level.INFO, "created issue vertex: {0}", createVertexResp.get("ogit/_id"));
    } catch (Throwable t) {
      LOG.log(Level.FINE, "could not create issue vertex: " + v, t);
      return false;
    }

    return true;
  }
}
