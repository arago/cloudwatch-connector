package de.arago.connector.cloudwatch;

import co.arago.hiro.client.api.HiroClient;
import co.arago.hiro.client.builder.ClientBuilder;
import co.arago.hiro.client.builder.TokenBuilder;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import de.arago.commons.configuration.Config;
import de.arago.commons.xmltools.XMLHelper;
import de.arago.graphit.api.exception.GraphitException;
import de.arago.graphit.api.util.GraphitCollections;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import net.minidev.json.JSONValue;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

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

  private AmazonSQSBufferedAsyncClient bufferedSQS;
  private HiroClient hiro;
  private Thread worker;

  public void configure(final Config c) {
    isEnabled = Boolean.parseBoolean(c.getOr("sqs.enabled", "true"));
    if (!isEnabled) {
      return;
    }

    awsKey = c.getOr("aws.AWS_ACCESS_KEY", "");
    awsSecret = c.getOr("aws.AWS_SECRET_KEY", "");

    queueUrl = c.getOr("sqs.url", "");

    if (queueUrl.isEmpty()) {
      throw new IllegalArgumentException("config does not contain sqs queueUrl");
    }

    graphitUrl = c.getOr("graphit.url", "");

    if (graphitUrl.isEmpty()) {
      throw new IllegalArgumentException("config does not contain graphit options");
    }

    authUrl = c.getOr("auth.url", "");
    authUser = c.getOr("auth.username", "");
    authPasswd = c.getOr("auth.passwd", "");
    authClientId = c.getOr("auth.clientId", "");
    authClientSecret = c.getOr("auth.clientSecret", "");

    sqsWaitTimeout = Integer.parseInt(c.getOr("sqs.timeout", "10"));
    sqsMessages = Integer.parseInt(c.getOr("sqs.messages", "10"));
    sqsMaxConnections = Integer.parseInt(c.getOr("sqs.connections", "50"));

    modelMachineNodePrefix = c.getOr("model.machine-node-prefix", "");
    modelDefaultNodeId = c.getOr("model.default-node-id", "");
  }

  public void start() {
    if (!isEnabled) {
      return;
    }

    // Create HIRO client
    ClientBuilder builder = new ClientBuilder()
      .setRestApiUrl(graphitUrl);

    if (authUser.isEmpty()) {
      builder.setTokenProvider(new TokenBuilder().makeClientCredentials(authUrl, authClientId, authClientSecret));
    } else {
      builder.setTokenProvider(new TokenBuilder().makePassword(authUrl, authClientId, authClientSecret, authUser, authPasswd));
    }

    hiro = builder.makeHiroClient();

    try {
      Map info = hiro.info();
      LOG.log(Level.FINE, "graphit: {0}", info);
    } catch (Throwable t) {
      throw new IllegalStateException("could not connect to graphit", t);
    }

    checkDefaultNode();
    initilaizeVariables();

    // Create the basic Amazon SQS async client
    final ClientConfiguration clientConfiguration = new ClientConfiguration();
    clientConfiguration.withMaxConnections(sqsMaxConnections);

    final AmazonSQSAsync sqsAsync;
    if (awsKey.isEmpty() || awsSecret.isEmpty()) {
      sqsAsync = new AmazonSQSAsyncClient(clientConfiguration);
    } else {
      sqsAsync = new AmazonSQSAsyncClient(new BasicAWSCredentials(awsKey, awsSecret));
    }

    // Create the buffered SQS client
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
      Map v = hiro.getVertex(modelDefaultNodeId);
      LOG.log(Level.FINE, "default node: {0}", v);
    } catch (GraphitException t) {
      throw new IllegalStateException("default model node not found, id=[" + modelDefaultNodeId + "]", t);
    }
  }

  private void initilaizeVariables() {
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
    return createIssue(getInstanceId(m.getBody()), m);
  }

  private boolean createIssue(String instanceId, Message m) throws Exception {
    String nodeId = modelDefaultNodeId;
    if (instanceId == null || instanceId.isEmpty()) {
      if (modelDefaultNodeId.isEmpty()) {
        LOG.log(Level.WARNING, "skipping issue creation: missing instanceId in attributes");
        return true;
      }
    } else if (!modelMachineNodePrefix.isEmpty()) {
      nodeId = modelMachineNodePrefix + instanceId;
    }

    try {
      hiro.getVertex(nodeId);
    } catch (GraphitException t) {
      if (t.getCode() == 404) {
        LOG.log(Level.FINE, "node for issue does not exists: {0}", nodeId);
        nodeId = modelDefaultNodeId;
      }
    }

    String issueXml = toIssueXML(m, nodeId);

    LOG.log(Level.FINE, "issue xml: {0}", issueXml);

    final Map v = GraphitCollections.newMap();
    v.put("ogit/Automation/issueFormalRepresentation", issueXml);

    try {
      Map createVertexResp = hiro.createVertex("ogit/Automation/AutomationIssue", v);
      LOG.log(Level.FINE, "issue vertex: {0}", createVertexResp);
      LOG.log(Level.INFO, "created issue vertex: {0}", createVertexResp.get("ogit/_id"));
    } catch (Throwable t) {
      LOG.log(Level.FINE, "could not create issue vertex: " + v, t);
      return false;
    }

    return true;
  }

  private String toIssueXML(Message m, String nodeId) throws Exception {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = dbf.newDocumentBuilder();
    Document doc = builder.newDocument();
    Element xml = getElement(m, nodeId, doc);

    return XMLHelper.toXML(xml);
  }

  private Element getElement(Message m, String nodeId, Document doc) {
    Element el = doc.createElement("Issue");
    el.setAttribute("NodeID", nodeId);
    el.setAttribute("xmlns", "https://graphit.co/schemas/v2/IssueSchema");
    el.setAttribute("IssueSubject", getSubjectFromBody(m.getBody()));

    Element el1 = doc.createElement(VARIABLE_PROCESS_CLOUDWATCH_EVENT);
    Element el1c = doc.createElement("Content");
    el1c.setAttribute("Value", "");
    el1.appendChild(el1c);
    el.appendChild(el1);

    Element el2 = doc.createElement(VARIABLE_AWSSQS_ATRIBUTES);
    Element el2c = doc.createElement("Content");
    el2c.setAttribute("Value", JSONValue.toJSONString(parseMessageAttributes(m.getMessageAttributes())));
    el2.appendChild(el2c);
    el.appendChild(el2);

    Element el3 = doc.createElement(VARIABLE_AWSSQS_BODY);
    Element el3c = doc.createElement("Content");
    el3c.setAttribute("Value", getMessageFromBody(m.getBody()));
    el3.appendChild(el3c);
    el.appendChild(el3);

    return el;
  }

  private Map parseMessageAttributes(Map<String, MessageAttributeValue> messageAttributes) {
    final Map ret = GraphitCollections.newMap();
    for (Map.Entry<String, MessageAttributeValue> attr : messageAttributes.entrySet()) {
      ret.put(attr.getKey(), attr.getValue().getStringValue());
    }

    return ret;
  }

  public String getInstanceId(String body) {
    Object o = JSONValue.parse(body);
    if (o instanceof Map) {
      Object o0 = ((Map) o).get("Message");
      if (o0 instanceof Map) {
        Object o1 = ((Map) o0).get("Trigger");
        if (o1 instanceof Map) {
          Object o2 = ((Map) o1).get("Dimensions");
          if (o2 instanceof List) {
            for (Object d : (List) o2) {
              if (d instanceof Map) {
                Map dim = (Map) d;
                String n = dim.get("name") + "";
                if (n.equals("InstanceId")) {
                  return dim.get("value") + "";
                }
              }
            }
          }
        }
      }
    }
    return "";
  }

  private String getSubjectFromBody(String body) {
    Object o = JSONValue.parse(body);
    if (o instanceof Map) {
      Map mp = (Map) o;
      return mp.get("Subject") + "";
    }
    return "";
  }

  private String getMessageFromBody(String body) {
    Object o = JSONValue.parse(body);
    if (o instanceof Map) {
      Map mp = (Map) o;
      return mp.get("Message") + "";
    }
    return "";
  }
}
