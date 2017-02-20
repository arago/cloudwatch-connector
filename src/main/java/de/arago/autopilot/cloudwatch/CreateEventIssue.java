package de.arago.autopilot.cloudwatch;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import de.arago.autopilot.client.api.graph.NodeData;
import de.arago.autopilot.client.api.issue.IssueAPI;
import de.arago.autopilot.client.util.impl.InMemoryNodeData;
import de.arago.autopilot.client.util.issue.Issues;
import de.arago.commons.configuration.Config;
import de.arago.commons.configuration.ConfigFactory;
import java.util.HashMap;
import java.util.Map;

public class CreateEventIssue extends BaseCommand {

    @Parameter(names = {"-k", "--aws-key"}, description = "The aws access key")
    public String awskey = "";

    @Parameter(names = {"-s", "--aws-secret"}, description = "The aws access secret")
    public String awssecret = "";

    @Parameter(names = {"-q", "--sqs-url"}, description = "The sqs queue url")
    public String queueUrl = "";    

    @Override
    protected String command() {
        return "autopilot-cloudwatch-connector";
    }

    @Override
    protected int runInnerLogic(JCommander commandline) {

        Config c = ConfigFactory.open("autopilot-cloudwatch-connector");
        if(awskey.isEmpty()) {
            awskey = c.getOr("AWS_ACCESS_KEY", "");
        }
        if(awssecret.isEmpty()) {
            awssecret = c.getOr("AWS_SECRET_KEY", "");
        }
        if(queueUrl.isEmpty()) {
            queueUrl = c.getOr("sqs-url", "");
        }
        if (uri == null) {
            uri = c.getOr("aae.endpoint", "");
            if(uri.isEmpty()){
                usage("Error: no Endpoint was resolved. An Endpoint URL is expected after -u", commandline);
                return 1;
            }
        }
        String marsNode = c.getOr("MARSModel.Application.AutoPilot_Infrastructure", "cloudpilot:Prod:Application:CloudPilot");

        IssueAPI i_api = connect(uri).getIssueAPI();
        if (!token.isEmpty()) {
            i_api.setToken(token);
        }
        
// Create the basic Amazon SQS async client
        AmazonSQSAsync sqsAsync;
        if(awskey.isEmpty()||awssecret.isEmpty()){
            sqsAsync = new AmazonSQSAsyncClient();
        } else {
            sqsAsync = new AmazonSQSAsyncClient(new BasicAWSCredentials(awskey, awssecret));
        }

// Create the buffered client
        AmazonSQSAsync bufferedSqs = new AmazonSQSBufferedAsyncClient(sqsAsync);

        while(true){
            ReceiveMessageRequest receiveRq = new ReceiveMessageRequest()
                    .withMaxNumberOfMessages(100)
                    .withQueueUrl(queueUrl);
            ReceiveMessageResult rx = bufferedSqs.receiveMessage(receiveRq);
            for( Message m : rx.getMessages()){
                try {
                    HashMap map = new HashMap();
                    map.put("NodeID", marsNode);
                    map.put("xmlns", "https://graphit.co/schemas/v2/IssueSchema");
//                    map.put("xmlns", "http://www.arago.de/IssueSchema");                    
//                    StringReader r = new StringReader(m.getBody());
//                    Map body = (Map)JSONValue.parse(m.getBody());
//                    map.put("CloudWatchEvent", body);
                    map.put("CloudWatchEvent", m.getBody());
                    NodeData issue = readFromMap("Issue", map);
                    String issue_id = i_api.write(Issues.toXML(issue));
                    System.out.println("Issue created: " + issue_id);
                    bufferedSqs.deleteMessage(queueUrl, m.getReceiptHandle());
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        System.exit(new CreateEventIssue().run(args));
    }
    
    private static NodeData readFromMap(String name, Map<Object, Object> map) {
        InMemoryNodeData result = new InMemoryNodeData();
        result.setName(name);
        for (Map.Entry entry : map.entrySet()) {
            Object v = entry.getValue();
            if (v instanceof Map) {
                result.getSubitems().add(readFromMap(entry.getKey().toString(), (Map) v));
            } else {
                result.getAttributes().put(entry.getKey().toString(), v.toString());
            }
        }
        return result;
    }
}
