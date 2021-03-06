package de.arago.connector.cloudwatch;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.minidev.json.JSONValue;

public class CloudWatchAlarmMessage {

  private String body;
  private final Map attributes = new HashMap();
  private final List<Map> dimmensions = new ArrayList();

  private String instanceId;
  private String newState;
  private String oldState;
  private String subject;

  public CloudWatchAlarmMessage(String body) {
    parseBody(body);
  }

  CloudWatchAlarmMessage(final Message m) {
    parseMessageAttributes(m.getMessageAttributes());
    parseBody(m.getBody());
  }

  public Map getAttributes() {
    return attributes;
  }

  public String getBody() {
    return body;
  }

  public String getSubject() {
    return subject;
  }

  String getInstanceId() {
    return instanceId;
  }

  String getOldStateValue() {
    return oldState;
  }

  String getNewStateValue() {
    return newState;
  }

  List getDimensions() {
    return dimmensions;
  }

  @Override
  public String toString() {
    return toMap() + "";
  }

  private Map toMap() {
    final Map ret = new HashMap();
    ret.put("instanceId", instanceId);
    ret.put("subject", subject);
    ret.put("oldState", oldState);
    ret.put("newState", newState);
    ret.put("dimmensions", dimmensions);
    ret.put("attributes", attributes);
    return ret;
  }

  private void parseBody(String body) {
    Object o = JSONValue.parse(body);
    if (o instanceof Map) {
      Map root = (Map) o;
      subject = root.get("Subject") + "";

      Object o0 = root.get("Message");
      this.body = o0 + "";
      if (o0 instanceof String) {
        o0 = JSONValue.parse(o0 + "");
      }

      if (o0 instanceof Map) {
        Map message = (Map) o0;

        newState = message.get("NewStateValue") + "";
        oldState = message.get("OldStateValue") + "";

        Object o1 = message.get("Trigger");
        if (o1 instanceof Map) {
          Object o2 = ((Map) o1).get("Dimensions");
          if (o2 instanceof List) {
            for (Object d : (List) o2) {
              if (d instanceof Map) {
                Map dim = (Map) d;
                dimmensions.add(dim);
                String n = dim.get("name") + "";
                if (n.equals("InstanceId")) {
                  instanceId = dim.get("value") + "";
                }
              }
            }
          }
        }
      }
    }
  }

  private void parseMessageAttributes(Map<String, MessageAttributeValue> messageAttributes) {
    for (Map.Entry<String, MessageAttributeValue> attr : messageAttributes.entrySet()) {
      attributes.put(attr.getKey(), attr.getValue().getStringValue());
    }
  }

}
