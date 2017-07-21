package de.arago.connector.cloudwatch;

import java.util.List;
import java.util.Map;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 */
public class CloudWatchSQSWorkerTest {

  public CloudWatchSQSWorkerTest() {
  }

  @Test
  public void testMessageParse() {
    String body = "{\"Message\":{\n"
      + "	\"AlarmName\": \"global-graph-process-status-Logstash-elkstack-i-xxxx\",\n"
      + "	\"AlarmDescription\": null,\n"
      + "	\"AWSAccountId\": \"0000\",\n"
      + "	\"NewStateValue\": \"OK\",\n"
      + "	\"NewStateReason\": \"Threshold Crossed: 1 datapoint [1.0 (20/07/17 15:03:00)] was not less than the threshold (1.0).\",\n"
      + "	\"StateChangeTime\": \"2017-07-20T15:04:35.978+0000\",\n"
      + "	\"Region\": \"EU - Ireland\",\n"
      + "	\"OldStateValue\": \"INSUFFICIENT_DATA\",\n"
      + "	\"Trigger\": {\n"
      + "		\"MetricName\": \"ProcessStatus\",\n"
      + "		\"Namespace\": \"System/Linux\",\n"
      + "		\"StatisticType\": \"Statistic\",\n"
      + "		\"Statistic\": \"AVERAGE\",\n"
      + "		\"Unit\": \"Count\",\n"
      + "		\"Dimensions\": [{\n"
      + "			\"name\": \"ProcessName\",\n"
      + "			\"value\": \"Logstash\"\n"
      + "		}, {\n"
      + "			\"name\": \"InstanceId\",\n"
      + "			\"value\": \"i-xxxx\"\n"
      + "		}],\n"
      + "		\"Period\": 60,\n"
      + "		\"EvaluationPeriods\": 1,\n"
      + "		\"ComparisonOperator\": \"LessThanThreshold\",\n"
      + "		\"Threshold\": 1.0,\n"
      + "		\"TreatMissingData\": \"\",\n"
      + "		\"EvaluateLowSampleCountPercentile\": \"\"\n"
      + "	}"
      + "	}"
      + "}";

    final CloudWatchAlarmMessage m = new CloudWatchAlarmMessage(body);

    assertEquals("i-xxxx", m.getInstanceId());
    assertEquals("INSUFFICIENT_DATA", m.getOldStateValue());
    assertEquals("OK", m.getNewStateValue());

    final List<Map> dims = m.getDimensions();
    assertEquals(2, dims.size());
    boolean foundInstanceId = false;
    for (Map d : dims) {
      if (d.get("name").equals("InstanceId") && d.get("value").equals("i-xxxx")) {
        foundInstanceId = true;
      }
    }
    assertEquals(true, foundInstanceId);
  }
}
