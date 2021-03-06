package de.arago.connector.cloudwatch;

import co.arago.hiro.client.api.HiroClient;
import co.arago.hiro.client.api.TimeseriesValue;
import co.arago.hiro.client.builder.ClientBuilder;
import co.arago.hiro.client.builder.TokenBuilder;
import co.arago.hiro.client.util.DefaultTimeseriesValue;
import co.arago.hiro.client.util.HiroException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.DimensionFilter;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.cloudwatch.model.ListMetricsRequest;
import com.amazonaws.services.cloudwatch.model.ListMetricsResult;
import com.amazonaws.services.cloudwatch.model.Metric;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.minidev.json.JSONValue;

public class CloudWatchMonitorWorker implements Closeable, Runnable {

  private static final Logger LOG = Logger.getLogger(CloudWatchMonitorWorker.class.getName());
  private static final String INSTANCEID = "InstanceId";
  private static final String TIMESERIES_MAIDTYPE = "CloudWatch";

  private boolean isEnabled;

  private String authUrl;
  private String authUser;
  private String authPasswd;
  private String authClientId;
  private String authClientSecret;

  private String graphitUrl;

  private String awsKey;
  private String awsSecret;

  private String monitoringEndpoint;
  private Set allowedMetricNames;
  private Set<String> namespaces;
  private final Map<String, String> knownInstanceIds = new HashMap();
  private final Map<String, Map> timeseriesMeta = new ConcurrentHashMap();
  private final Map<String, Integer> metricsPeriodities = new ConcurrentHashMap();
  private final Map<String, String> metricsTransforms = new ConcurrentHashMap();
  private int defaultPeriodity;
  private String defaultTransform;
  private int metricsPollInterval;
  private int metricsBatchSize;
  private String modelDefaultNodeId;

  private HiroClient hiro;
  private AmazonCloudWatchClient cloudwatchClient;
  private Thread worker;

  public void configure(final YamlConfig c) {
    isEnabled = c.get("cloudwatch.enabled", true);
    if (!isEnabled) {
      return;
    }

    awsKey = c.get("aws.AWS_ACCESS_KEY", "");
    awsSecret = c.get("aws.AWS_SECRET_KEY", "");

    graphitUrl = c.get("graphit.url", "");

    if (graphitUrl.isEmpty()) {
      throw new IllegalArgumentException("config does not contain graphit options");
    }

    monitoringEndpoint = c.get("cloudwatch.endpoint", "");
    defaultTransform = c.get("cloudwatch.default-transform", "Average");
    defaultPeriodity = c.get("cloudwatch.default-periodity", 180);
    metricsPollInterval = c.get("cloudwatch.poll-interval-sec", 300);
    metricsBatchSize = c.get("cloudwatch.batch-size", 500);

    allowedMetricNames = new HashSet((List) c.get("cloudwatch.metrics-names"));
    if (allowedMetricNames.isEmpty()) {
      allowedMetricNames.add("All");
    }
    LOG.log(Level.FINE, "allowed metrics names={0}", allowedMetricNames);

    namespaces = new HashSet((List) (c.get("cloudwatch.namespaces")));
    LOG.log(Level.FINE, "allowed namespaces={0}", namespaces);

    List<Map> periodities = c.get("cloudwatch.metrics-periodities");
    if (periodities != null) {
      for (Map<String, Object> sub : periodities) {
        String name = (String) sub.get("name");
        Integer period = (Integer) sub.get("periodity");
        if (name != null && period != null) {
          metricsPeriodities.put(name, period);
        }
      }
    }
    LOG.log(Level.FINE, "metrics periodities={0}", metricsPeriodities);

    List<Map> transforms = c.get("cloudwatch.metrics-transforms");
    if (transforms != null) {
      for (Map<String, String> sub: transforms) {
        String name = sub.get("name");
        String type = sub.get("type");
        if (name != null && type != null) {
          metricsTransforms.put(name, type);
        }
      }
    }
    LOG.log(Level.FINE, "metrics transforms={0}", metricsTransforms);

    authUrl = c.get("auth.url", "");
    authUser = c.get("auth.username", "");
    authPasswd = c.get("auth.passwd", "");
    authClientId = c.get("auth.clientId", "");
    authClientSecret = c.get("auth.clientSecret", "");

    modelDefaultNodeId = c.get("model.default-node-id", "");
  }

  public void start() {
    if (!isEnabled) {
      return;
    }

    // Create HIRO client
    ClientBuilder builder = new ClientBuilder()
      .setRestApiUrl(graphitUrl);

      builder.setTokenProvider(new TokenBuilder().makePassword(authUrl, authClientId, authClientSecret, authUser, authPasswd));

    hiro = builder.makeHiroClient();

    try {
      Map info = hiro.info();
      LOG.log(Level.FINE, "graphit: {0}", info);
    } catch (Throwable t) {
      throw new IllegalStateException("could not connect to graphit", t);
    }

    // Create CloudWatch client
    if (awsKey.isEmpty() || awsSecret.isEmpty()) {
      cloudwatchClient = new AmazonCloudWatchClient();
    } else {
      cloudwatchClient = new AmazonCloudWatchClient(new BasicAWSCredentials(awsKey, awsSecret));
    }

    cloudwatchClient.setEndpoint(monitoringEndpoint);

    try {
      String serviceName = cloudwatchClient.getServiceName();
      LOG.log(Level.FINE, "cloudwatch service: {0}, offset: {1}", new Object[]{serviceName, cloudwatchClient.getTimeOffset()});
    } catch (Throwable t) {
      throw new IllegalStateException("could not connect to cloudwatch", t);
    }

    worker = new Thread(this);
    worker.start();
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
      try {
        long then = new Date().getTime();
        int count = 0;

        discoverInstancesFromModel();
        discoverTimeseriesMetadata();

        for (String namespace : namespaces) {
          final List<Metric> metricsList = getMetricsList(namespace, INSTANCEID);
          LOG.log(Level.FINE, "metrics count: {0} for {1}", new Object[]{metricsList.size(), namespace});

          if (LOG.isLoggable(Level.FINEST)) {
            for (Metric metric : metricsList) {
              LOG.log(Level.FINEST, "metric: {0}", metric.toString());
            }
          }

          final Map<String, Map<String, List<Datapoint>>> metricsData = new ConcurrentHashMap<>();
          long currentTimestamp = (new Date()).getTime();

          for (final Metric metric : metricsList) {
            if (Thread.currentThread().isInterrupted()) {
              break;
            }

            final String instanceId = getInstanceId(metric.getDimensions());

            if (!metricsData.containsKey(instanceId)) {
              metricsData.put(instanceId, new ConcurrentHashMap<>());
            }

            if (!metricsData.get(instanceId).containsKey(metric.getMetricName())) {
              final String metricName = metric.getMetricName();
              long startTimestamp = calculateMetricsStart(instanceId, metricName);
              long endTimestamp = calculateMetricsEnd(metricName, currentTimestamp, startTimestamp);
              int periodity = getPeriodity(metricName);
              if (startTimestamp + 1000 * defaultPeriodity > endTimestamp) {
                continue;
              }

              try {
                final GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
                  .withNamespace(namespace)
                  .withDimensions(metric.getDimensions())
                  .withMetricName(metricName)
                  .withPeriod(periodity)
                  .withStatistics(getTransform(metricName))
                  .withStartTime(new Date(startTimestamp))
                  .withEndTime(new Date(endTimestamp));
                final GetMetricStatisticsResult result = cloudwatchClient.getMetricStatistics(request);
                final List<Datapoint> dataPoints = result.getDatapoints();
                if (dataPoints != null && !dataPoints.isEmpty()) {
                  metricsData.get(instanceId).put(metricName, dataPoints);
                  if (LOG.isLoggable(Level.FINEST)) {
                    LOG.log(Level.FINEST, "data: {0}", dataPoints);
                  }
                }
              } catch (Exception e) {
                LOG.log(Level.WARNING, "Error while getting the metrics for instanceId: " + instanceId + " metric: " + metric.getMetricName(), e);
              }
              ++count;

              storeMetricsData(metric.getDimensions(), metricsData, startTimestamp);
            }
          }
        }

        long processTime = new Date().getTime() - then;
        LOG.log(Level.INFO, "metrics processed count: {0}, time: {1} ms", new Object[]{count, processTime});

        for (int i = 0; i < metricsPollInterval; ++i) {
          if (Thread.currentThread().isInterrupted()) {
            break;
          }
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ex) {
            //blank
          }
        }
      } catch (Throwable t) {
        LOG.log(Level.WARNING, "error processing metrics", t);
        try {
          Thread.sleep(5000);
        } catch (InterruptedException ex) {
          LOG.log(Level.SEVERE, null, ex);
        }
      }
    }
  }

  private List<Metric> getMetricsList(String namespace, String filterName) {
    final List<DimensionFilter> filters = new ArrayList();
    DimensionFilter dimensionFilter = new DimensionFilter();
    dimensionFilter.withName(filterName);
    filters.add(dimensionFilter);

    final ListMetricsRequest request = new ListMetricsRequest();
    request.withNamespace(namespace);
    request.withDimensions(filters);

    ListMetricsResult listMetricsResult = cloudwatchClient.listMetrics(request);

    final List<Metric> metricList = new ArrayList();
    for (Metric metric : listMetricsResult.getMetrics()) {
      if (isKnownInstanceId(metric) && isAllowedMetricName(metric.getMetricName())) {
        metricList.add(metric);
      }
    }

    while (listMetricsResult.getNextToken() != null) {
      request.setNextToken(listMetricsResult.getNextToken());
      listMetricsResult = cloudwatchClient.listMetrics(request);
      for (Metric metric : listMetricsResult.getMetrics()) {
        if (isKnownInstanceId(metric) && isAllowedMetricName(metric.getMetricName())) {
          metricList.add(metric);
        }
      }
    }

    return metricList;
  }

  private boolean isKnownInstanceId(final Metric metric) {
    return knownInstanceIds.containsKey(getInstanceId(metric.getDimensions()));
  }

  private boolean isAllowedMetricName(String metricName) {
    return (allowedMetricNames.contains("All") || allowedMetricNames.contains(metricName));
  }

  private void discoverInstancesFromModel() {
    try {
      String query = "ogit\\/Automation\\/marsNodeType:\"Machine\" AND \\/EC2Tags:*";
      final Map qParams = new HashMap();
      qParams.put("limit", "-1");
      qParams.put("fields", Constants.Attributes.OGIT__ID);
      waitForValidToken();
      final List result = hiro.vertexQuery(query, qParams);
      LOG.log(Level.FINEST, "discovered nodes={0}", result);
      for (Object v : result) {
        Object j = JSONValue.parse("" + v);
        if (j instanceof Map) {
          String ogitId = (String) ((Map) j).get(Constants.Attributes.OGIT__ID);
          String[] s = ogitId.split(":");
          if (s.length > 3) {
            knownInstanceIds.put(s[3], s[0] + ":" + s[1] + ":" + s[2] + ":");
          }
        }
      }
    } catch (Throwable t) {
      LOG.log(Level.WARNING, "could not query for Model", t);
    }
    LOG.log(Level.FINE, "known instances: {0}", knownInstanceIds);
  }

  private void discoverTimeseriesMetadata() {
    try {
      String query = "ogit\\/_type:$ntype AND \\/MAIDType:$mtype";
      final Map qParams = new HashMap();
      qParams.put("limit", "-1");
      qParams.put("ntype", Constants.Entities.OGIT_TIMESERIES);
      qParams.put("mtype", TIMESERIES_MAIDTYPE);
      waitForValidToken();
      final List result = hiro.vertexQuery(query, qParams);
      LOG.log(Level.FINEST, "discovered timeseries meta={0}", result);
      for (Object v : result) {
        Object j = JSONValue.parse("" + v);
        if (j instanceof Map) {
          Map m = (Map) j;
          String[] s = ((String) m.get("/nodeID")).split(":");
          if (s.length > 3) {
            String instanceId = s[3];
            String dataName = (String) m.get(Constants.Attributes.OGIT_NAME);
            if (!timeseriesMeta.containsKey(instanceId)) {
              timeseriesMeta.put(instanceId, new HashMap());
            }
            timeseriesMeta.get(instanceId).put(dataName, m);
          }
        }
      }
    } catch (Throwable t) {
      LOG.log(Level.WARNING, "could not query for Model", t);
    }
    LOG.log(Level.FINE, "timeseries metadata for nodes count: {0}", timeseriesMeta.size());
  }

  private long calculateMetricsStart(String instanceId, String metricName) {
    Map inst = timeseriesMeta.get(instanceId);
    if (inst != null) {
      Object dname = inst.get(metricName);
      if (dname != null && dname instanceof Map) {
        Object to = ((Map) dname).get("/KeyValueStore.StoredTo");
        if (to != null) {
          return (1000 * Long.parseLong((String) to));
        }
      }
    }
    return (new Date()).getTime() - 1000 * (86400);
  }

  private long calculateMetricsEnd(String metricName, long currentTimestamp, long startTimestamp) {
    int periodity = getPeriodity(metricName);
    long endTimestamp = startTimestamp + 1000 * periodity * metricsBatchSize;
    if (endTimestamp > currentTimestamp) {
      endTimestamp = currentTimestamp;
    }
    return endTimestamp;
  }

  private void storeMetricsData(final List<Dimension> dimensions, Map<String, Map<String, List<Datapoint>>> metricsData, long startTimestamp) {
    for (String instanceId : metricsData.keySet()) {
      if (!timeseriesMeta.containsKey(instanceId)) {
        timeseriesMeta.put(instanceId, new HashMap());
      }
      Iterator<Map.Entry<String, List<Datapoint>>> iter = metricsData.get(instanceId).entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<String, List<Datapoint>> mData = iter.next();
        String metricName = mData.getKey();
        String fullMetricName = getFullMetricName(metricName, dimensions);
        String units = getUnits(mData.getValue());
        Object meta = timeseriesMeta.get(instanceId).get(fullMetricName);
        String tsid;
        if (meta == null) {
          tsid = createTimeseries(dimensions, instanceId, metricName, units, startTimestamp);
        } else {
          tsid = (String) ((Map) meta).get(Constants.Attributes.OGIT__ID);
        }

        if (tsid != null && !tsid.isEmpty() && !mData.getValue().isEmpty()) {
          if (writeTimeseriesValues(tsid, mData.getValue(), metricName)) {
            iter.remove();
          }
        }
      }
    }
  }

  private String createTimeseries(final List<Dimension> dimensions, String instanceId, String metricName, String units, long startTimestamp) {
    final Map params = new HashMap();
    for (Dimension d : dimensions) {
      params.put("/" + d.getName(), d.getValue() + "");
    }
    params.put("/nodeID", knownInstanceIds.get(instanceId) + instanceId);
    params.put("/DataName", metricName);
    params.put("/MAIDType", TIMESERIES_MAIDTYPE);
    params.put("/KeyValueStore.StoredFrom", (startTimestamp / 1000) + "");
    params.put("/Periodity", getPeriodity(metricName) + "");
    params.put("/Transformation", getTransform(metricName));
    if (!units.isEmpty()) {
      params.put("/Units", units);
    }

    params.put(Constants.Attributes.OGIT_NAME, getFullMetricName(metricName, dimensions));

    try {
      waitForValidToken();
      Map createVertexResp = hiro.createVertex(Constants.Entities.OGIT_TIMESERIES, params, new HashMap());
      LOG.log(Level.INFO, "created timeseries vertex: {0}", createVertexResp.get(Constants.Attributes.OGIT__ID));
      LOG.log(Level.FINEST, "created timeseries vertex: {0}", createVertexResp);
      return (String) createVertexResp.get(Constants.Attributes.OGIT__ID);
    } catch (HiroException g) {
      LOG.log(Level.WARNING, "can not create timeseries vertex: " + params, g);
    }
    return "";
  }

  private void updateTimeseries(String tsid, long storeto, String metricName) {
    final Map params = new HashMap();
    String storeToStr = (storeto / 1000) + "";
    params.put("/KeyValueStore.StoredTo", storeToStr);
    params.put("/Periodity", getPeriodity(metricName) + "");
    params.put("/Transformation", getTransform(metricName));
    waitForValidToken();
    Map updateVertexResp = hiro.updateVertex(tsid, params, new HashMap());
    LOG.log(Level.FINEST, "updated timeseries vertex: {0}", updateVertexResp);
  }

  private boolean writeTimeseriesValues(String tsid, final List<Datapoint> mData, String metricName) {
    long storeto = 0L;
    final List<TimeseriesValue> values = new ArrayList();
    for (final Datapoint val : mData) {
      if (val.getTimestamp().getTime() > storeto) {
        storeto = val.getTimestamp().getTime();
      }
      final TimeseriesValue v = new DefaultTimeseriesValue(val.getTimestamp().getTime(), val.getAverage() + "");
      values.add(v);
    }
    try {
      waitForValidToken();
      hiro.updateTsValues(tsid, values);
      LOG.log(Level.FINEST, "pushed timeseries values: {0} count={1}", new Object[]{tsid, values.size()});
      updateTimeseries(tsid, storeto, metricName);
    } catch (Throwable g) {
      LOG.log(Level.WARNING, "failed to update timeseries values for: " + tsid, g);
      return false;
    }
    return true;
  }

  private String getInstanceId(final List<Dimension> dimensions) {
    for (Dimension d : dimensions) {
      if (d.getName().equals(INSTANCEID)) {
        return d.getValue();
      }
    }
    return "";
  }

  private int getPeriodity(String metricName) {
    if (metricsPeriodities.containsKey(metricName)) {
      return metricsPeriodities.get(metricName);
    } else if (metricsPeriodities.containsKey("All")) {
      return metricsPeriodities.get("All");
    }
    return defaultPeriodity;
  }

  private String getTransform(String metricName) {
    if (metricsTransforms.containsKey(metricName)) {
      return metricsTransforms.get(metricName);
    } else if (metricsTransforms.containsKey("All")) {
      return metricsTransforms.get("All");
    }
    return defaultTransform;
  }

  private String getUnits(final List<Datapoint> mData) {
    if (!mData.isEmpty()) {
      return mData.get(0).getUnit();
    }
    return "";
  }

  private String getFullMetricName(String metricName, List<Dimension> dimensions) {
    final Map params = new HashMap();
    for (Dimension d : dimensions) {
      params.put(d.getName(), d.getValue() + "");
    }

    if (params.containsKey("MountPath")) {
      return metricName + " " + params.get("MountPath");
    } else if (params.containsKey("ProcessName")) {
      return metricName + " " + params.get("ProcessName");
    } else {
      return metricName;
    }
  }

  private void waitForValidToken() {
    while (true) {
      try {
        hiro.getVertex(modelDefaultNodeId, new HashMap());
        break;
      } catch (Throwable t) {
        LOG.log(Level.WARNING, "hiro client problem", t);
        if (t.getMessage().contains("token invalid")) {
          try {
            Thread.sleep(3000);
          } catch (InterruptedException ignored) {
          }
        } else {
          break;
        }
      }
    }
  }
}
