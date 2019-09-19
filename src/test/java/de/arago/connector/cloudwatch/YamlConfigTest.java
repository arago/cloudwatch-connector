/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.arago.connector.cloudwatch;

import java.util.List;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

/**
 *
 */
public class YamlConfigTest {
  
  public YamlConfigTest() {
  }
  
  @Test
  public void testGet_String() {
    Map data = new Yaml().load(YamlConfig.class.getResourceAsStream("/cloudwatch-connector.yaml"));
    
    YamlConfig config = new YamlConfig(data);
    
    assertEquals(true, config.get("cloudwatch.enabled"));
    assertTrue(config.get("cloudwatch.namespaces") instanceof List);
    
    List items = config.get("cloudwatch.namespaces");
    assertEquals(3, items.size());
    
    assertNull(config.get("cloudwatch.bla"));
    assertEquals("default", config.get("cloudwatch.bla", "default"));
    assertEquals(15, (int) config.get("cloudwatch.bla", 15));
    assertEquals(300, (int) config.get("cloudwatch.poll-interval-sec"));
  }
}
