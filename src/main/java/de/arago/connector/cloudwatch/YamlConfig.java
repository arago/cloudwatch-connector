/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.arago.connector.cloudwatch;

import static co.arago.hiro.client.util.Helper.notEmpty;
import java.util.Map;

/**
 *
 */
public final class YamlConfig {
  private final Map<String, ?> data;

  public YamlConfig(Map data) {
    this.data = data;
  }
  
  public <T> T get(String what) {
    notEmpty(what, "what");
    Map here     = data;
    Object value = null;
    
    for (final String part: what.split("\\."))
    {
      if (here == null) {
        break;
      }
      
      value = here.get(part);
      
      if (value instanceof Map){
        here = (Map) value;
      }
    }
    
    return (T) value;
  }
  
  public <T> T get(String what, Object orElse) {
    Object ret = get(what);
    return (T) (ret == null?orElse:ret);
  }
}
