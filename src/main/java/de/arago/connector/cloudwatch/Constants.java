/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.arago.connector.cloudwatch;

/**
 *
 */
public interface Constants {
  String NAMESPACE_SEPERATOR = "/";
  String NAMESPACE_OGIT = "ogit" + NAMESPACE_SEPERATOR;
  
  interface Attributes {
    String OGIT__ID = NAMESPACE_OGIT + "_id";
    String OGIT_NAME = NAMESPACE_OGIT + "name";
  }
  
  interface Entities {
    String OGIT_TIMESERIES = NAMESPACE_OGIT + "Timeseries";
  }
}
