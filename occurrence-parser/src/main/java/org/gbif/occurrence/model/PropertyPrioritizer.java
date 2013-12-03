/*
 * Copyright 2011 Global Biodiversity Information Facility (GBIF)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.model;

import org.gbif.occurrence.constants.PrioritizedPropertyNameEnum;
import org.gbif.occurrence.parsing.xml.PrioritizedProperty;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In schemas that have multiple representations of the same field (eg decimal latitude vs text latitude) this class
 * gives a framework for setting the order of preference of fields for resolving cases where more than one of them
 * is populated.
 */
public abstract class PropertyPrioritizer {

  private static final Logger LOG = LoggerFactory.getLogger(PropertyPrioritizer.class);


  protected Map<PrioritizedPropertyNameEnum, Set<PrioritizedProperty>> prioritizedProps =
    new HashMap<PrioritizedPropertyNameEnum, Set<PrioritizedProperty>>();

  public abstract void resolvePriorities();

  public void addPrioritizedProperty(PrioritizedProperty prop) {
    if (LOG.isDebugEnabled()) LOG.debug(">> addPrioritizedProperty [{}]", prop.debugDump());

    if (prop.getName() != null) {
      Set<PrioritizedProperty> nameProps = prioritizedProps.get(prop.getName());
      if (nameProps == null) nameProps = new HashSet<PrioritizedProperty>();
      nameProps.add(prop);
      prioritizedProps.put(prop.getName(), nameProps);
    } else {
      LOG.warn("Attempting add of null PrioritizedProperty");
    }

    LOG.debug("<< addPrioritizedProperty");
  }

  /** Highest priority is 1. */
  protected String findHighestPriority(Set<PrioritizedProperty> props) {
    String result = null;
    int highestPriority = Integer.MAX_VALUE;
    for (PrioritizedProperty prop : props) {
      if (prop.getPriority().intValue() < highestPriority) {
        highestPriority = prop.getPriority();
        result = prop.getProperty();
      }
    }

    return result;
  }
}
