/*
 * Copyright 2015 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.jbosson.plugins.amq;

import org.rhq.core.clientapi.descriptor.configuration.ConfigurationProperty;
import org.rhq.core.clientapi.descriptor.configuration.ListProperty;
import org.rhq.core.clientapi.descriptor.configuration.MapProperty;
import org.rhq.core.clientapi.descriptor.configuration.PropertyType;
import org.rhq.core.clientapi.descriptor.configuration.SimpleProperty;

import java.util.Map;

public class OpParameter {

   private String name;
   private ConfigurationProperty type;

   public OpParameter(String name, Class<?> paramType) {
      this.name = name;
      this.type = createType(paramType);
   }

   private ConfigurationProperty createType(Class<?> paramType) {
      if (paramType.isArray()) {
         return new ListProperty();
      }
      if (Map.class.isAssignableFrom(paramType)) {
         return new MapProperty();
      }
      SimpleProperty simple = new SimpleProperty();
      simple.setType(convert(paramType));
      return simple;
   }

   public static PropertyType convert(Class<?> paramType) {
      if (paramType == String.class) {
         return PropertyType.STRING;
      }
      if (paramType == Boolean.class || paramType == boolean.class) {
         return PropertyType.BOOLEAN;
      }
      if (paramType == Integer.class || paramType == Byte.class || paramType == Short.TYPE
               || paramType == int.class || paramType == byte.class || paramType == short.class) {
         return PropertyType.INTEGER;
      }
      if (paramType == Long.class || paramType == long.class) {
         return PropertyType.LONG;
      }
      if (paramType == Double.class || paramType == double.class) {
         return PropertyType.DOUBLE;
      }
      if (paramType == Float.class || paramType == float.class) {
         return PropertyType.FLOAT;
      }
      throw new IllegalArgumentException("Type not supported: " + paramType);
   }

   public String getName() {
      return name;
   }

   public ConfigurationProperty getType() {
      return type;
   }

   @Override
   public String toString() {
      return "param[" + name + "], type: " + (type instanceof SimpleProperty ? ((SimpleProperty)type).getType() : type);
   }
}
