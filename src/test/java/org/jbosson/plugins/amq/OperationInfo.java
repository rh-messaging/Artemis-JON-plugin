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

import org.apache.activemq.artemis.api.core.management.Parameter;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class OperationInfo {

   private String name;
   private List<OpParameter> parameters = new ArrayList<OpParameter>();

   public OperationInfo(Method m) {
      this.name = m.getName();
      Class<?>[] paramTypes = m.getParameterTypes();
      Annotation[][] paramAnnotations = m.getParameterAnnotations();
      for (int i = 0; i < paramTypes.length; i++) {
         Parameter param = (Parameter) paramAnnotations[i][0];
         parameters.add(new OpParameter(param.name(), paramTypes[i]));
      }
   }

   public OperationInfo(String opName, Class[] types) {
      this.name = opName;
      if (types != null) {
         for (int i = 0; i < types.length; i++) {
            parameters.add(new OpParameter("", types[i]));
         }
      }
   }

   public List<OpParameter> getParameters() {
      return parameters;
   }

   public String getName() {
      return this.name;
   }
}
