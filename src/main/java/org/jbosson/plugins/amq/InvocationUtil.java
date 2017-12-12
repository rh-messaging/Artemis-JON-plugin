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

import org.mc4j.ems.connection.bean.EmsBean;
import org.mc4j.ems.connection.bean.operation.EmsOperation;
import org.rhq.core.domain.configuration.PropertyList;
import org.rhq.core.domain.configuration.PropertyMap;
import org.rhq.core.domain.configuration.PropertySimple;

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;

public final class InvocationUtil {

   public static EmsOperation findOperation(EmsBean emsBean, String opName, int args) throws Exception {
      final SortedSet<EmsOperation> operations = emsBean.getOperations();
      EmsOperation op = null;
      for (EmsOperation operation : operations) {
         if (opName.equals(operation.getName()) &&
                  operation.getParameters().size() == args) {
            op = operation;
            break;
         }
      }

      if (op == null) {
         throw new Exception("Can't find operation " + opName + " with " + args + " arguments.");
      }
      return op;
   }

   //resolve Map<Object, Map<>[] type
   public static void fillInMapMapArray(PropertyMap mapMapArray, Object opResult) {

      Map<Object, Map[]> mapResult = (Map<Object, Map[]>) opResult;

      Iterator<Object> owners = mapResult.keySet().iterator();
      while (owners.hasNext()) {
         Object key = owners.next();
         Map[] mapProps = mapResult.get(key);

         PropertyList listMap = new PropertyList("messages");

         if (mapProps != null) {
            for (int i = 0; i < mapProps.length; i++) {
               Map mProp = mapProps[i];
               if (mProp != null) {
                  PropertyMap mMap = new PropertyMap("messageProp");
                  Iterator mIter = mProp.keySet().iterator();
                  while (mIter.hasNext()) {
                     Object mKey = mIter.next();
                     Object mValue = mProp.get(mKey);
                     mMap.put(new PropertySimple(mKey.toString(), (mValue == null ? null : mValue.toString())));
                  }
                  listMap.add(mMap);
               }
            }
         }
         mapMapArray.put(listMap);
      }
   }

   public static void fillInMapArray(PropertyList messageList, Object opResult) {

      int nMsgs = Array.getLength(opResult);

      for (int i = 0; i < nMsgs; i++) {
         final PropertyMap msgMap = new PropertyMap("message");
         final Map<String, Object> msgData = (Map<String, Object>) Array.get(opResult, i);

         for (String key : msgData.keySet()) {
            final Object value = msgData.get(key);
            msgMap.put(new PropertySimple(key, (value != null) ? value.toString() : null));
         }
         messageList.add(msgMap);
      }
   }

   public static void fillInArray(PropertyList messageList, Object opResult) {
      int nMsgs = Array.getLength(opResult);
      for (int i = 0; i < nMsgs; i++) {
         final String value = (String) Array.get(opResult, i);
         messageList.add(new PropertySimple("value", value));
      }
   }

   public static String extractTrueOperationName(String opValue) {
      String opName = null;
      //some have alternative name, e.g. listRemoteAddresses2,operation=listRemoteAddresses
      int index = opValue.indexOf("=");
      if (index > -1) {
         opName = opValue.substring(index + 1).trim();
      } else {
         opName = opValue;
      }
      return opName;
   }

   public static Long toLong(String value) {
      if (value == null) return null;
      return Long.valueOf(value);
   }

   public static Boolean toBoolean(String value) {
      if (value == null) return null;
      return Boolean.valueOf(value);
   }

   public static Integer toInt(String value) {
      if (value == null) return null;
      return Integer.valueOf(value);
   }

   public static Double toDouble(String value) {
      if (value == null) return null;
      return Double.valueOf(value);
   }
}
