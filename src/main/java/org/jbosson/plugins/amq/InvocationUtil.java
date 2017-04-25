package org.jbosson.plugins.amq;

import org.mc4j.ems.connection.bean.EmsBean;
import org.mc4j.ems.connection.bean.operation.EmsOperation;
import org.rhq.core.domain.configuration.PropertyList;
import org.rhq.core.domain.configuration.PropertyMap;
import org.rhq.core.domain.configuration.PropertySimple;

import java.lang.reflect.Array;
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

}
