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
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.PropertyList;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.pluginapi.operation.OperationResult;
import org.rhq.plugins.jmx.JMXComponent;

import static org.jbosson.plugins.amq.InvocationUtil.fillInArray;
import static org.jbosson.plugins.amq.InvocationUtil.findOperation;

public class ArtemisServiceComponent<T extends JMXComponent<?>> extends ArtemisResourceComponent<T> {

   private static final String CREATE_ADDRESS_OPERATION = "createAddress";
   private static final String LIST_PREPARED_TX_OPERATION = "listPreparedTransactions";
   private static final String LIST_PREPARED_TX_AS_JSON_OPERATION = "listPreparedTransactionDetailsAsJSON";
   private static final String LIST_PREPARED_TX_AS_HTML_OPERATION = "listPreparedTransactionDetailsAsHTML";
   private static final String LIST_HEURISTIC_ROLLEDBACK_TX_OPERATION = "listHeuristicRolledBackTransactions";
   private static final String LIST_HEURISTIC_COMMITTED_TX_OPERATION = "listHeuristicCommittedTransactions";
   private static final String COMMIT_PREPARED_TX_OPERATION = "commitPreparedTransaction";
   private static final String ROLLBACK_PREPARED_TX_OPERATION = "rollbackPreparedTransaction";
   private static final String LIST_REMOTE_ADDRESSES_OPERATION = "listRemoteAddresses";
   private static final String CLOSE_CONNECTIONS_FOR_ADDRESS_OPERATION = "closeConnectionsForAddress";
   private static final String CLOSE_CONSUMER_CONNECTIONS_FOR_ADDRESS_OPERATION = "closeConsumerConnectionsForAddress";
   private static final String CLOSE_CONNECTIONS_FOR_USER_OPERATION = "closeConnectionsForUser";
   private static final String LIST_CONNECTION_IDS_OPERATION = "listConnectionIDs";
   private static final String LIST_PRODUCERS_INFO_AS_JSON_OPERATION = "listProducersInfoAsJSON";
   private static final String LIST_CONNECTIONS_AS_JSON_OPERATION = "listConnectionsAsJSON";
   private static final String LIST_CONSUMERS_AS_JSON_OPERATION = "listConsumersAsJSON";
   private static final String LIST_ALL_CONSUMERS_AS_JSON_OPERATION = "listAllConsumersAsJSON";
   private static final String LIST_SESSIONS_AS_JSON_OPERATION = "listSessionsAsJSON";
   private static final String LIST_SESSIONS_OPERATION = "listSessions";
   private static final String GET_ROLES_AS_JSON_OPERATION = "getRolesAsJSON";
   private static final String GET_ADDRESS_SETTINGS_AS_JSON_OPERATION = "getAddressSettingsAsJSON";
   private static final String GET_DIVERT_NAMES_OPERATION = "getDivertNames";
   private static final String GET_CONNECTOR_SERVICES_OPERATION = "getConnectorServices";
   private static final String LIST_NETWORK_TOPOLOGY_OPERATION = "listNetworkTopology";
   private static final String GET_ADDRESS_INFO_OPERATION = "getAddressInfo";
   private static final String LIST_BINDINGS_FOR_ADDRESS_OPERATION = "listBindingsForAddress";
   private static final String LIST_ADDRESSES_OPERATION = "listAddresses";

   @Override
   public OperationResult invokeOperation(String name, Configuration parameters, EmsBean emsBean) throws Exception {

      if (CREATE_ADDRESS_OPERATION.equals(name)) {
         return handleCreateAddress(parameters, emsBean);
      } else if (LIST_PREPARED_TX_OPERATION.equals(name)) {
         return handleListPreparedTx(emsBean);
      } else if (LIST_PREPARED_TX_AS_JSON_OPERATION.equals(name)) {
         return handleListPreparedTxAsJSON(emsBean);
      } else if (LIST_PREPARED_TX_AS_HTML_OPERATION.equals(name)) {
         return handleListPreparedTxAsHtml(emsBean);
      } else if (LIST_HEURISTIC_ROLLEDBACK_TX_OPERATION.equals(name)) {
         return handleListHeuristicRolledbackTx(emsBean);
      } else if (LIST_HEURISTIC_COMMITTED_TX_OPERATION.equals(name)) {
         return handleListHeuristicCommittedTx(emsBean);
      } else if (COMMIT_PREPARED_TX_OPERATION.equals(name)) {
         return handleCommitPreparedTx(parameters, emsBean);
      } else if (ROLLBACK_PREPARED_TX_OPERATION.equals(name)) {
         return handleRollbackPreparedTx(parameters, emsBean);
      } else if (LIST_REMOTE_ADDRESSES_OPERATION.equals(name)) {
         return handleListRemoteAddresses(emsBean);
      } else if (CLOSE_CONNECTIONS_FOR_ADDRESS_OPERATION.equals(name)) {
         return handleCloseConnectionsForAddress(parameters, emsBean);
      } else if (CLOSE_CONSUMER_CONNECTIONS_FOR_ADDRESS_OPERATION.equals(name)) {
         return handleCloseConsumerConnectionsForAddress(parameters, emsBean);
      } else if (CLOSE_CONNECTIONS_FOR_USER_OPERATION.equals(name)) {
         return handleCloseConnectionsForUser(parameters, emsBean);
      } else if (LIST_CONNECTION_IDS_OPERATION.equals(name)) {
         return handleListConnectionIDs(emsBean);
      } else if (LIST_PRODUCERS_INFO_AS_JSON_OPERATION.equals(name)) {
         return handleListProducersInfoAsJSON(emsBean);
      } else if (LIST_CONNECTIONS_AS_JSON_OPERATION.equals(name)) {
         return handleListConnectionsAsJSON(emsBean);
      } else if (LIST_CONSUMERS_AS_JSON_OPERATION.equals(name)) {
         return handleListConsumersAsJSON(parameters, emsBean);
      } else if (LIST_ALL_CONSUMERS_AS_JSON_OPERATION.equals(name)) {
         return handleListAllConsumersAsJSON(emsBean);
      } else if (LIST_SESSIONS_AS_JSON_OPERATION.equals(name)) {
         return handleListSessionsAsJSON(parameters, emsBean);
      } else if (LIST_SESSIONS_OPERATION.equals(name)) {
         return handleListSessions(parameters, emsBean);
      } else if (GET_ROLES_AS_JSON_OPERATION.equals(name)) {
         return handleGetRolesAsJSON(parameters, emsBean);
      } else if (GET_ADDRESS_SETTINGS_AS_JSON_OPERATION.equals(name)) {
         return handleGetAddressSettingsAsJSON(parameters, emsBean);
      } else if (GET_DIVERT_NAMES_OPERATION.equals(name)) {
         return handleGetDivertNames(emsBean);
      } else if (GET_CONNECTOR_SERVICES_OPERATION.equals(name)) {
         return handleGetConnectorServicesOperation(emsBean);
      } else if (LIST_NETWORK_TOPOLOGY_OPERATION.equals(name)) {
         return handleListNetworkTopology(emsBean);
      } else if (GET_ADDRESS_INFO_OPERATION.equals(name)) {
         return handleGetAddressInfo(parameters, emsBean);
      } else if (LIST_BINDINGS_FOR_ADDRESS_OPERATION.equals(name)) {
         return handleListBindingsForAddress(parameters, emsBean);
      } else if (LIST_ADDRESSES_OPERATION.equals(name)) {
         return handleListAddresses(parameters, emsBean);
      } else {
         return super.invokeOperation(name, parameters, emsBean);
      }
   }

   private OperationResult handleListAddresses(Configuration parameters, EmsBean emsBean) throws Exception {
      EmsOperation operation = findOperation(emsBean, LIST_ADDRESSES_OPERATION, 1);
      String separator = parameters.getSimpleValue("separator");
      final Object opResult = operation.invoke(separator);
      final OperationResult result = new OperationResult("result");
      result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
      return result;
   }

   private OperationResult handleListBindingsForAddress(Configuration parameters, EmsBean emsBean) throws Exception {
      EmsOperation operation = findOperation(emsBean, LIST_BINDINGS_FOR_ADDRESS_OPERATION, 1);
      String address = parameters.getSimpleValue("address");
      final Object opResult = operation.invoke(address);
      final OperationResult result = new OperationResult("result");
      result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
      return result;
   }

   private OperationResult handleGetAddressInfo(Configuration parameters, EmsBean emsBean) throws Exception {
      EmsOperation operation = findOperation(emsBean, GET_ADDRESS_INFO_OPERATION, 1);
      String address = parameters.getSimpleValue("address");
      final Object opResult = operation.invoke(address);
      final OperationResult result = new OperationResult("result");
      result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
      return result;
   }

   private OperationResult handleListNetworkTopology(EmsBean emsBean) throws Exception {
      EmsOperation operation = findOperation(emsBean, LIST_NETWORK_TOPOLOGY_OPERATION, 0);
      final Object opResult = operation.invoke();
      final OperationResult result = new OperationResult();
      final PropertyList messageList = new PropertyList("result");
      fillInArray(messageList, opResult);
      result.getComplexResults().put(messageList);
      return result;
   }

   private OperationResult handleGetConnectorServicesOperation(EmsBean emsBean) throws Exception {
      EmsOperation operation = findOperation(emsBean, GET_CONNECTOR_SERVICES_OPERATION, 0);
      final Object opResult = operation.invoke();
      final OperationResult result = new OperationResult();
      final PropertyList messageList = new PropertyList("result");
      fillInArray(messageList, opResult);
      result.getComplexResults().put(messageList);
      return result;
   }

   private OperationResult handleGetDivertNames(EmsBean emsBean) throws Exception {
      EmsOperation operation = findOperation(emsBean, GET_DIVERT_NAMES_OPERATION, 0);
      final Object opResult = operation.invoke();
      final OperationResult result = new OperationResult();
      final PropertyList messageList = new PropertyList("result");
      fillInArray(messageList, opResult);
      result.getComplexResults().put(messageList);
      return result;
   }

   private OperationResult handleGetAddressSettingsAsJSON(Configuration parameters, EmsBean emsBean) throws Exception {
      EmsOperation operation = findOperation(emsBean, GET_ADDRESS_SETTINGS_AS_JSON_OPERATION, 1);
      String addressMatch = parameters.getSimpleValue("addressMatch");
      final Object opResult = operation.invoke(addressMatch);
      final OperationResult result = new OperationResult("result");
      result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
      return result;
   }

   private OperationResult handleGetRolesAsJSON(Configuration parameters, EmsBean emsBean) throws Exception {
      EmsOperation operation = findOperation(emsBean, GET_ROLES_AS_JSON_OPERATION, 1);
      String addressMatch = parameters.getSimpleValue("addressMatch");
      final Object opResult = operation.invoke(addressMatch);
      final OperationResult result = new OperationResult("result");
      result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
      return result;
   }

   private OperationResult handleListSessions(Configuration parameters, EmsBean emsBean) throws Exception {
      EmsOperation operation = findOperation(emsBean, LIST_SESSIONS_OPERATION, 1);
      String connID = parameters.getSimpleValue("connectionID");
      final Object opResult = operation.invoke(connID);
      final OperationResult result = new OperationResult();
      final PropertyList messageList = new PropertyList("result");
      fillInArray(messageList, opResult);
      result.getComplexResults().put(messageList);
      return result;
   }

   private OperationResult handleListSessionsAsJSON(Configuration parameters, EmsBean emsBean) throws Exception {
      EmsOperation operation = findOperation(emsBean, LIST_SESSIONS_AS_JSON_OPERATION, 1);
      String connID = parameters.getSimpleValue("connectionID");
      final Object opResult = operation.invoke(connID);
      final OperationResult result = new OperationResult("result");
      result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
      return result;
   }

   private OperationResult handleListAllConsumersAsJSON(EmsBean emsBean) throws Exception {
      EmsOperation operation = findOperation(emsBean, LIST_ALL_CONSUMERS_AS_JSON_OPERATION, 0);
      final Object opResult = operation.invoke();
      final OperationResult result = new OperationResult("result");
      result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
      return result;
   }

   private OperationResult handleListConsumersAsJSON(Configuration parameters, EmsBean emsBean) throws Exception {
      EmsOperation operation = findOperation(emsBean, LIST_CONSUMERS_AS_JSON_OPERATION, 1);
      String connID = parameters.getSimpleValue("connectionID");
      final Object opResult = operation.invoke(connID);
      final OperationResult result = new OperationResult("result");
      result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
      return result;
   }

   private OperationResult handleListConnectionsAsJSON(EmsBean emsBean) throws Exception {
      EmsOperation operation = findOperation(emsBean, LIST_CONNECTIONS_AS_JSON_OPERATION, 0);
      final Object opResult = operation.invoke();
      final OperationResult result = new OperationResult("result");
      result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
      return result;
   }

   private OperationResult handleListProducersInfoAsJSON(EmsBean emsBean) throws Exception {
      EmsOperation operation = findOperation(emsBean, LIST_PRODUCERS_INFO_AS_JSON_OPERATION, 0);
      final Object opResult = operation.invoke();
      final OperationResult result = new OperationResult("result");
      result.getComplexResults().put(new PropertySimple("value", opResult.toString()));
      return result;
   }

   private OperationResult handleListConnectionIDs(EmsBean emsBean) throws Exception {
      EmsOperation operation = findOperation(emsBean, LIST_CONNECTION_IDS_OPERATION, 0);
      final Object opResult = operation.invoke();
      final OperationResult result = new OperationResult();
      final PropertyList messageList = new PropertyList("result");
      fillInArray(messageList, opResult);
      result.getComplexResults().put(messageList);
      return result;
   }

   private OperationResult handleCloseConnectionsForUser(Configuration parameters, EmsBean emsBean) throws Exception {
      EmsOperation operation = findOperation(emsBean, CLOSE_CONNECTIONS_FOR_USER_OPERATION, 1);
      String user = parameters.getSimpleValue("userName");
      final Object opResult = operation.invoke(user);
      final OperationResult result = new OperationResult();
      result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
      return result;
   }

   private OperationResult handleCloseConsumerConnectionsForAddress(Configuration parameters, EmsBean emsBean) throws Exception {
      EmsOperation operation = findOperation(emsBean, CLOSE_CONSUMER_CONNECTIONS_FOR_ADDRESS_OPERATION, 1);
      String ip = parameters.getSimpleValue("ipAddress");
      final Object opResult = operation.invoke(ip);
      final OperationResult result = new OperationResult();
      result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
      return result;
   }

   private OperationResult handleCloseConnectionsForAddress(Configuration parameters, EmsBean emsBean) throws Exception {
      EmsOperation operation = findOperation(emsBean, CLOSE_CONNECTIONS_FOR_ADDRESS_OPERATION, 1);
      String ip = parameters.getSimpleValue("ipAddress");
      final Object opResult = operation.invoke(ip);
      final OperationResult result = new OperationResult();
      result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
      return result;
   }

   private OperationResult handleListRemoteAddresses(EmsBean emsBean) throws Exception {
      EmsOperation operation = findOperation(emsBean, LIST_REMOTE_ADDRESSES_OPERATION, 0);
      final Object opResult = operation.invoke();
      final OperationResult result = new OperationResult();
      final PropertyList messageList = new PropertyList("result");
      fillInArray(messageList, opResult);
      result.getComplexResults().put(messageList);
      return result;
   }

   private OperationResult handleRollbackPreparedTx(Configuration parameters, EmsBean emsBean) throws Exception {
      EmsOperation operation = findOperation(emsBean, ROLLBACK_PREPARED_TX_OPERATION, 1);
      String txId = parameters.getSimpleValue("transactionAsBase64");
      final Object opResult = operation.invoke(txId);
      final OperationResult result = new OperationResult();
      result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
      return result;
   }

   private OperationResult handleCommitPreparedTx(Configuration parameters, EmsBean emsBean) throws Exception {
      EmsOperation operation = findOperation(emsBean, COMMIT_PREPARED_TX_OPERATION, 1);
      String txId = parameters.getSimpleValue("transactionAsBase64");
      final Object opResult = operation.invoke(txId);
      final OperationResult result = new OperationResult();
      result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
      return result;
   }

   private OperationResult handleListHeuristicCommittedTx(EmsBean emsBean) throws Exception {
      EmsOperation operation = findOperation(emsBean, LIST_HEURISTIC_COMMITTED_TX_OPERATION, 0);
      final Object opResult = operation.invoke();
      final OperationResult result = new OperationResult();
      final PropertyList messageList = new PropertyList("result");
      fillInArray(messageList, opResult);
      result.getComplexResults().put(messageList);
      return result;
   }

   private OperationResult handleListHeuristicRolledbackTx(EmsBean emsBean) throws Exception {
      EmsOperation operation = findOperation(emsBean, LIST_HEURISTIC_ROLLEDBACK_TX_OPERATION, 0);
      final Object opResult = operation.invoke();
      final OperationResult result = new OperationResult();
      final PropertyList messageList = new PropertyList("result");
      fillInArray(messageList, opResult);
      result.getComplexResults().put(messageList);
      return result;
   }

   private OperationResult handleListPreparedTxAsHtml(EmsBean emsBean) throws Exception {
      EmsOperation operation = findOperation(emsBean, LIST_PREPARED_TX_AS_HTML_OPERATION, 0);
      final Object opResult = operation.invoke();
      final OperationResult result = new OperationResult();
      result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
      return result;
   }

   private OperationResult handleListPreparedTxAsJSON(EmsBean emsBean) throws Exception {
      EmsOperation operation = findOperation(emsBean, LIST_PREPARED_TX_AS_JSON_OPERATION, 0);
      final Object opResult = operation.invoke();
      final OperationResult result = new OperationResult();
      result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
      return result;
   }

   private OperationResult handleListPreparedTx(EmsBean emsBean) throws Exception {
      EmsOperation operation = findOperation(emsBean, LIST_PREPARED_TX_OPERATION, 0);
      final Object opResult = operation.invoke();
      final OperationResult result = new OperationResult();
      final PropertyList messageList = new PropertyList("result");
      fillInArray(messageList, opResult);
      result.getComplexResults().put(messageList);
      return result;
   }

   private OperationResult handleCreateAddress(Configuration parameters, EmsBean emsBean) throws Exception {
      EmsOperation operation = findOperation(emsBean, CREATE_ADDRESS_OPERATION, 2);
      String name = parameters.getSimpleValue("name");
      String routingTypes = parameters.getSimpleValue("routingTypes");
      final Object opResult = operation.invoke(name, routingTypes);
      final OperationResult result = new OperationResult();
      result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
      return result;
   }

}
