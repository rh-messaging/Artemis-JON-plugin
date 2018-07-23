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
package org.jbosson.plugins.amq.jmx;

import org.apache.activemq.artemis.cli.factory.jmx.ManagementFactory;
import org.apache.activemq.artemis.core.server.management.ManagementContext;
import org.apache.activemq.artemis.dto.ManagementContextDTO;
import org.jbosson.plugins.amq.ArtemisServiceComponent;
import org.jbosson.plugins.amq.OperationInfo;
import org.junit.Test;
import org.mc4j.ems.connection.EmsConnectException;
import org.mc4j.ems.connection.EmsConnection;
import org.mc4j.ems.connection.settings.ConnectionSettings;
import org.mc4j.ems.connection.support.ConnectionProvider;
import org.mc4j.ems.connection.support.metadata.JSR160ConnectionTypeDescriptor;
import org.rhq.core.domain.configuration.PropertyList;
import org.rhq.core.pluginapi.operation.OperationResult;

import java.net.URL;

public class JmxAuthTest extends AmqJonRuntimeTestBase {

   @Override
   protected ManagementContext configureJmxAccess() throws Exception {
      ManagementContextDTO managementDTO = getManagementDTO();
      ManagementContext managementContext = ManagementFactory.create(managementDTO);
      return managementContext;
   }

   private ManagementContextDTO getManagementDTO() throws Exception {
      URL url = getClass().getClassLoader().getResource("management.xml");
      String configuration = "xml:" + url.toURI().toString().substring("file:".length());

      configuration = configuration.replace("\\", "/");

      ManagementContextDTO mContextDTO = ManagementFactory.createJmxAclConfiguration(configuration, null, null, null);
      return mContextDTO;
   }

   @Override
   protected String getJmxCredentials() {
      return "jmxpassword";
   }

   protected String getJmxPrincipal() {
      return "jmxuser";
   }

   @Test
   public void testJmxConnectionAuthen() {
      //ok
      connectJmxWithAuthen("jmxuser", "jmxpassword");
      //wrong pass
      try {
         connectJmxWithAuthen("jmxuser", "wrongpassword");
         fail("didn't get exception with wrong password");
      } catch (SecurityException e) {
         //correct
      }
      //non exist user
      try {
         connectJmxWithAuthen("nouser", "jmxpassword");
         fail("didn't get exception with wrong principal");
      } catch (SecurityException e) {
         //correct
      }
   }

   @Test
   public void testInvokeUsingValidRole() throws Exception {
      OperationInfo op = getBrokerOperation(ArtemisServiceComponent.LIST_CONNECTION_IDS_OPERATION);
      OperationResult result = brokerComponent.invokeOperation(op.getName(), null, brokerBean);
      PropertyList connList = result.getComplexResults().getList("result");
      assertEquals(0, connList.getList().size());

      //create one connection
      createConnection();
      result = brokerComponent.invokeOperation(op.getName(), null, brokerBean);
      connList = result.getComplexResults().getList("result");
      assertEquals(1, connList.getList().size());
   }

   private void connectJmxWithAuthen(String user, String password) {
      ConnectionSettings emsConnectionSettings = new ConnectionSettings();
      JSR160ConnectionTypeDescriptor descriptor = new JSR160ConnectionTypeDescriptor();
      emsConnectionSettings.initializeConnectionType(descriptor);
      emsConnectionSettings.setServerUrl(jmxServiceURL);
      emsConnectionSettings.setPrincipal(user);
      emsConnectionSettings.setCredentials(password);

      ConnectionProvider provider = emsFactory.getConnectionProvider(emsConnectionSettings);
      EmsConnection jmxConn = null;
      try {
         jmxConn = provider.connect();
      } catch (EmsConnectException e) {
         if (e.getCause() instanceof SecurityException) {
            throw new SecurityException(e);
         } else {
            throw e;
         }
      } finally {
         if (jmxConn != null) {
            jmxConn.close();
         }
      }
   }
}
