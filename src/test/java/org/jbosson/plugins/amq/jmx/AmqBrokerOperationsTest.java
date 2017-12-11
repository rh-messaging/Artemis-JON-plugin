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

import org.jbosson.plugins.amq.ArtemisServiceComponent;
import org.jbosson.plugins.amq.OperationInfo;
import org.junit.Before;
import org.junit.Test;
import org.mc4j.ems.connection.bean.EmsBean;
import org.rhq.core.domain.configuration.PropertyList;
import org.rhq.core.pluginapi.operation.OperationResult;

public class AmqBrokerOperationsTest extends AmqJonRuntimeTestBase {

   private ArtemisServiceComponent brokerComponent;
   private EmsBean brokerBean;

   @Before
   public void setUp() throws Exception {
      super.setUp();
      brokerBean = getAmQServerBean();
      brokerComponent = new ArtemisServiceComponent();
   }

   @Test
   public void testInvokingListConnectionIDs() throws Exception {
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

}
