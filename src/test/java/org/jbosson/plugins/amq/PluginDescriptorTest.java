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

import org.apache.activemq.artemis.api.core.management.AcceptorControl;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.api.core.management.BridgeControl;
import org.apache.activemq.artemis.api.core.management.BroadcastGroupControl;
import org.apache.activemq.artemis.api.core.management.ClusterConnectionControl;
import org.apache.activemq.artemis.api.core.management.DivertControl;
import org.apache.activemq.artemis.api.core.management.Operation;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.jbosson.plugins.amq.depedent.PluginDescriptorUtil;
import org.junit.Before;
import org.junit.Test;
import org.rhq.core.clientapi.descriptor.configuration.ConfigurationDescriptor;
import org.rhq.core.clientapi.descriptor.configuration.ConfigurationProperty;
import org.rhq.core.clientapi.descriptor.configuration.ListProperty;
import org.rhq.core.clientapi.descriptor.configuration.MapProperty;
import org.rhq.core.clientapi.descriptor.configuration.PropertyType;
import org.rhq.core.clientapi.descriptor.configuration.SimpleProperty;
import org.rhq.core.clientapi.descriptor.plugin.OperationDescriptor;
import org.rhq.core.clientapi.descriptor.plugin.PluginDescriptor;
import org.rhq.core.clientapi.descriptor.plugin.ServiceDescriptor;

import javax.xml.bind.JAXBElement;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.jbosson.plugins.amq.InvocationUtil.extractTrueOperationName;
import static org.junit.Assert.assertTrue;

/**
 * the test makes sure the arguments of each operation
 * defined in plugin.xml are correct in name, type and order
 */
public class PluginDescriptorTest {

   private static final String BROKER_SERVICE_NAME = "Broker";

   private PluginDescriptor pluginDescriptor;
   private ServiceDescriptor brokerService;

   @Before
   public void setUp() throws Exception {
      URL url = PluginDescriptorTest.class.getClassLoader().getResource("META-INF/rhq-plugin.xml");
      pluginDescriptor = PluginDescriptorUtil.loadPluginDescriptor(url);
      List<ServiceDescriptor> services = pluginDescriptor.getServices();
      for (ServiceDescriptor sd : services) {
         String serviceName = sd.getName();
         if (BROKER_SERVICE_NAME.equals(serviceName)) {
            brokerService = sd;
            break;
         }
      }
   }

   @Test
   public void testBrokerOperationSignatures() throws Exception {
      List<OperationDescriptor> ops = brokerService.getOperation();
      for (OperationDescriptor od : ops) {
         verifyOperation(ActiveMQServerControl.class, od);
      }
   }

   @Test
   public void testBrokerAddressOperationSignatures() throws Exception {
      ServiceDescriptor addressService = getChildService(brokerService, "Address");
      List<OperationDescriptor> ops = addressService.getOperation();
      for (OperationDescriptor od : ops) {
         verifyOperation(AddressControl.class, od);
      }
      //queue control
      ServiceDescriptor[] qServices = new ServiceDescriptor[2];
      qServices[0] = getChildService(addressService, "Anycast Queue");
      qServices[1] = getChildService(addressService, "Multicast Queue");
      for (ServiceDescriptor sd : qServices) {
         List<OperationDescriptor> qOps = sd.getOperation();
         for (OperationDescriptor od : qOps) {
            verifyOperation(QueueControl.class, od);
         }
      }
   }

   @Test
   public void testBrokerAcceptorOperationSignatures() throws Exception {
      ServiceDescriptor addressService = getChildService(brokerService, "Acceptor");
      List<OperationDescriptor> ops = addressService.getOperation();
      for (OperationDescriptor od : ops) {
         verifyOperation(AcceptorControl.class, od);
      }
   }

   @Test
   public void testBrokerClusterConnectionOperationSignatures() throws Exception {
      ServiceDescriptor addressService = getChildService(brokerService, "Cluster Connection");
      List<OperationDescriptor> ops = addressService.getOperation();
      for (OperationDescriptor od : ops) {
         verifyOperation(ClusterConnectionControl.class, od);
      }
   }

   @Test
   public void testBrokerBridgeOperationSignatures() throws Exception {
      ServiceDescriptor addressService = getChildService(brokerService, "Bridge");
      List<OperationDescriptor> ops = addressService.getOperation();
      for (OperationDescriptor od : ops) {
         verifyOperation(BridgeControl.class, od);
      }
   }

   @Test
   public void testBrokerBroadcastGroupOperationSignatures() throws Exception {
      ServiceDescriptor addressService = getChildService(brokerService, "Broadcast Group");
      List<OperationDescriptor> ops = addressService.getOperation();
      for (OperationDescriptor od : ops) {
         verifyOperation(BroadcastGroupControl.class, od);
      }
   }

   @Test
   public void testBrokerDivertOperationSignatures() throws Exception {
      ServiceDescriptor addressService = getChildService(brokerService, "Divert");
      List<OperationDescriptor> ops = addressService.getOperation();
      for (OperationDescriptor od : ops) {
         verifyOperation(DivertControl.class, od);
      }
   }

   private ServiceDescriptor getChildService(ServiceDescriptor parent, String serviceName) {
      List<ServiceDescriptor> children = parent.getServices();
      for (ServiceDescriptor descriptor : children) {
         if (serviceName.equals(descriptor.getName())) {
            return descriptor;
         }
      }
      return null;
   }

   private void verifyOperation(Class serviceInterface, OperationDescriptor op) {
      String opName = op.getName();
      System.out.println("Verifying operation: " + opName);
      opName = extractTrueOperationName(opName);
      List<OperationInfo> opInfos = getOperationInfo(serviceInterface, opName);

      assertTrue("Operation not found: " + opName, opInfos.size() > 0);
      System.out.println("Found " +  opInfos.size() + " methods named " + opName);

      ConfigurationDescriptor paramsConfig = op.getParameters();

      List<JAXBElement<? extends ConfigurationProperty>> params = new ArrayList<JAXBElement<? extends ConfigurationProperty>>();
      if (paramsConfig != null) {
         params = paramsConfig.getConfigurationProperty();
      }

      boolean foundMatch = false;
      StringBuilder reason = new StringBuilder();
      int n = 0;
      for (OperationInfo opInfo : opInfos) {
         reason.append("(" + n + ") ");
         n++;
         foundMatch = true;
         List<OpParameter> opParameters = opInfo.getParameters();

         if (opParameters.size() != params.size()) {
            foundMatch = false;
            reason.append("parameter number not match, expected: " + params.size() + " but actual: " + opParameters.size() + "; ");

            int opsz = opParameters.size();
            for (int o = 0; o < opsz; o++) {
               OpParameter opm = opParameters.get(o);
               System.out.println("dump actual parameter " + o + " : " + opm.getName());
            }
            int psz = params.size();
            for (int p = 0; p < psz; p++) {
               JAXBElement<? extends ConfigurationProperty> pNode = params.get(p);
               ConfigurationProperty prop = pNode.getValue();
               String name = prop.getName();
               System.out.println("dump parameter " + p + " : " + name);
            }
            continue;
         }

         int numParams = opParameters.size();
         for (int i = 0; i < numParams; i++) {
            JAXBElement<? extends ConfigurationProperty> pNode = params.get(i);
            OpParameter opParam = opParameters.get(i);
            System.out.println("examining parameter " + i + " : " + opParam);
            if (!isParametersMatch(opParam, pNode, reason, i)) {
               foundMatch = false;
               break;
            }
         }
         if (foundMatch) {
            break;
         }
      }
      assertTrue("Couldn't find op: " + op.getName() + " in " + serviceInterface + " reason: " + reason.toString(), foundMatch);
   }

   private List<OperationInfo> getOperationInfo(Class serviceInterface, String opName) {

      List<OperationInfo> result = new ArrayList<OperationInfo>();

      Method[] methods = serviceInterface.getMethods();
      for (Method m : methods) {
         if (opName.equals(m.getName())) {
            Operation op = m.getAnnotation(Operation.class);
            if (op != null) {
               result.add(new OperationInfo(m));
            }
         }
      }
      return result;
   }

   private boolean isParametersMatch(OpParameter opParam, JAXBElement<? extends ConfigurationProperty> pNode, StringBuilder reason, int i) {
      ConfigurationProperty prop = pNode.getValue();
      String name = prop.getName();
      if (!opParam.getName().equals(name)) {
         reason.append("parameter " + i + " name not match, expected: " + name + " but actual: " + opParam.getName() + "; ");
         return false;
      }

      return matchParameterType(opParam.getType(), prop, reason, i);
   }

   private boolean matchParameterType(ConfigurationProperty prop1, ConfigurationProperty prop2, StringBuilder reason, int i) {

      boolean result = false;
      if (prop1 instanceof SimpleProperty) {
         SimpleProperty simple1 = (SimpleProperty) prop1;
         SimpleProperty simple2 = (SimpleProperty) prop2;
         PropertyType type1 = simple1.getType();
         PropertyType type2 = simple2.getType();
         result = type1 == type2;
         if (!result) {
            reason.append("parameter " + i + " type not match, expected: " + type1 + " but " + type2 + "; ");
         }
      } else if (prop1 instanceof ListProperty) {
         result = prop2 instanceof ListProperty;
         if (!result) {
            reason.append("parameter " + i + " type not match, expected: List but " + prop2 + "; ");
         }
      } else if (prop1 instanceof MapProperty) {
         result = prop2 instanceof MapProperty;
         if (!result) {
            reason.append("parameter " + i + " type not match, expected: Map but " + prop2 + "; ");
         }
      }
      return result;
   }
}
