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

import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.jbosson.plugins.amq.depedent.PluginDescriptorUtil;
import org.junit.Before;
import org.rhq.core.clientapi.descriptor.plugin.PluginDescriptor;
import org.rhq.core.clientapi.descriptor.plugin.ServiceDescriptor;

import java.net.URL;
import java.util.List;

public class AmqJonTestBase extends ActiveMQTestBase {

   protected static final String BROKER_SERVICE_NAME = "Broker";

   protected PluginDescriptor pluginDescriptor;
   protected ServiceDescriptor brokerService;

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

}
