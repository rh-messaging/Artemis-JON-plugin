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

import org.rhq.core.pluginapi.inventory.DiscoveredResourceDetails;
import org.rhq.core.pluginapi.inventory.ResourceDiscoveryContext;
import org.rhq.plugins.jmx.EmbeddedJMXServerDiscoveryComponent;
import org.rhq.plugins.jmx.JMXComponent;
import org.rhq.plugins.jmx.JMXDiscoveryComponent;

import java.util.Set;

/**
 * Forces Connection Type to PARENT.
 *
 * @author dbokde
 */
public class ArtemisEmbeddedJMXServerDiscoveryComponent extends EmbeddedJMXServerDiscoveryComponent {

    @Override
    public Set<DiscoveredResourceDetails> discoverResources(ResourceDiscoveryContext<JMXComponent<?>> context) throws Exception {

        Set<DiscoveredResourceDetails> detailsSet = super.discoverResources(context);

        // for JMX connection type to PARENT for JMX Server embedded in Fuse Servers
        for (DiscoveredResourceDetails details : detailsSet) {
            // remove the connector address created by the base class when using Manual Add
            // the base class does this when it sees the jmxremote.port command line option
            // disregarding the parent component's connection!!!
            details.getPluginConfiguration().setSimpleValue(JMXDiscoveryComponent.CONNECTOR_ADDRESS_CONFIG_PROPERTY,
                null);
            details.getPluginConfiguration().setSimpleValue(JMXDiscoveryComponent.CONNECTION_TYPE,
                JMXDiscoveryComponent.PARENT_TYPE);
        }

        return detailsSet;
    }

}
