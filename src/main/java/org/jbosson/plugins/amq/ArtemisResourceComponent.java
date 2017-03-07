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

import java.util.Set;

import org.mc4j.ems.connection.bean.attribute.EmsAttribute;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.configuration.definition.ConfigurationDefinition;
import org.rhq.core.domain.configuration.definition.PropertyDefinition;
import org.rhq.core.domain.configuration.definition.PropertyDefinitionSimple;
import org.rhq.core.domain.measurement.MeasurementDataNumeric;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.plugins.jmx.JMXComponent;
import org.rhq.plugins.jmx.MBeanResourceComponent;

/**
 * Adds support for converting <code>MemoryPercentUsage</code> metric value from absolute integer to percentage.
 *
 * @author dbokde
 */
public class ArtemisResourceComponent<T extends JMXComponent<?>> extends MBeanResourceComponent<T> {

    private static final String MEMORY_PERCENT_USAGE = "MemoryPercentUsage";

    @Override
    public void getValues(MeasurementReport report, Set<MeasurementScheduleRequest> requests) {
        super.getValues(report, requests);
        // normalize percentage metric values to 0..1.0
        for (MeasurementDataNumeric dataNumeric : report.getNumericData()) {
            if (MEMORY_PERCENT_USAGE.equals(dataNumeric.getName()) && dataNumeric.getValue() != null) {
                dataNumeric.setValue(dataNumeric.getValue() / 100.0);
            }
        }
    }

    @Override
    public Configuration loadResourceConfiguration() {
        Configuration configuration = new Configuration();
        ConfigurationDefinition configurationDefinition = this.getResourceContext().getResourceType()
            .getResourceConfigurationDefinition();

        for (PropertyDefinition property : configurationDefinition.getPropertyDefinitions().values()) {
            if (property instanceof PropertyDefinitionSimple) {
                EmsAttribute attribute = getEmsBean().getAttribute(property.getName());
                if (attribute != null) {
                    configuration.put(new PropertySimple(property.getName(), attribute.refresh()));
                }
            }
        }

        return configuration;
    }
}
