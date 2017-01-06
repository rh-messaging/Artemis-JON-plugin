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

import org.mc4j.ems.connection.EmsConnection;
import org.mc4j.ems.connection.bean.EmsBean;
import org.rhq.core.domain.measurement.AvailabilityType;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.core.pluginapi.inventory.ResourceContext;
import org.rhq.plugins.jmx.JMXComponent;
import org.rhq.plugins.jmx.MBeanResourceComponent;

import java.util.Set;

/**
 * Extends {@link org.rhq.plugins.jmx.MBeanResourceComponent} to support optional plugin property
 * statsObjectName that contains the name of the bean used to provide metrics.
 * <p/>
 * This is useful in situations where the operations are provided by one bean and
 * the metrics by another bean with common objectName properties.
 *
 * @author dbokde
 */
public class ArtemisMBeanResourceComponent<T extends JMXComponent<?>> extends MBeanResourceComponent<T> {

    protected String statsObjectName;
    protected EmsBean statsBean;

    @Override
    public void start(ResourceContext context) {
        super.start(context);

        // look for statsObjectName property
        this.statsObjectName = getResourceContext().getPluginConfiguration().getSimpleValue(ArtemisMBeanDiscoveryComponent.STATS_OBJECT_NAME_PROPERTY);

        if (statsObjectName != null) {
            // transform for future lookups
            setStatsBean(loadBean(this.statsObjectName));
        }
    }

    @Override
    public void stop() {
        super.stop();
        setStatsBean(null);
    }

    @Override
    public void getValues(MeasurementReport report, Set<MeasurementScheduleRequest> requests) {
        super.getValues(report, requests,
            this.statsObjectName == null ? getEmsBean() : getStatsBean());
    }

    @Override
    public AvailabilityType getAvailability() {

        final AvailabilityType availability = super.getAvailability();

        if (availability == AvailabilityType.UP && statsObjectName != null) {
            try {
                if (isMBeanAvailable(getStatsBean())) {
                    return AvailabilityType.UP;
                } else {
                    return AvailabilityType.DOWN;
                }
            } catch (RuntimeException e) {
                if (this.statsBean != null) {
                    // Retry by connecting to a new parent connection (this bean might have been connected to by an old
                    // provider that's since been recreated).
                    this.statsBean = null;
                    if (isMBeanAvailable(getStatsBean())) {
                        return AvailabilityType.UP;
                    } else {
                        return AvailabilityType.DOWN;
                    }
                } else {
                    throw e;
                }
            }
        }
        
        return availability;
    }

    protected boolean isMBeanAvailable(EmsBean emsBean) {
        boolean isAvailable = emsBean.isRegistered();
        if (isAvailable == false) {
            // in some buggy situations, a remote server might tell us an MBean isn't registered but it really is.
            // see JBPAPP-2031 for more
            String emsBeanName = emsBean.getBeanName().getCanonicalName();
            int size = emsBean.getConnectionProvider().getExistingConnection().queryBeans(emsBeanName).size();
            isAvailable = (size == 1);
        }
        return isAvailable;
    }

    public EmsBean getStatsBean() {
        // check if the stats object name was set
        if (statsObjectName == null) {
            return null;
        }

        // make sure the connection used to cache the stats bean is still the current connection. if not, re-cache the bean
        EmsConnection beanConn = (null != this.statsBean) ? this.statsBean.getConnectionProvider().getExistingConnection() : null;
        EmsConnection currConn = (null != this.statsBean) ? getEmsConnection() : null;

        if ((null == this.statsBean) || !beanConn.equals(currConn)) {
            this.statsBean = loadBean(statsObjectName);
            if (null == this.statsBean)
                throw new IllegalStateException("EMS bean was null for Resource with type ["
                    + getResourceContext().getResourceType() + "] and statsObjectName key [" + statsObjectName
                    + "].");
        }

        return this.statsBean;
    }

    protected void setStatsBean(EmsBean bean) {
        this.statsBean = bean;
    }

}
