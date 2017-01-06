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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rhq.core.system.ProcessInfo;
import org.rhq.plugins.jmx.MBeanResourceComponent;
import sun.jvmstat.monitor.HostIdentifier;
import sun.jvmstat.monitor.MonitorException;
import sun.jvmstat.monitor.MonitoredHost;
import sun.jvmstat.monitor.MonitoredVm;
import sun.jvmstat.monitor.MonitoredVmUtil;
import sun.jvmstat.monitor.VmIdentifier;
import sun.management.ConnectorAddressLink;

import javax.management.remote.JMXServiceURL;
import java.net.URISyntaxException;
import java.util.Set;

/**
 * Similar to the {@link org.rhq.plugins.jmx.util.JvmUtility},
 * but uses jvmstat instead of the Attach API.
 *
 * @author dbokde
 */
public class JvmStatUtility {

    private static final Log LOG = LogFactory.getLog(MBeanResourceComponent.class);

    private static boolean jvmstatApiAvailable;

    static {
        try {
            Class.forName("sun.jvmstat.monitor.MonitoredHost");
            jvmstatApiAvailable = true;
        } catch (ClassNotFoundException e) {
            LOG.warn("JDK does not support jvmstat - cannot discover JVMs using Sun jvmstat API; "
                + "to fix this, run the RHQ Agent on a Sun JDK.");
        }
    }

    public static JMXServiceURL extractJMXServiceURL(ProcessInfo process) {
        if (!jvmstatApiAvailable) {
            LOG.debug("Returning null, since the jvmstat API is not available...");
            return null;
        }

        final long pid = process.getPid();
        JMXServiceURL serviceUrl;
        try {

            // get local host
            final MonitoredHost host = MonitoredHost.getMonitoredHost(new HostIdentifier((String) null));
            // get monitored VM
            final MonitoredVm vm = attachToMonitoredVm(host, pid);
            if (vm == null) {
                return null;
            }

            // get the service url, only if the service supports attach-on-demand
            if (!MonitoredVmUtil.isAttachable(vm)) {
                LOG.warn("JVM with PID[" + pid + "] does not support attach-on-demand");
                return null;
            }

            // can the PID be converted to int, throw an exception on platforms where this can't be done!
            if (pid < Integer.MIN_VALUE || pid > Integer.MAX_VALUE) {
                throw new Exception("Java process PID [" + pid +
                    "] cannot be converted to integer to extract JMX url using ConnectorAddressLink.importFrom(int)");
            }

            // get the connector address from vm
            serviceUrl = new JMXServiceURL(ConnectorAddressLink.importFrom((int) pid));
            try {
                vm.detach();
            } catch (Exception e) {
                // We already succeeded in obtaining the connector address,
                // so just log this, rather than throwing an exception.
                LOG.error("Failed to detach from JVM [" + vm.getVmIdentifier() + "].", e);
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to extract JMX service URL for process with PID [" + pid
                + "].", e);
        }

        LOG.debug("JMX service URL for java process with PID[" + pid + "]: " + serviceUrl);
        return serviceUrl;
    }

    private static MonitoredVm attachToMonitoredVm(MonitoredHost host, long pid) throws MonitorException, URISyntaxException {
        final Set vmIds = host.activeVms();
        for (Object vmId : vmIds) {
            if (vmId instanceof Integer &&
                ((Integer)vmId).longValue() == pid) {
                return host.getMonitoredVm(new VmIdentifier(vmId.toString()));
            }
        }
        return null;
    }

    private JvmStatUtility() {
    }

}
