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
import org.mc4j.ems.connection.ConnectionFactory;
import org.mc4j.ems.connection.EmsConnection;
import org.mc4j.ems.connection.settings.ConnectionSettings;
import org.mc4j.ems.connection.support.ConnectionProvider;
import org.mc4j.ems.connection.support.metadata.ConnectionTypeDescriptor;
import org.mc4j.ems.connection.support.metadata.J2SE5ConnectionTypeDescriptor;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.measurement.AvailabilityType;
import org.rhq.core.pluginapi.event.log.LogFileEventResourceComponentHelper;
import org.rhq.core.pluginapi.inventory.InvalidPluginConfigurationException;
import org.rhq.core.pluginapi.inventory.ResourceComponent;
import org.rhq.core.pluginapi.inventory.ResourceContext;
import org.rhq.plugins.jmx.JMXComponent;
import org.rhq.plugins.jmx.JMXDiscoveryComponent;
import org.rhq.plugins.jmx.util.ConnectionProviderFactory;

import javax.management.remote.JMXServiceURL;
import java.io.File;

/**
 * Adds log file tracking support.
 *
 * @author dbokde
 */
public class ArtemisServerComponent<T extends ResourceComponent<?>> implements JMXComponent<T> {

    private final Log log = LogFactory.getLog(getClass());

    // needed to be copied from JMXServerComponent, since these are not protected, gaahhh!
    protected EmsConnection connection;
    protected ConnectionProvider connectionProvider;

    // needed for log file tracking
    protected LogFileEventResourceComponentHelper logFileEventDelegate;

    /**
     * The context of a component that is started. Note, other classes should use #getResourceContext(), rather than
     * this field.
     */
    ResourceContext context;

    public void start(ResourceContext context) throws Exception {
        this.context = context;
        log.debug("Starting connection to " + context.getResourceType() + "[" + context.getResourceKey() + "]...");

        // If connecting to the EMS fails, log a warning but still succeed in starting. getAvailability() will keep
        // trying to connect each time it is called.
        try {
            internalStart();
        } catch (Exception e) {
            if (e.getCause() instanceof SecurityException) {
                throw new InvalidPluginConfigurationException("Failed to authenticate to managed JVM - "
                    + "principal and/or credentials connection properties are not set correctly.");
            }
            log.warn("Failed to connect to " + context.getResourceType() + "[" + context.getResourceKey() + "].", e);
        }

        // start log file tracking
        logFileEventDelegate = new LogFileEventResourceComponentHelper(context);
        logFileEventDelegate.startLogFileEventPollers();
    }

    protected void internalStart() throws Exception {
        Configuration pluginConfig = context.getPluginConfiguration();
        String connectionTypeDescriptorClassName = pluginConfig.getSimple(JMXDiscoveryComponent.CONNECTION_TYPE)
            .getStringValue();
        if (JMXDiscoveryComponent.PARENT_TYPE.equals(connectionTypeDescriptorClassName)) {
            // Our parent is itself a JMX component, so just reuse its connection.
            this.connection = ((JMXComponent) context.getParentResourceComponent()).getEmsConnection();
            this.connectionProvider = this.connection.getConnectionProvider();
        } else {
            final File tempDir = this.context.getTemporaryDirectory();
            try {
                this.connectionProvider = ConnectionProviderFactory.createConnectionProvider(pluginConfig,
                    this.context.getNativeProcess(), tempDir);
            } catch (RuntimeException e) {
                // check if Attach API failed, since this resource may have been discovered using jvmstat API
                // avoid loading AttachNotSupportedException class, since its optional
                if (e.getCause() != null &&
                    e.getCause().getClass().getName().equals(
                        ArtemisServerDiscoveryComponent.ATTACH_NOT_SUPPORTED_EXCEPTION_CLASS_NAME)) {

                    // create a connection provider using JvmStatUtility
                    final Class<?> connectionTypeDescriptorClass;
                    connectionTypeDescriptorClass = Class.forName(connectionTypeDescriptorClassName);
                    ConnectionTypeDescriptor connectionType =
                        (ConnectionTypeDescriptor) connectionTypeDescriptorClass.newInstance();

                    // create connection provider settings
                    ConnectionSettings settings = new ConnectionSettings();
                    if (!(connectionType instanceof J2SE5ConnectionTypeDescriptor)) {
                        throw new Exception("Unsupported connection type descriptor " + connectionTypeDescriptorClass);
                    }
                    settings.setConnectionType(connectionType);

                    // get service URL using jvmstat
                    final JMXServiceURL jmxServiceURL = JvmStatUtility.extractJMXServiceURL(context.getNativeProcess());
                    if (jmxServiceURL == null) {
                        throw new Exception("Failed to get JMX service URL using jvmstat");
                    }
                    settings.setServerUrl(jmxServiceURL.toString());

                    settings.getControlProperties().setProperty(
                        ConnectionFactory.COPY_JARS_TO_TEMP, String.valueOf(Boolean.TRUE));
                    settings.getControlProperties().setProperty(
                        ConnectionFactory.JAR_TEMP_DIR, tempDir.getAbsolutePath());

                    ConnectionFactory connectionFactory = new ConnectionFactory();
                    connectionFactory.discoverServerClasses(settings);

                    connectionProvider = connectionFactory.getConnectionProvider(settings);

                } else {
                    // re-throw
                    throw e;
                }
            }
            this.connection = this.connectionProvider.connect();
            this.connection.loadSynchronous(false);
        }
    }

    public void stop() {

        // stop log file tracking
        logFileEventDelegate.stopLogFileEventPollers();

        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                log.error("Error closing EMS connection: " + e);
            }
            connection = null;
        }
    }

    public EmsConnection getEmsConnection() {
        return this.connection;
    }

    public AvailabilityType getAvailability() {
        if ((connectionProvider) == null || !connectionProvider.isConnected()) {
            try {
                internalStart();
            } catch (Exception e) {
                log.debug("Still unable to reconnect to " + context.getResourceType() + "[" + context.getResourceKey()
                    + "] due to error: " + e);
            }
        }

        return ((connectionProvider != null) && connectionProvider.isConnected()) ? AvailabilityType.UP
            : AvailabilityType.DOWN;
    }

    protected ResourceContext getResourceContext() {
        return this.context;
    }

}
