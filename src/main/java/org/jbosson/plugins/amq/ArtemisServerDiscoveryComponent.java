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
import org.mc4j.ems.connection.EmsConnection;
import org.mc4j.ems.connection.bean.EmsBean;
import org.mc4j.ems.connection.support.ConnectionProvider;
import org.mc4j.ems.connection.support.metadata.J2SE5ConnectionTypeDescriptor;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.PropertyList;
import org.rhq.core.domain.configuration.PropertyMap;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.configuration.definition.PropertyDefinition;
import org.rhq.core.domain.event.EventSeverity;
import org.rhq.core.pluginapi.event.log.LogFileEventResourceComponentHelper;
import org.rhq.core.pluginapi.inventory.DiscoveredResourceDetails;
import org.rhq.core.pluginapi.inventory.InvalidPluginConfigurationException;
import org.rhq.core.pluginapi.inventory.ManualAddFacet;
import org.rhq.core.pluginapi.inventory.ProcessScanResult;
import org.rhq.core.pluginapi.inventory.ResourceDiscoveryComponent;
import org.rhq.core.pluginapi.inventory.ResourceDiscoveryContext;
import org.rhq.core.system.ProcessInfo;
import org.rhq.plugins.jmx.JMXComponent;
import org.rhq.plugins.jmx.JMXDiscoveryComponent;
import org.rhq.plugins.jmx.util.ConnectionProviderFactory;
import org.rhq.plugins.jmx.util.JvmUtility;

import javax.management.remote.JMXServiceURL;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Discovery component for Fuse servers.
 * <p>
 * It allows plugin to use a custom resource key system property instead of org.rhq.resourceKey.
 * The custom property is specified using the plugin property 'resourceKey' and default (initial) value
 * with the system property name.
 * </p>
 *
 * @author dbokde
 */
public class ArtemisServerDiscoveryComponent implements ResourceDiscoveryComponent, ManualAddFacet {

    private static final Log log = LogFactory.getLog(ArtemisServerDiscoveryComponent.class);

    // Runtime discovery
    public final Set<DiscoveredResourceDetails> discoverResources(ResourceDiscoveryContext context) {

        // populate system properties in plugin properties
        Set<DiscoveredResourceDetails> discoveredResources = new LinkedHashSet<DiscoveredResourceDetails>();
        Map<String, List<DiscoveredResourceDetails>> duplicatesByKey = new LinkedHashMap<String, List<DiscoveredResourceDetails>>();

        // get discovered processes that match the process-scan
        @SuppressWarnings("unchecked")
        List<ProcessScanResult> discoveredProcesses = context.getAutoDiscoveredProcesses();

        for (ProcessScanResult process : discoveredProcesses) {
            try {
                ProcessInfo processInfo = process.getProcessInfo();
                DiscoveredResourceDetails details1 = discoverResourceDetails(context, processInfo);
                if (details1 != null) {
                    if (discoveredResources.contains(details1)) {
                        List<DiscoveredResourceDetails> duplicates = duplicatesByKey.get(details1.getResourceKey());
                        if (duplicates == null) {
                            duplicates = new ArrayList<DiscoveredResourceDetails>();
                            duplicatesByKey.put(details1.getResourceKey(), duplicates);
                        }
                        duplicates.add(details1);
                    }
                    discoveredResources.add(details1);
                }
            } catch (RuntimeException re) {
                re.printStackTrace();
                // Don't let a runtime exception for a particular ProcessInfo cause the entire discovery scan to fail.
                if (log.isDebugEnabled()) {
                    log.debug("Error when trying to discover Fuse Server process [" +
                          process + "].", re);
                } else {
                    log.warn("Error when trying to discover Fuse Server process [" +
                          process + "] (enable DEBUG for stack trace): " + re);
                }
            }
        }

        for (String duplicateKey : duplicatesByKey.keySet()) {
            List<DiscoveredResourceDetails> duplicates = duplicatesByKey.get(duplicateKey);
            log.error("Multiple Resources with the same key (" + duplicateKey
                  + ") were discovered - none will be reported to the plugin container! This most likely means that there are multiple java processes running with the same value for the "
                  + getResourceKeyProperty(context.getDefaultPluginConfiguration()) + " system property specified on their command lines. Here is the list of Resources: "
                  + duplicates);
            discoveredResources.remove(duplicates.get(0));
        }

        // populate Fuse Server specific properties
        Set<DiscoveredResourceDetails> resultSet = new HashSet<DiscoveredResourceDetails>();
        for (DiscoveredResourceDetails details : discoveredResources) {
            try {
                if (!populateResourceProperties(context, details)) {
                    // required resource type 'marker' property was missing, for e.g. a custom versionFile
                    continue;
                }
                // configure log file after resource properties are set
                final Configuration pluginConfiguration = details.getPluginConfiguration();
                final ProcessInfo processInfo = details.getProcessInfo();
                final String homePath = getSystemPropertyValue(processInfo,
                      pluginConfiguration.getSimpleValue(PluginUtil.HOME_PROPERTY));
                initLogEventSourcesConfigProp(new File(homePath), pluginConfiguration, processInfo);

                resultSet.add(details);
            } catch (InvalidPluginConfigurationException e) {
                log.warn(String.format("Ignoring resource %s, due to error: %s",
                      details.getResourceName(), e.getMessage()), e);
            }
        }

        return resultSet;
    }

    // Manual Add
    public final DiscoveredResourceDetails discoverResource(Configuration pluginConfig,
                                                            ResourceDiscoveryContext discoveryContext)
          throws InvalidPluginConfigurationException {
        final String resourceTypeName = discoveryContext.getResourceType().getName();

        // get the user provided Connector Address
        final String connectorAddress = pluginConfig.getSimpleValue(
              JMXDiscoveryComponent.CONNECTOR_ADDRESS_CONFIG_PROPERTY, null);
        if (connectorAddress == null) {
            throw new InvalidPluginConfigurationException(
                  "A connector address must be specified when manually adding a " +
                        resourceTypeName);
        }

        // property for JMXServerComponent to use to connect to the process later
        // also needed by ConnectionProviderFactory.createConnectionProvider below
        pluginConfig.put(new PropertySimple(JMXDiscoveryComponent.CONNECTION_TYPE,
              J2SE5ConnectionTypeDescriptor.class.getName()));

        // check whether we can connect to the process
        ConnectionProvider connectionProvider;
        EmsConnection connection;
        try {
            connectionProvider = ConnectionProviderFactory.createConnectionProvider(pluginConfig, null,
                  discoveryContext.getParentResourceContext().getTemporaryDirectory());
            connection = connectionProvider.connect();
            connection.loadSynchronous(false);
        } catch (Exception e) {
            if (e.getCause() instanceof SecurityException) {
                throw new InvalidPluginConfigurationException("Failed to authenticate to " +
                      resourceTypeName + " with connector address [" + connectorAddress +
                      "] - principal and/or credentials connection properties are not set correctly.");
            }
            throw new RuntimeException("Failed to connect to " + resourceTypeName +
                  " with connector address [" + connectorAddress + "]", e);
        }

        // try to get the actual JVM Process from the EMS connection, returns null if native layer is not available
        final ProcessInfo jvmProcess = getJvmProcess(discoveryContext, connection, connectorAddress);

        // get home path, either from plugin config or discovered process
        final String homePath;
        final String homePropertyName = pluginConfig.getSimpleValue(PluginUtil.HOME_PROPERTY);
        if (jvmProcess == null) {
            homePath = pluginConfig.getSimpleValue(homePropertyName);
        } else {
            homePath = getSystemPropertyValue(jvmProcess, homePropertyName);
        }
        if (homePath == null || homePath.isEmpty()) {
            throw new InvalidPluginConfigurationException("Missing required property " + homePropertyName);
        }

        // create resource details using the JVM process, which may be null if not found
        DiscoveredResourceDetails details = buildResourceDetails(pluginConfig,
              discoveryContext, jvmProcess, connectorAddress);

        // catastrophic failure in manual add if version file is missing
        if (details == null) {
            throw new InvalidPluginConfigurationException(
                  String.format("Version file %s could not be found in %s",
                        pluginConfig.getSimpleValue(PluginUtil.VERSION_FILE_PROPERTY), homePath));
        }

        // add a flag to indicate that this resource was manually added
        pluginConfig.put(new PropertySimple("manuallyAdded", true));

        // populate system properties in plugin properties
        if (!populateResourceProperties(discoveryContext, details)) {
            throw new InvalidPluginConfigurationException(
                  "Error setting plugin properties, check agent log for details (you may have to enable debug first)");
        }

        // configure log file after resource properties are set
        initLogEventSourcesConfigProp(new File(homePath), pluginConfig, jvmProcess);

        return details;
    }

    protected ProcessInfo getJvmProcess(ResourceDiscoveryContext discoveryContext,
                                        EmsConnection connection, String connectorAddress) {

        // check whether native system is supported
        if (!discoveryContext.getSystemInformation().isNative()) {
            log.warn("Native layer is not available or has been disabled, process properties discovery not supported");
            return null;
        }

        ProcessInfo processInfo;
        final String resourceTypeName = discoveryContext.getResourceType().getName();
        try {
            final EmsBean runtimeEmsBean = connection.getBean(ManagementFactory.RUNTIME_MXBEAN_NAME);
            final RuntimeMXBean runtimeMXBean = runtimeEmsBean.getProxy(RuntimeMXBean.class);
            final String runtimeMXBeanName = runtimeMXBean != null ? runtimeMXBean.getName() : null;
            if (runtimeMXBeanName != null && runtimeMXBeanName.contains("@")) {
                final String pid = runtimeMXBeanName.substring(0, runtimeMXBeanName.indexOf('@'));
                processInfo = new ProcessInfo(Long.valueOf(pid));
                // validate process info to make sure command line args are accessible
                // this can happen for processes running with a user id different than the agent
                if (processInfo.getCommandLine() == null ||
                      processInfo.getCommandLine().length == 0) {
                    log.debug("Unable to get command line args for PID [" + pid +
                          "] for [" + resourceTypeName + "], with connector address [" + connectorAddress +
                          "], using java.lang.management.RuntimeMXBean to get JVM args");
                    final List<String> inputArguments = runtimeMXBean.getInputArguments();
                    final String[] args = inputArguments.toArray(new String[inputArguments.size()]);
                    log.debug("JVM args for PID[" + pid + "] using java.lang.management.RuntimeMXBean: " +
                          Arrays.toString(args));
                    processInfo = new ProcessInfoWithArgs(Long.valueOf(pid), args);
                }
            } else {
                throw new RuntimeException("Unable to get Process PID using java.lang.management.RuntimeMXBean for [" +
                      resourceTypeName + "] , with connector address [" + connectorAddress + "]");
            }
        } catch (Exception e) {
            throw new RuntimeException("Error getting Process PID for resource [" +
                  resourceTypeName + "] with JMX connector [" + connectorAddress + "]: " + e.getMessage(), e);
        }

        return processInfo;
    }

    // tries to use the Attach API first to avoid having to use the default JMX connector properties,
    // if that throws an AttachNotSupportedException (karaf causes it for some reason),
    // tried to use jvmstat API to get JMX connector properties (a la jconsole),
    // if that fails, uses getConfigWithJmxServiceUrl() to create JMX connection
    protected DiscoveredResourceDetails discoverResourceDetails(ResourceDiscoveryContext context, ProcessInfo process) {

        Configuration pluginConfig;

        // Start up a JMX agent within the JVM via the Sun Attach API, and return a URL that can be used to connect
        // to that agent.
        // Note, this will only work if the remote JVM is Java 6 or later, and maybe some 64 bit Java 5 - see
        // JBNADM-3332. Also, the RHQ Agent will have to be running on a JDK, not a JRE, so that we can access
        // the JDK's tools.jar, which contains the Sun JVM Attach API classes.
        JMXServiceURL jmxServiceURL;
        try {
            jmxServiceURL = JvmUtility.extractJMXServiceURL(process);
        } catch (RuntimeException e) {
            // avoid loading AttachNotSupportedException class, since its optional
            if (e.getCause() != null &&
                  e.getCause().getClass().getName().equals(PluginUtil.ATTACH_NOT_SUPPORTED_EXCEPTION_CLASS_NAME)) {
                // try connecting using jvmstat
                jmxServiceURL = JvmStatUtility.extractJMXServiceURL(process);
            } else {
                // re-throw
                throw e;
            }
        }
        if (jmxServiceURL != null) {
            pluginConfig = context.getDefaultPluginConfiguration();
        } else {
            // create plugin config with JMX connection properties
            pluginConfig = getConfigWithJmxServiceUrl(context, process);
        }

        // log the JMX URL being used
        if (log.isDebugEnabled()) {
            log.debug("JMX service URL for java process [" + process + "] is [" +
                  (jmxServiceURL != null ? jmxServiceURL :
                        pluginConfig.getSimpleValue(JMXDiscoveryComponent.CONNECTOR_ADDRESS_CONFIG_PROPERTY))
                  + "].");
        }

        return buildResourceDetails(pluginConfig, context, process,
              pluginConfig.getSimpleValue(JMXDiscoveryComponent.CONNECTOR_ADDRESS_CONFIG_PROPERTY));
    }

    // use default configuration, which MUST contain a default value for JMX connector address
    protected Configuration getConfigWithJmxServiceUrl(ResourceDiscoveryContext context, ProcessInfo process) {

        Configuration configuration = context.getDefaultPluginConfiguration();
        if (configuration.getSimpleValue(JMXDiscoveryComponent.CONNECTOR_ADDRESS_CONFIG_PROPERTY) == null) {
            throw new InvalidPluginConfigurationException("Missing Property " +
                  JMXDiscoveryComponent.CONNECTOR_ADDRESS_CONFIG_PROPERTY +
                  " in Resource type " + context.getResourceType().getName());
        }

        return configuration;
    }

    // may return null if the version file is missing
    protected DiscoveredResourceDetails buildResourceDetails(Configuration pluginConfig,
                                                             ResourceDiscoveryContext context, ProcessInfo process,
                                                             String connectorAddress) {
        // this also checks for the resourceKey property in the process if its not null
        final String resourceKey = buildResourceKey(pluginConfig, context, process);

        // find resource home directory
        final Configuration defaultConfig = context.getDefaultPluginConfiguration();
        final String homeProperty = defaultConfig.getSimpleValue(PluginUtil.HOME_PROPERTY);

        // use provided home property value or find the matching system property
        final String homePath;
        if (process == null) {
            homePath = pluginConfig.getSimpleValue(homeProperty);
        } else {
            homePath = getSystemPropertyValue(process, homeProperty);
        }
        if (homePath == null || homePath.isEmpty()) {
            throw new InvalidPluginConfigurationException("Missing required property " + homeProperty);
        }
        final File homeDir = new File(homePath);
        if (!homeDir.exists()) {
            throw new InvalidPluginConfigurationException(
                  String.format("Home directory %s does NOT exist", homePath));
        }

        pluginConfig.setSimpleValue(homeProperty, homePath);

        // get resource name using resourceKey property
        final String name = String.format("%s %s", resourceKey, context.getResourceType().getName());

        // get resource version from versionFile property
        String version = getResourceVersion(defaultConfig, homeDir);
        // this method returns null if the version file can't be found
        if (version == null) {
            return null;
        }
        String description = context.getResourceType().getDescription() +
              ", monitored via " + (connectorAddress == null ? "Sun JVM Attach API" : "JMX Remoting");

        // property for JMXServerComponent to use to connect to the process later
        pluginConfig.put(new PropertySimple(JMXDiscoveryComponent.CONNECTION_TYPE,
              J2SE5ConnectionTypeDescriptor.class.getName()));

        return new DiscoveredResourceDetails(context.getResourceType(), resourceKey, name, version, description,
              pluginConfig, process);
    }

    // configure log file defined in configuration
    protected void initLogEventSourcesConfigProp(File homeDir, Configuration pluginConfiguration, ProcessInfo process) {

        // get log file to track
        String logPath = pluginConfiguration.getSimpleValue(PluginUtil.LOG_FILE_PROPERTY);
        if (logPath == null) {
            log.warn("Missing property " + PluginUtil.LOG_FILE_PROPERTY + " in Fuse Server configuration");
            return;
        }

        // replace any system properties in log file path
        if (logPath.contains("{")) {
            if (process == null) {
                // required properties must be present in pluginConfiguration
                Matcher matcher = Pattern.compile("\\{([^\\}]+)\\}").matcher(logPath);
                while (matcher.find()) {
                    final String key = matcher.group(1);
                    final String value = pluginConfiguration.getSimpleValue(key);
                    if (value == null) {
                        throw new InvalidPluginConfigurationException("Missing property " + key);
                    }
                    logPath = logPath.replaceAll("\\{" + key + "\\}", value);
                }
            } else {
                for (String argument : process.getCommandLine()) {
                    if (argument.startsWith("-D") && argument.contains("=")) {
                        String[] keyValue = argument.substring(2).split("=");
                        logPath = logPath.replaceAll("\\{" + keyValue[0] + "\\}",
                              keyValue[1]);
                    }
                }
            }
        }

        PropertyList logEventSources = pluginConfiguration.getList(
              LogFileEventResourceComponentHelper.LOG_EVENT_SOURCES_CONFIG_PROP);
        if (logEventSources == null) {
            logEventSources = new PropertyList(LogFileEventResourceComponentHelper.LOG_EVENT_SOURCES_CONFIG_PROP);
            pluginConfiguration.put(logEventSources);
        }

        // if logPath is not absolute assume relative to homeDir
        final File logFile = new File(logPath);
        final File serverLogFile = logFile.isAbsolute() ? logFile : new File(homeDir, logPath);
        if (serverLogFile.exists() && !serverLogFile.isDirectory()) {
            PropertyMap agentLogEventSource = new PropertyMap(
                  LogFileEventResourceComponentHelper.LOG_EVENT_SOURCE_CONFIG_PROP);
            agentLogEventSource.put(new PropertySimple(
                  LogFileEventResourceComponentHelper.LogEventSourcePropertyNames.LOG_FILE_PATH, serverLogFile));
            agentLogEventSource.put(new PropertySimple(
                  LogFileEventResourceComponentHelper.LogEventSourcePropertyNames.ENABLED, Boolean.FALSE));
            agentLogEventSource.put(new PropertySimple(
                  LogFileEventResourceComponentHelper.LogEventSourcePropertyNames.MINIMUM_SEVERITY, EventSeverity.ERROR
                  .name()));
            logEventSources.add(agentLogEventSource);
        }

    }

    protected String buildResourceKey(Configuration pluginConfig, ResourceDiscoveryContext context, ProcessInfo process) {

        final String resourceKeyProperty = getResourceKeyProperty(pluginConfig);
        final String keyString;
        if (process == null) {
            keyString = pluginConfig.getSimpleValue(resourceKeyProperty);
        } else {
            keyString = getSystemPropertyValue(process, resourceKeyProperty);
        }

        if (keyString == null || keyString.isEmpty()) {
            if (process == null) {
                throw new InvalidPluginConfigurationException("Missing required property" + resourceKeyProperty);
            } else {
                throw new InvalidPluginConfigurationException("Process [" + process.getPid() + "] with command line "
                      + Arrays.toString(process.getCommandLine())
                      + " cannot be discovered, because it does not specify "
                      + "-D" + resourceKeyProperty + "=UNIQUE_KEY");
            }
        }
        return context.getResourceType().getName() + "{" + keyString + "}";
    }

    protected String getResourceKeyProperty(Configuration pluginConfig) {
        return pluginConfig.getSimpleValue(PluginUtil.RESOURCE_KEY_PROPERTY);
    }

    protected String getSystemPropertyValue(ProcessInfo process, String systemPropertyName) {
        for (String argument : process.getCommandLine()) {
            String prefix = "-D" + systemPropertyName + "=";
            if (argument.startsWith(prefix)) {
                return argument.substring(prefix.length());
            }
        }
        return null;
    }

    protected boolean populateResourceProperties(ResourceDiscoveryContext context, DiscoveredResourceDetails details) {

        // verify values for system properties group if process is not set, or get them from process if set
        List<PropertyDefinition> group = details.getResourceType().getPluginConfigurationDefinition().
              getPropertiesInGroup(PluginUtil.SYSTEM_PROPERTIES_GROUP);

        final ProcessInfo processInfo = details.getProcessInfo();
        final Configuration pluginConfiguration = details.getPluginConfiguration();

        for (PropertyDefinition property : group) {
            final String name = property.getName();
            if (processInfo == null) {
                // make sure required system property is set
                final String value = pluginConfiguration.getSimpleValue(name);
                if (value == null || value.isEmpty()) {
                    throw new InvalidPluginConfigurationException("Missing system property " + name);
                }
            } else {
                pluginConfiguration.setSimpleValue(name, getSystemPropertyValue(processInfo, name));
            }
        }

        return true;
    }

    // set resource version using versionFile resource type property
    protected String getResourceVersion(Configuration defaultConfig, File homeDir) {

      /*  final String versionFilePath = defaultConfig.getSimpleValue(VERSION_FILE_PROPERTY);
        // extract file name from path, file path MUST always use '/' as separator
        final int lastSlash = versionFilePath.lastIndexOf('/');
        final String namePattern = (lastSlash == -1) ? versionFilePath
              : versionFilePath.substring(lastSlash + 1, versionFilePath.length());

        // find version file under home directory
        File versionFile = null;
        if (versionFilePath.startsWith(RECURSIVE_SEARCH_PATH)) {
            final String versionFileSubPath = versionFilePath.substring(RECURSIVE_SEARCH_PATH.length());
            versionFile = findVersionFile(homeDir, Pattern.compile(versionFileSubPath));
        } else {
            // use relative path to find file, only the last segment can have a version pattern
            final File parentPath = (lastSlash == -1) ? homeDir
                  : new File(homeDir, versionFilePath.substring(0, lastSlash));
            if (parentPath.exists() && parentPath.isDirectory()) {
                File[] files = parentPath.listFiles();
                for (File file : files) {
                    if (file.getName().matches(namePattern)) {
                        versionFile = file;
                        break;
                    }
                }
            }
        }

        // version file MUST match to ensure resource type match
        if (versionFile == null) {
            log.debug(String.format("Version file %s not found in %s", versionFilePath, homeDir.getAbsolutePath()));
            return null;
        }*/

        // if versionFile doesn't have a capture group, resource version is UNKNOWN
        String versionStr = "UNKNOWN";

        // if there is a capture group in the version file pattern, use it as the version
        // quick look for a capture group
        /*if (namePattern.indexOf('(') != -1) {
            Pattern pattern = Pattern.compile(namePattern);
            Matcher matcher = pattern.matcher(versionFile.getName());
            if (matcher.find() && matcher.groupCount() > 0) {
                versionStr = matcher.group(1);
            }
        }*/

        return versionStr;
    }

    protected File findVersionFile(File dir, Pattern pattern) {

        if (dir.isDirectory() && dir.canRead()) {
            File[] files = dir.listFiles();
            // reverse sort file names to match highest version of a library
            Arrays.sort(files, new Comparator<File>() {
                public int compare(File o1, File o2) {
                    return o2.getName().compareTo(o1.getName());
                }
            });
            // use a list for ordered sub directory traversal
            List<File> subDirs = new ArrayList<File>();
            for (File file : files) {
                if (file.isDirectory()) {
                    subDirs.add(file);
                    // normalize file name to use '/'
                } else if (pattern.matcher(
                      file.getAbsolutePath().replace(File.separatorChar, '/')).find()) {
                    return file;
                }
            }
            for (File subDir : subDirs) {
                File versionFile = findVersionFile(subDir, pattern);
                if (versionFile != null) {
                    return versionFile;
                }
            }
        } else {
            log.warn(String.format("Unable to access %s", dir));
        }

        return null;
    }

}
