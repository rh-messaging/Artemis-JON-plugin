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
import org.hyperic.sigar.OperatingSystem;
import org.hyperic.sigar.SigarException;
import org.mc4j.ems.connection.ConnectionFactory;
import org.mc4j.ems.connection.EmsConnection;
import org.mc4j.ems.connection.bean.EmsBean;
import org.mc4j.ems.connection.bean.EmsBeanName;
import org.mc4j.ems.connection.settings.ConnectionSettings;
import org.mc4j.ems.connection.support.ConnectionProvider;
import org.mc4j.ems.connection.support.metadata.ConnectionTypeDescriptor;
import org.mc4j.ems.connection.support.metadata.J2SE5ConnectionTypeDescriptor;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.measurement.AvailabilityType;
import org.rhq.core.pluginapi.availability.AvailabilityContext;
import org.rhq.core.pluginapi.event.log.LogFileEventResourceComponentHelper;
import org.rhq.core.pluginapi.inventory.InvalidPluginConfigurationException;
import org.rhq.core.pluginapi.inventory.ResourceComponent;
import org.rhq.core.pluginapi.inventory.ResourceContext;
import org.rhq.core.pluginapi.operation.OperationFacet;
import org.rhq.core.pluginapi.operation.OperationResult;
import org.rhq.core.pluginapi.util.ProcessExecutionUtility;
import org.rhq.core.pluginapi.util.StartScriptConfiguration;
import org.rhq.core.system.ProcessExecution;
import org.rhq.core.system.ProcessExecutionResults;
import org.rhq.core.system.ProcessInfo;
import org.rhq.core.system.SystemInfo;
import org.rhq.plugins.jmx.JMXComponent;
import org.rhq.plugins.jmx.JMXDiscoveryComponent;
import org.rhq.plugins.jmx.util.ConnectionProviderFactory;

import javax.management.remote.JMXServiceURL;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Adds log file tracking support.
 *
 * @author dbokde
 */
public class ArtemisServerComponent<T extends ResourceComponent<?>> implements JMXComponent<T>, OperationFacet {

    private static final Log log = LogFactory.getLog(ArtemisServerComponent.class);
    public static final boolean OS_IS_WINDOWS = (File.separatorChar == '\\');
    private static final String SEPARATOR = "\n-----------------------\n";
    private static final int RETRIES = 60;
    private static final int TIMEOUT_INTERVAL = 1000;

    private static final String AMQ_BASE_PROPERTY = "artemis.instance";

    // needed to be copied from JMXServerComponent, since these are not protected, gaahhh!
    protected EmsConnection connection;
    protected ConnectionProvider connectionProvider;

    // needed for log file tracking
    protected LogFileEventResourceComponentHelper logFileEventDelegate;

    private StartScriptConfiguration startScriptConfig;
    private Configuration pluginConfiguration;


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
        pluginConfiguration = context.getPluginConfiguration();

        // start log file tracking
        logFileEventDelegate = new LogFileEventResourceComponentHelper(context);
        logFileEventDelegate.startLogFileEventPollers();

        // load start script config
        this.startScriptConfig = new StartScriptConfiguration(context.getPluginConfiguration());
    }

    protected void internalStart() throws Exception {
        Configuration pluginConfig = context.getPluginConfiguration();
        String connectionTypeDescriptorClassName = pluginConfig.getSimple(JMXDiscoveryComponent.CONNECTION_TYPE)
            .getStringValue();
        PluginUtil.loadPluginConfiguration(pluginConfig);

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
                        PluginUtil.ATTACH_NOT_SUPPORTED_EXCEPTION_CLASS_NAME)) {

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

    public OperationResult invokeOperation(String name, Configuration parameters) throws InterruptedException, Exception {
        if (name.equals("start")) {
            return startServer();
        } else if (name.equals("restart")) {
            return restartServer();
        } else if (name.equals("stop")) {
            return stopServer();
        } else throw new UnsupportedOperationException(name);
    }

    /**
     * Start the server by calling the start script defined in the plugin configuration.
     *
     * @return the result of the operation
     */
    protected OperationResult startServer() {
        OperationResult operationResult = new OperationResult();
        if (isManuallyAddedServer(operationResult, "Starting")) {
            return operationResult;
        }

        List<String> errors = validateStartScriptPluginConfigProps();
        if (!errors.isEmpty()) {
            setErrorMessage(operationResult, errors);
            return operationResult;
        }

        String startScriptPrefix = startScriptConfig.getStartScriptPrefix();
        File startScriptFile = getStartScriptFile();

        ProcessExecution processExecution = ProcessExecutionUtility.createProcessExecution(startScriptPrefix,
                 startScriptFile);

        List<String> arguments = processExecution.getArguments();
        if (arguments == null) {
            arguments = new ArrayList<String>();
        }

        //start server command is ./artemis run
        arguments.add(0, "run");
        processExecution.setArguments(arguments);

        List<String> startScriptArgs = startScriptConfig.getStartScriptArgs();
        for (String startScriptArg : startScriptArgs) {
            startScriptArg = replacePropertyPatterns(startScriptArg);
            arguments.add(startScriptArg);
        }

        Map<String, String> startScriptEnv = startScriptConfig.getStartScriptEnv();
        for (String envVarName : startScriptEnv.keySet()) {
            String envVarValue = startScriptEnv.get(envVarName);
            envVarValue = replacePropertyPatterns(envVarValue);
            startScriptEnv.put(envVarName, envVarValue);
        }

        if (startScriptEnv.get("JAVA_HOME") == null) {
            String javaHome = System.getProperty("java.home");
            startScriptEnv.put("JAVA_HOME", javaHome);
        }
        processExecution.setEnvironmentVariables(startScriptEnv);

        processExecution.setWorkingDirectory(startScriptFile.getParent());
        processExecution.setCaptureOutput(true);
        processExecution.setWaitForCompletion(15000L); // 15 seconds // TODO: Should we wait longer than 15 seconds?
        processExecution.setKillOnTimeout(false);

        if (log.isDebugEnabled()) {
            log.debug("About to execute the following process: [" + processExecution + "]");
        }
        SystemInfo systemInfo = context.getSystemInformation();
        ProcessExecutionResults results = systemInfo.executeProcess(processExecution);
        logExecutionResults(results);
        if (results.getError() != null) {
            operationResult.setErrorMessage(results.getError().getMessage());
        } else if (results.getExitCode() != null && results.getExitCode().intValue() != 0) {
            operationResult.setErrorMessage("Start failed with error code " + results.getExitCode() + ":\n" + results.getCapturedOutput());
        } else {
            // Try to connect to the server - ping once per second, timing out after 60s.
            boolean up = waitForServerToStart(results);
            if (up) {
                operationResult.setSimpleResult("Success");
            } else {
                operationResult.setErrorMessage("Was not able to start the server");
            }
        }
        context.getAvailabilityContext().requestAvailabilityCheck();

        return operationResult;
    }

    /**
     * Restart the server by first executing the 'stop' script in karaf.base and then calling
     * the {@link #startServer} method to start it again.
     *
     * @return State of execution
     * @throws Exception If anything goes wrong
     */
    protected OperationResult restartServer() throws Exception {

        OperationResult operationResult = new OperationResult();

        if (isManuallyAddedServer(operationResult, "Restarting")) {
            return operationResult;
        }

        operationResult = stopServer();
        if (operationResult.getErrorMessage() != null) {
            operationResult.setErrorMessage("Restart failed while attempting to shut down: " + operationResult.getErrorMessage());
            return operationResult;
        }

        return startServer();
    }

    private OperationResult stopServer() {
        OperationResult operationResult = new OperationResult();

        if (isManuallyAddedServer(operationResult, "Stopping")) {
            return operationResult;
        }

        List<String> errors = validateStartScriptPluginConfigProps();
        if (!errors.isEmpty()) {
            OperationResult result  = new OperationResult();
            setErrorMessage(result, errors);
            return result;
        }

        final String amqBinPath = pluginConfiguration.getSimpleValue(AMQ_BASE_PROPERTY) + File.separator + "bin";

        final ProcessExecution processExecution = ProcessExecutionUtility.createProcessExecution(
                 new File(amqBinPath, OS_IS_WINDOWS ? "artemis.bat" : "artemis"));


        List<String> arguments = new ArrayList<String>();
        arguments.add("stop");
        processExecution.setWorkingDirectory(amqBinPath);
        processExecution.addArguments(arguments);
        processExecution.setCaptureOutput(true);
        processExecution.setWaitForCompletion(15000L); // 15 seconds // TODO: Should we wait longer than 15 seconds?
        processExecution.setKillOnTimeout(false);

        if (log.isDebugEnabled()) {
            log.debug("About to execute the following process: [" + processExecution + "]");
        }
        SystemInfo systemInfo = context.getSystemInformation();
        ProcessExecutionResults results = systemInfo.executeProcess(processExecution);
        logExecutionResults(results);
        if (results.getError() != null) {
            operationResult.setErrorMessage(results.getError().getMessage());
        } else if (results.getExitCode() == null || results.getExitCode() != 0) {
            operationResult.setErrorMessage("Stop failed with error code: " + results.getExitCode() + ":\n" + results.getCapturedOutput());
        } else {
            // Try to disconnect from the server - ping once per second, timing out after 60s.
            boolean down = waitForServerToStop();

            //try to kill the process if shutdown failed, only works for agents with native support
            if (!down) {
                if (getResourceContext().getSystemInformation().isNative()) {

                    ProcessInfo nativeProcess = getResourceContext().getNativeProcess();
                    if (nativeProcess != null) {
                        final long pid = nativeProcess.getPid();
                        log.error("Shutdown failed, trying to send SIGKILL to process " + pid);

                        try {
                            nativeProcess.kill(OperatingSystem.IS_WIN32 ? "KILL" : "SIGKILL");
                            log.info("Successfully sent SIGKILL to process " + pid);
                            down = waitForServerToStop();
                        } catch (SigarException e) {
                            operationResult.setErrorMessage(String.format("Error killing process %s: %s",
                                     pid, e.getMessage()));
                        }
                    }
                }
            }
            if (down) {
                operationResult.setSimpleResult("Success");
            } else {
                operationResult.setErrorMessage("Was not able to stop the server");
            }
        }

        context.getAvailabilityContext().requestAvailabilityCheck();

        return operationResult;
    }

    private boolean waitForServerToStart(ProcessExecutionResults results) {
        boolean up = false;
        int count = 0;
        while (!up && count < RETRIES) {
            try{
                // check availability first
                final AvailabilityContext availabilityContext = getResourceContext().getAvailabilityContext();
                availabilityContext.requestAvailabilityCheck();
                if (availabilityContext.getLastReportedAvailability() == AvailabilityType.UP) {
                    Set<EmsBean> emsBeans = getEmsConnection().getBeans();
                    if (emsBeans != null && !emsBeans.isEmpty()) {
                        for (EmsBean bean : emsBeans) {
                            final EmsBeanName beanName = bean.getBeanName();
                            if ("org.apache.activemq.artemis".equals(beanName.getDomain())) {
                                up = true;
                                break;
                            }
                        }
                    }
                }
            } catch (Exception e) {
                //do absolutely nothing
                //if an exception is thrown that means the server is still down, so consider this
                //a single failed attempt, equivalent to res.isSuccess == false
            }

            if (!up) {
                try {
                    Thread.sleep(TIMEOUT_INTERVAL); // Wait 1s
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            count++;
        }

        if (!up) {
            String output = results.getCapturedOutput();
            if (output.contains("AMQ221007")) {
                //AMQ221007 means "Server is now live"
                //We can be sure that the server is up
                up = true;
            }
        }
        return up;
    }

    private boolean waitForServerToStop() {
        boolean down = false;
        int count = 0;
        while (!down && count < RETRIES) {
            try {
                // check availability first
                final AvailabilityContext availabilityContext = getResourceContext().getAvailabilityContext();
                availabilityContext.requestAvailabilityCheck();
                if (availabilityContext.getLastReportedAvailability() == AvailabilityType.DOWN) {
                    down = true;
                }
            } catch (Exception e) {
                //do absolutely nothing
                //if an exception is thrown that means the server is still down, so consider this
                //a single failed attempt, equivalent to res.isSuccess == false
            }

            if (!down) {
                try {
                    Thread.sleep(TIMEOUT_INTERVAL); // Wait 1s
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            count++;
        }
        return down;
    }

    private boolean isManuallyAddedServer(OperationResult operationResult, String operation) {
        /*
        if (pluginConfiguration.get("manuallyAdded")!=null) {
            operationResult.setErrorMessage(operation + " is not enabled for manually added servers");
            return true;
        }
        */
        return false;
    }

    private List<String> validateStartScriptPluginConfigProps() {
        List<String> errors = new ArrayList<String>();

        File startScriptFile = getStartScriptFile();

        if (!startScriptFile.exists()) {
            errors.add("Start script '" + startScriptFile + "' does not exist.");
        } else {
            if (!startScriptFile.isFile()) {
                errors.add("Start script '" + startScriptFile + "' is not a regular file.");
            } else {
                if (!startScriptFile.canRead()) {
                    errors.add("Start script '" + startScriptFile + "' is not readable.");
                }
                if (!startScriptFile.canExecute()) {
                    errors.add("Start script '" + startScriptFile + "' is not executable.");
                }
            }
        }

        Map<String, String> startScriptEnv = startScriptConfig.getStartScriptEnv();
        if (startScriptEnv.isEmpty()) {
            errors.add("No start script environment variables are set. At a minimum, PATH should be set "
                     + "(on UNIX, it should contain at least /bin and /usr/bin). It is recommended that "
                     + "JAVA_HOME also be set, otherwise the PATH will be used to find java.");
        }

        return errors;
    }

    private File getStartScriptFile() {
        File startScriptFile = startScriptConfig.getStartScript();
        File homeDir = new File(pluginConfiguration.getSimpleValue(
                 AMQ_BASE_PROPERTY));
        if (startScriptFile != null) {
            if (!startScriptFile.isAbsolute()) {
                startScriptFile = new File(homeDir, startScriptFile.getPath());
            }
        } else {
            // Use the default start script.
            String startScriptFileName = (OS_IS_WINDOWS ? "artemis.bat" : "artemis");
            File binDir = new File(homeDir, "bin");
            startScriptFile = new File(binDir, startScriptFileName);
        }
        return startScriptFile;
    }

    private void setErrorMessage(OperationResult operationResult, List<String> errors) {
        StringBuilder buffer = new StringBuilder("This Resource's connection properties contain errors: ");
        for (int i = 0, errorsSize = errors.size(); i < errorsSize; i++) {
            if (i != 0) {
                buffer.append(", ");
            }
            String error = errors.get(i);
            buffer.append('[').append(error).append(']');
        }
        operationResult.setErrorMessage(buffer.toString());
    }
    // Replace any "%xxx%" substrings with the values of plugin config props "xxx".
    private String replacePropertyPatterns(String value) {
        Pattern pattern = Pattern.compile("(%([^%]*)%)");
        Matcher matcher = pattern.matcher(value);
        Configuration pluginConfig = context.getPluginConfiguration();
        StringBuffer buffer = new StringBuffer();
        while (matcher.find()) {
            String propName = matcher.group(2);
            PropertySimple prop = pluginConfig.getSimple(propName);
            String propValue = ((prop != null) && (prop.getStringValue() != null)) ? prop.getStringValue() : "";
            String propPattern = matcher.group(1);
            String replacement = (prop != null) ? propValue : propPattern;
            matcher.appendReplacement(buffer, Matcher.quoteReplacement(replacement));
        }

        matcher.appendTail(buffer);
        return buffer.toString();
    }

    private void logExecutionResults(ProcessExecutionResults results) {
        log.info("Exit code from process execution: " + results.getExitCode());
        log.info("Output from process execution: " + SEPARATOR + results.getCapturedOutput() + SEPARATOR);
    }

}
