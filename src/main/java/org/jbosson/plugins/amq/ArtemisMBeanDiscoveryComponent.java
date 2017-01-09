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
import org.mc4j.ems.connection.bean.attribute.EmsAttribute;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.configuration.definition.PropertyDefinition;
import org.rhq.core.domain.resource.ResourceType;
import org.rhq.core.pluginapi.inventory.DiscoveredResourceDetails;
import org.rhq.core.pluginapi.inventory.ResourceComponent;
import org.rhq.core.pluginapi.inventory.ResourceContext;
import org.rhq.core.pluginapi.inventory.ResourceDiscoveryContext;
import org.rhq.core.util.MessageDigestGenerator;
import org.rhq.plugins.jmx.JMXComponent;
import org.rhq.plugins.jmx.MBeanResourceComponent;
import org.rhq.plugins.jmx.MBeanResourceDiscoveryComponent;
import org.rhq.plugins.jmx.util.ObjectNameQueryUtility;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Adds support to {@link org.rhq.plugins.jmx.MBeanResourceDiscoveryComponent} to allow mapping
 * bean attributes to resource properties.
 * <p/>
 * Also adds support for using parent resource properties in nameTemplate and descriptionTemplate.
 * <p/>
 * Also allows skipping unknown properties in objectName, specifically added for CXF.
 *
 * @author dbokde
 */
public class ArtemisMBeanDiscoveryComponent<T extends JMXComponent<?>> extends MBeanResourceDiscoveryComponent<T> {

    private final Log log = LogFactory.getLog(getClass());

    private static final Pattern PROPERTY_NAME_PATTERN = Pattern.compile("\\{([^\\{\\},=:\\*\\?]+)\\}");
    private static final Pattern OBJECT_NAME_PROPERTY_PATTERN = Pattern.compile("\\%([^\\%,=:\\*\\?]+)\\%");
    private static final String OPTIONAL_PROPERTY_DEFAULT_VALUE = "-";

    protected static final int MAX_LENGTH = 500;

    private static final String BEAN_PROPERTIES = "beanProperties";
    private static final String SKIP_UNKNOWN_PROPS_PROPERTY = "skipUnknownProps";
    public static final String STATS_OBJECT_NAME_PROPERTY = "statsObjectName";
    protected ResourceDiscoveryContext<T> discoveryContext;

    @Override
    public Set<DiscoveredResourceDetails> discoverResources(ResourceDiscoveryContext<T> context) {

        this.discoveryContext = context;

        // call super to get resources, skip unknown props by default if not set in resource plugin config
        Set<DiscoveredResourceDetails> detailsSet = super.discoverResources(context,
            Boolean.parseBoolean(context.getDefaultPluginConfiguration().getSimpleValue(SKIP_UNKNOWN_PROPS_PROPERTY, "true")));

        // get the beanProperties property group
        final List<PropertyDefinition> beanProperties = context.getResourceType().getPluginConfigurationDefinition().
            getPropertiesInGroup(BEAN_PROPERTIES);

        // now substitute any parent resource plugin properties and MBean properties in name and description
        final Set<DiscoveredResourceDetails> resultSet = new HashSet<DiscoveredResourceDetails>();
        for (DiscoveredResourceDetails details : detailsSet) {

            // check if there are any bean beanProperties to set in the first place
            if (!beanProperties.isEmpty()) {
                if (!setBeanProperties(context, details, beanProperties)) {
                    // bean is ignored if required bean beanProperties are not found
                    // so this acts like a second filter, in addition to the objectName args
                    continue;
                }
            }

            final ResourceContext<?> parentContext = context.getParentResourceContext();
            // if it has a parent and we need to replace something in name or description
            if (parentContext != null &&
                (details.getResourceName().contains("{") || details.getResourceDescription().contains("}"))) {

                final Configuration parentConfiguration = parentContext.getPluginConfiguration();
                final Map<String, PropertySimple> simpleProperties = parentConfiguration.getSimpleProperties();

                setResourceNameAndDescription(details, simpleProperties.values());
            }

            processStatsObjectName(context, details);

            // pass along these details
            resultSet.add(details);
        }

        return resultSet;
    }

    // Substitutes parent and plugin properties in statsObjectName if present
    private void processStatsObjectName(ResourceDiscoveryContext discoveryContext, DiscoveredResourceDetails details) {
        final Configuration pluginConfiguration = details.getPluginConfiguration();
        final Configuration parentConfiguration = (discoveryContext.getParentResourceContext() != null) ?
            discoveryContext.getParentResourceContext().getPluginConfiguration() : null;
        String statsObjectName = pluginConfiguration.getSimpleValue(STATS_OBJECT_NAME_PROPERTY);
        if (statsObjectName != null) {
            // get stats object name and substitute resource and parent properties
            statsObjectName = substituteConfigProperties(statsObjectName, pluginConfiguration, false);
            if (parentConfiguration != null) {
                statsObjectName = substituteConfigProperties(statsObjectName, parentConfiguration, true);
            }
            pluginConfiguration.setSimpleValue(STATS_OBJECT_NAME_PROPERTY, statsObjectName);
        }
    }

    private String substituteConfigProperties(String objectName, Configuration configuration, boolean isParent) {

        final Pattern p;
        if (isParent) {
            p = PROPERTY_NAME_PATTERN;
        } else {
            p = OBJECT_NAME_PROPERTY_PATTERN;
        }

        StringBuffer buffer = new StringBuffer();
        final Matcher m = p.matcher(objectName);
        while (m.find()) {
            String name = m.group(1);
            m.appendReplacement(buffer, configuration.getSimpleValue(name));
        }
        m.appendTail(buffer);
        return buffer.toString();
    }

    private boolean setBeanProperties(ResourceDiscoveryContext<T> context, DiscoveredResourceDetails details, List<PropertyDefinition> beanProperties) {
        // get all MBean properties from details, would this cause a reconnect???
        String beanName = details.getPluginConfiguration().getSimpleValue(PROPERTY_OBJECT_NAME);
        EmsBean bean = context.getParentResourceComponent().getEmsConnection().getBean(beanName);

        final List<PropertySimple> newProperties = new ArrayList<PropertySimple>(beanProperties.size());
        for (PropertyDefinition property : beanProperties) {
            String name = property.getName();
            EmsAttribute attribute = bean.getAttribute(name);

            if (attribute == null) {
                if (property.isRequired()) {
                    // missing property, ignore this bean resource
                    log.warn(String.format("Required Property %s is missing in Bean %s, ignoring Bean for resource type %s",
                        name, beanName, details.getResourceType().getName()));
                    return false;
                }
                // set default value for optional property
                newProperties.add(new PropertySimple(name, OPTIONAL_PROPERTY_DEFAULT_VALUE));
            } else {
                final String value = attribute.getValue().toString();
                // set the property as a simple String value in plugin configuration
                details.getPluginConfiguration().setSimpleValue(name, value);

                // collect properties for updating resource name and description
                newProperties.add(new PropertySimple(name, value));
            }
        }

        // update name and description using discovered properties
        setResourceNameAndDescription(details, newProperties);

        return true;
    }

    private void setResourceNameAndDescription(DiscoveredResourceDetails details,
                                               Collection<PropertySimple> simpleProperties) {

        String resourceName = details.getResourceName();
        String description = details.getResourceDescription();

        final boolean nameHasTemplate = resourceName.contains("{");
        final boolean descriptionHasTemplate = description.contains("}");
        if (nameHasTemplate || descriptionHasTemplate) {
            Map<String, String> variableValues = convertToMap(simpleProperties);
            if (nameHasTemplate) {
                resourceName = formatMessage(resourceName, variableValues);
            }
            if (descriptionHasTemplate) {
                description = formatMessage(description, variableValues);
            }

            details.setResourceName(resourceName);
            details.setResourceDescription(description);
        }
    }

    private Map<String, String> convertToMap(Collection<PropertySimple> simpleProperties) {
        final HashMap<String, String> result = new HashMap<String, String>();
        for (PropertySimple property : simpleProperties) {
            result.put(property.getName(), property.getStringValue());
        }
        return result;
    }

    @Override
    public Set<DiscoveredResourceDetails> performDiscovery(Configuration pluginConfiguration,
                                                           JMXComponent parentResourceComponent,
                                                           ResourceType resourceType, boolean skipUnknownProps) {

        String objectNameQueryTemplateOrig = pluginConfiguration.getSimple(PROPERTY_OBJECT_NAME).getStringValue();

        log.debug("Discovering MBean resources with object name query template: " + objectNameQueryTemplateOrig);

        EmsConnection connection = parentResourceComponent.getEmsConnection();

        if (connection == null) {
            throw new NullPointerException("The parent resource component [" + parentResourceComponent
                + "] returned a null connection - cannot discover MBeans without a connection");
        }

        Set<DiscoveredResourceDetails> services = new HashSet<DiscoveredResourceDetails>();
        String templates[] = objectNameQueryTemplateOrig.split("\\|");
        for (String objectNameQueryTemplate : templates) {
            if (discoveryContext.getParentResourceContext() == null) {
                System.out.println("ArtemisMBeanDiscoveryComponent.performDiscovery");
            }
            // Get the query template, replacing the parent key variables with the values from the parent configuration
            ObjectNameQueryUtility queryUtility = new ObjectNameQueryUtility(objectNameQueryTemplate,
                (this.discoveryContext != null) ? this.discoveryContext.getParentResourceContext()
                    .getPluginConfiguration() : null);
            String translatedQuery = queryUtility.getTranslatedQuery();
            //this is a hack, there is no way to get a handle on the parents parents configuration!!!!!
            if (queryUtility.getQueryTemplate().contains("{brokerName}")) {
                String brokerName = ((MBeanResourceComponent) parentResourceComponent).getEmsBean().getBeanName().getKeyProperty("brokerName");
                translatedQuery = translatedQuery.replace("{brokerName}", brokerName);
            }
            List<EmsBean> beans = connection.queryBeans(translatedQuery);
            if (log.isDebugEnabled()) {
                log.debug("Found [" + beans.size() + "] mbeans for query [" + translatedQuery + "].");
            }
            for (EmsBean bean : beans) {
                if (queryUtility.setMatchedKeyValues(bean.getBeanName().getKeyProperties())) {
                    // Only use beans that have all the properties we've made variables of

                    // Don't match beans that have unexpected properties
                    if (skipUnknownProps
                        && queryUtility.isContainsExtraKeyProperties(bean.getBeanName().getKeyProperties().keySet())) {
                        continue;
                    }

                    String resourceKey = bean.getBeanName().getCanonicalName(); // The detected object name

                    String nameTemplate = (pluginConfiguration.getSimple(PROPERTY_NAME_TEMPLATE) != null) ? pluginConfiguration
                        .getSimple(PROPERTY_NAME_TEMPLATE).getStringValue() : null;

                    String descriptionTemplate = (pluginConfiguration.getSimple(PROPERTY_DESCRIPTION_TEMPLATE) != null) ? pluginConfiguration
                        .getSimple(PROPERTY_DESCRIPTION_TEMPLATE).getStringValue() : null;

                    String name = resourceKey;

                    if (nameTemplate != null) {
                        name = formatMessage(nameTemplate, queryUtility.getVariableValues());
                    }

                    String description = null;
                    if (descriptionTemplate != null) {
                        description = formatMessage(descriptionTemplate, queryUtility.getVariableValues());
                    }

                    if (resourceKey.length() > MAX_LENGTH) {
                        resourceKey = new MessageDigestGenerator(MessageDigestGenerator.SHA_256).calcDigestString(
                            resourceKey);
                    }
                    DiscoveredResourceDetails service = new DiscoveredResourceDetails(resourceType, resourceKey,
                        name, "", description, null, null);
                    Configuration config = service.getPluginConfiguration();
                    config.put(new PropertySimple(PROPERTY_OBJECT_NAME, bean.getBeanName().toString()));

                    Map<String, String> mappedVariableValues = queryUtility.getVariableValues();
                    for (String key : mappedVariableValues.keySet()) {
                        config.put(new PropertySimple(key, mappedVariableValues.get(key)));
                    }

                    services.add(service);

                    // Clear out the variables for the next bean detected
                    queryUtility.resetVariables();
                }
            }

            if (log.isDebugEnabled()) {
                log.debug("[" + services.size() + "] services have been added");
            }

        }

        return services;
    }

    protected static String formatMessage(String messageTemplate, Map<String, String> variableValues) {

        StringBuffer result = new StringBuffer();

        // collect keys and values to determine value size limit if needed
        final Map<String, String> replaceMap = new HashMap<String, String>();

        final Matcher matcher = PROPERTY_NAME_PATTERN.matcher(messageTemplate);
        int count = 0;
        while (matcher.find()) {
            final String key = matcher.group(1);
            final String value = variableValues.get(key);

            if (value != null) {
                replaceMap.put(key, value);
                matcher.appendReplacement(result, Matcher.quoteReplacement(value));
            } else {
                matcher.appendReplacement(result, Matcher.quoteReplacement(matcher.group()));
            }

            count++;
        }
        matcher.appendTail(result);

        // check if the result exceeds MAX_LENGTH for formatted properties
        if (!replaceMap.isEmpty() && result.length() > MAX_LENGTH) {
            // sort values according to size
            final SortedSet<String> values = new TreeSet<String>(new Comparator<String>() {
                public int compare(String o1, String o2) {
                    return o1.length() - o2.length();
                }
            });
            values.addAll(replaceMap.values());

            // find total value characters allowed
            int available = MAX_LENGTH - PROPERTY_NAME_PATTERN.matcher(messageTemplate).replaceAll("").length();

            // fit values from small to large in the allowed size to determine the maximum average
            int averageLength = available / count;
            for (String value : values) {
                final int length = value.length();
                if (length > averageLength) {
                    break;
                }
                available -= length;
                count--;
                averageLength = available / count;
            }

            // replace values
            matcher.reset();
            result.delete(0, result.length());
            while (matcher.find()) {
                String value = replaceMap.get(matcher.group(1));
                if (value != null && value.length() > averageLength) {
                    value = value.substring(0, averageLength);
                }
                matcher.appendReplacement(result, value != null ? value : matcher.group());
            }
            matcher.appendTail(result);
        }

        return result.toString();
    }
}
