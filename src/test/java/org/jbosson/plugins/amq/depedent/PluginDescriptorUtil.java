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
package org.jbosson.plugins.amq.depedent;

import org.apache.commons.jxpath.JXPathContext;
import org.rhq.core.clientapi.agent.metadata.ConfigurationMetadataParser;
import org.rhq.core.clientapi.agent.metadata.PluginMetadataParser;
import org.rhq.core.clientapi.descriptor.DescriptorPackages;
import org.rhq.core.domain.configuration.definition.ConfigurationDefinition;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.util.ValidationEventCollector;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.StringReader;
import java.net.URL;

import org.rhq.core.clientapi.descriptor.plugin.PluginDescriptor;
import org.rhq.core.clientapi.descriptor.plugin.ResourceDescriptor;

/**
 * Utils class from rhq-core-client-api
 * because the test-jar is not available on maven repo
 * we copied it here.
 */
public class PluginDescriptorUtil {

   public static PluginDescriptor loadPluginDescriptor(String file) {
      URL pluginDescriptorURL = PluginDescriptorUtil.class.getClassLoader().getResource(file);
      if (pluginDescriptorURL == null) {
         throw new RuntimeException("File " + file + " not found");
      }
      return loadPluginDescriptor(pluginDescriptorURL);
   }

   /**
    * Loads the plugin descriptor from the specified file. The file path specified should
    * be a class path relative path. For example, if the descriptor file is located at
    * org.rhq.enterprise.server.configuration.my-descriptor.xml, then you should specify
    * /org/rhq/enterprise/server/configuration/my-descriptor.xml.
    *
    * @param pluginDescriptorURL The class path relative path of the descriptor file
    * @return The {@link PluginDescriptor}
    */
   public static PluginDescriptor loadPluginDescriptor(URL pluginDescriptorURL) {
      try {
         JAXBContext jaxbContext = JAXBContext.newInstance(DescriptorPackages.PC_PLUGIN);
         URL pluginSchemaURL = PluginDescriptorUtil.class.getClassLoader().getResource("rhq-plugin.xsd");
         Schema pluginSchema = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI).newSchema(
                  pluginSchemaURL);
         Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
         ValidationEventCollector vec = new ValidationEventCollector();
         unmarshaller.setEventHandler(vec);
         unmarshaller.setSchema(pluginSchema);

         return (PluginDescriptor) unmarshaller.unmarshal(pluginDescriptorURL.openStream());
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   /**
    * Transforms the given string into a plugin descriptor object.
    *
    * @param string The plugin descriptor specified as a string
    * @return The {@link PluginDescriptor}
    */
   public static PluginDescriptor toPluginDescriptor(String string) {
      try {
         JAXBContext jaxbContext = JAXBContext.newInstance(DescriptorPackages.PC_PLUGIN);
         URL pluginSchemaURL = PluginMetadataParser.class.getClassLoader().getResource("rhq-plugin.xsd");
         Schema pluginSchema = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI).newSchema(pluginSchemaURL);

         Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
         ValidationEventCollector vec = new ValidationEventCollector();
         unmarshaller.setEventHandler(vec);
         unmarshaller.setSchema(pluginSchema);

         StringReader reader = new StringReader(string);

         return (PluginDescriptor) unmarshaller.unmarshal(reader);
      }
      catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   /**
    * Searches for and returns the matching descriptor element (or elements) from the plugin
    * descriptor. <code>path</code> should be an XPath-like expression. See
    * http://commons.apache.org/jxpath/users-guide.html for more information on supported syntax.
    *
    * @param descriptor The plugin descriptor
    * @param path The XPath-ish search expression
    * @return The matching element or elements from the descriptor
    */
   public static Object find(PluginDescriptor descriptor, String path) {
      JXPathContext context = JXPathContext.newContext(descriptor);
      return context.getValue(path);
   }

   /**
    * This method does a few things. It first searches the descriptor with the specified
    * <code>path</code>. When a matching element is found, which is presumably either a
    * {@link org.rhq.core.clientapi.descriptor.plugin.ServerDescriptor} or a
    * {@link org.rhq.core.clientapi.descriptor.plugin.ServerDescriptor}, its plugin
    * configuration is retrieved. It then gets parsed and the resulting
    * {@link ConfigurationDefinition} is returned.
    *
    * @param descriptor The plugin descriptor
    * @param path The XPath-ish expression that specifies the path to the element from which
    * the plugin configuration should be loaded
    * @param configName A name to give the configuration definition
    * @return The parsed {@link ConfigurationDefinition}
    */
   public static ConfigurationDefinition loadPluginConfigDefFor(PluginDescriptor descriptor, String path,
                                                                String configName) {
      try {
         ResourceDescriptor resourceDescriptor = (ResourceDescriptor) find(descriptor, path);
         return ConfigurationMetadataParser.parse(configName, resourceDescriptor.getPluginConfiguration());
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   /**
    * This method does a few things. It first searches the descriptor with the specified
    * <code>path</code>. When a matching element is found, which is presumably either a
    * {@link org.rhq.core.clientapi.descriptor.plugin.ServerDescriptor} or a
    * {@link org.rhq.core.clientapi.descriptor.plugin.ServerDescriptor}, its resource
    * configuration is retrieved. It then gets parsed and the resulting
    * {@link ConfigurationDefinition} is returned.
    *
    * @param descriptor The plugin descriptor
    * @param path The XPath-ish expression that specifies the path to the element from which
    * the resource configuration should be loaded
    * @param configName A name to give the configuration definition
    * @return The parsed {@link ConfigurationDefinition}
    */
   public static ConfigurationDefinition loadResourceConfigDefFor(PluginDescriptor descriptor, String path,
                                                                  String configName) {
      try {
         ResourceDescriptor resourceDescriptor = (ResourceDescriptor) find(descriptor, path);
         return ConfigurationMetadataParser.parse(configName, resourceDescriptor.getResourceConfiguration());
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

}
