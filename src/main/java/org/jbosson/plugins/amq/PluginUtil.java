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
import org.rhq.core.domain.configuration.Configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

public final class PluginUtil {

   private static final Log log = LogFactory.getLog(PluginUtil.class);

   public static final String SYSTEM_PROPERTIES_GROUP = "systemProperties";
   public static final String VERSION_FILE_PROPERTY = "versionFile";
   public static final String RESOURCE_KEY_PROPERTY = "resourceKey";
   public static final String HOME_PROPERTY = "homeProperty";
   public static final String LOG_FILE_PROPERTY = "logFile";
   public static final String ATTACH_NOT_SUPPORTED_EXCEPTION_CLASS_NAME = "com.sun.tools.attach.AttachNotSupportedException";
   public static final String CONFIG_FILE_NAME = "org.jboss.rh-messaging.amq.jon.cfg";

   public static void loadPluginConfiguration(Configuration pluginConfig) {

      final String homePropertyName = pluginConfig.getSimpleValue(HOME_PROPERTY);

      String homeInstance = pluginConfig.getSimpleValue(homePropertyName);

      if (homeInstance == null) {
         homeInstance = System.getProperty(homePropertyName);
      }

      String configFileName = homeInstance + File.separator + "etc" + File.separator + CONFIG_FILE_NAME;

      File cfgFile = new File(configFileName);
      Properties properties = new Properties();

      if (cfgFile.exists()) {
         FileInputStream stream = null;
         try {
            stream = new FileInputStream(cfgFile);
            properties.load(stream);
         } catch (IOException e) {
            log.warn("Failed to load properties", e);
         } finally {
            if (stream != null) {
               try {
                  stream.close();
               } catch (IOException e) {
                  // ignore
               }
            }
         }
      }
      Set<String> propNames = properties.stringPropertyNames();
      Iterator<String> iter = propNames.iterator();
      while (iter.hasNext()) {
         String key = iter.next();
         pluginConfig.setSimpleValue(key, properties.getProperty(key));
      }
   }
}
