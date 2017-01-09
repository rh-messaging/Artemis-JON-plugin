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

import org.rhq.core.system.ProcessInfo;
import org.rhq.core.system.SystemInfoException;

/**
 * Dummy {@link ProcessInfo} to allow {@link ArtemisServerDiscoveryComponent} to use
 * JVM arguments obtained from {@link java.lang.management.RuntimeMXBean}
 * instead of actual process command line arguments
 *
 * @author dbokde
 */
public class ProcessInfoWithArgs extends ProcessInfo {

    public ProcessInfoWithArgs(long pid, String[] args) {
        super(pid);
        commandLine = args;
    }

    @Override
    public void refresh() throws SystemInfoException {
        // cache args
        final String[] args = commandLine;
        try {
            super.refresh();
        } finally {
            // restore cached args if needed
            commandLine = (commandLine.length == 0) ? args : commandLine;
        }
    }

}
