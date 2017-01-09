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

import java.lang.reflect.Array;
import java.util.SortedSet;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.mc4j.ems.connection.bean.EmsBean;
import org.mc4j.ems.connection.bean.operation.EmsOperation;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.PropertyList;
import org.rhq.core.domain.configuration.PropertyMap;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.pluginapi.operation.OperationResult;
import org.rhq.plugins.jmx.JMXComponent;

/**
 * Adds support for browseMessages operation
 *
 * @author dbokde
 */
public class ArtemisDestinationComponent<T extends JMXComponent<?>> extends ArtemisResourceComponent<T> {

    private static final String BROWSE_OPERATION = "browse";

    @Override
    public OperationResult invokeOperation(String name, Configuration parameters, EmsBean emsBean) throws Exception {
        // intercept the browseMessages operation
        if (!BROWSE_OPERATION.equals(name)) {
            return super.invokeOperation(name, parameters, emsBean);
        }

        // null check for bean
        if (emsBean == null) {
            throw new Exception("Can not invoke operation [" + name
                + "], as we can't connect to the MBean - is it down?");
        }

        // get selector
        final String selector = parameters.getSimpleValue("selector");

        // find the operation
        final int requiredParams = selector != null && selector.trim().isEmpty() ? 1 : 0;
        final String returnType = new CompositeData[0].getClass().toString().substring(6);
        final SortedSet<EmsOperation> operations = emsBean.getOperations();
        EmsOperation browseMessagesOp = null;
        for (EmsOperation operation : operations) {
            if (BROWSE_OPERATION.equals(operation.getName()) &&
                operation.getReturnType().equals(returnType) &&
                operation.getParameters().size() == requiredParams) {
                browseMessagesOp = operation;
                break;
            }
        }

        if (browseMessagesOp == null) {
            throw new Exception("Missing operation CompositeData[] browse("
                    + (requiredParams == 1 ? "String" : "") + ")");
        }

        // invoke
        final Object opResult = requiredParams == 1 ?
                browseMessagesOp.invoke(selector) : browseMessagesOp.invoke();

        // map result to a list of mapped values
        final PropertyList messageList = new PropertyList("messageList");
        int nMsgs = Array.getLength(opResult);
        for (int i = 0; i < nMsgs; i++) {

            final PropertyMap msgMap = new PropertyMap("message");
            final CompositeData compositeData = (CompositeData) Array.get(opResult, i);

            for (String key : compositeData.getCompositeType().keySet()) {
                // get the value of the key and add it to the message map
                final Object value = compositeData.get(key);
                if (value != null && value instanceof TabularData) {
                    // skip TabularData properties like BooleanProperties
                } else if (value != null && value.getClass().isArray()) {
                    StringBuilder builder = new StringBuilder("[");
                    final int size = Array.getLength(value);
                    for (int index = 0; index < size; index++) {
                        builder.append(Array.get(value, index));
                        if (index < (size - 1)) {
                            builder.append(", ");
                        }
                    }
                    builder.append(']');
                    msgMap.put(new PropertySimple(key, builder.toString()));
                } else {
                    // convert to string and save
                    msgMap.put(new PropertySimple(key, (value != null) ? value.toString() : null));
                }
            }

            // add the message to the list
            messageList.add(msgMap);
        }

        final OperationResult result = new OperationResult();
        result.getComplexResults().put(messageList);
        return result;
    }

}
