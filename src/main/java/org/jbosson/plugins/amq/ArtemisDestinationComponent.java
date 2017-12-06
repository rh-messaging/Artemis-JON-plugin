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

import static org.jbosson.plugins.amq.InvocationUtil.fillInMapArray;
import static org.jbosson.plugins.amq.InvocationUtil.fillInMapMapArray;
import static org.jbosson.plugins.amq.InvocationUtil.findOperation;

/**
 * Adds support for browseMessages operation
 *
 * @author dbokde
 */
public class ArtemisDestinationComponent<T extends JMXComponent<?>> extends ArtemisResourceComponent<T> {

    private static final String BROWSE_OPERATION = "browse";
    private static final String LIST_MESSAGES_AS_JSON_OPERATION = "listMessagesAsJSON";
    private static final String LIST_SCHEDULED_MESSAGES_AS_JSON_OPERATION = "listScheduledMessagesAsJSON";
    private static final String LIST_SCHEDULED_MESSAGES_OPERATION = "listScheduledMessages";
    private static final String LIST_MESSAGES_OPERATION = "listMessages";
    private static final String LIST_DELIVERING_MESSAGES_AS_JSON_OPERATION = "listDeliveringMessagesAsJSON";
    private static final String LIST_DELIVERING_MESSAGES_OPERATION = "listDeliveringMessages";
    private static final String COUNT_MESSAGES_OPERATION = "countMessages";
    private static final String REMOVE_MESSAGE_OPERATION = "removeMessage";
    private static final String REMOVE_MESSAGES_OPERATION = "removeMessages";
    private static final String EXPIRE_MESSAGES_OPERATION = "expireMessages";
    private static final String EXPIRE_MESSAGE_OPERATION = "expireMessage";
    private static final String RETRY_MESSAGE_OPERATION = "retryMessage";
    private static final String MOVE_MESSAGES_OPERATION = "moveMessages";
    private static final String MOVE_MESSAGE_OPERATION = "moveMessage";
    private static final String SEND_MESSAGE_TO_DLA_OPERATION = "sendMessageToDeadLetterAddress";
    private static final String SEND_MESSAGES_TO_DLA_OPERATION = "sendMessagesToDeadLetterAddress";
    private static final String CHANGE_MESSAGE_PRIORITY_OPERATION = "changeMessagePriority";
    private static final String CHANGE_MESSAGES_PRIORITY_OPERATION = "changeMessagesPriority";
    private static final String LIST_MESSAGE_COUNTER_OPERATION = "listMessageCounter";
    private static final String LIST_MESSAGE_COUNTER_AS_HTML_OPERATION = "listMessageCounterAsHTML";
    private static final String LIST_MESSAGE_COUNTER_HISTORY_OPERATION = "listMessageCounterHistory";
    private static final String LIST_MESSAGE_COUNTER_HISTORY_AS_HTML_OPERATION = "listMessageCounterHistoryAsHTML";
    private static final String LIST_CONSUMERS_AS_JSON_OPERATION = "listConsumersAsJSON";

    @Override
    public OperationResult invokeOperation(String name, Configuration parameters, EmsBean emsBean) throws Exception {

        if (BROWSE_OPERATION.equals(name)) {
            return handleInvokeBrowse(parameters, emsBean);
        } else if (LIST_MESSAGES_AS_JSON_OPERATION.equals(name)) {
            return handleInvokeListMessagesAsJSON(parameters, emsBean);
        } else if (LIST_SCHEDULED_MESSAGES_AS_JSON_OPERATION.equals(name)) {
            return handleInvokeListScheduledMessagesAsJSON(emsBean);
        } else if (LIST_SCHEDULED_MESSAGES_OPERATION.equals(name)) {
            return handleInvokeListScheduledMessages(emsBean);
        } else if (LIST_MESSAGES_OPERATION.equals(name)) {
            return handleInvokeListMessages(parameters, emsBean);
        } else if (LIST_DELIVERING_MESSAGES_AS_JSON_OPERATION.equals(name)) {
            return handleInvokeListDeliveringMessagesAsJSON(emsBean);
        } else if (LIST_DELIVERING_MESSAGES_OPERATION.equals(name)) {
            return handleInvokeListDeliveringMessages(emsBean);
        } else if (COUNT_MESSAGES_OPERATION.equals(name)) {
            return handleInvokeCountMessages(parameters, emsBean);
        } else if (REMOVE_MESSAGE_OPERATION.equals(name)) {
            return handleInvokeRemoveMessage(parameters, emsBean);
        } else if (REMOVE_MESSAGES_OPERATION.equals(name)) {
            return handleInvokeRemoveMessages(parameters, emsBean);
        } else if (EXPIRE_MESSAGES_OPERATION.equals(name)) {
            return handleInvokeExpireMessages(parameters, emsBean);
        } else if (EXPIRE_MESSAGE_OPERATION.equals(name)) {
            return handleInvokeExpireMessage(parameters, emsBean);
        } else if (RETRY_MESSAGE_OPERATION.equals(name)) {
            return handleInvokeRetryMessage(parameters, emsBean);
        } else if (MOVE_MESSAGES_OPERATION.equals(name)) {
            return handleInvokeMoveMessages(parameters, emsBean);
        } else if (MOVE_MESSAGE_OPERATION.equals(name)) {
            return handleInvokeMoveMessage(parameters, emsBean);
        } else if (SEND_MESSAGE_TO_DLA_OPERATION.equals(name)) {
            return handleInvokeSendMessageToDLA(parameters, emsBean);
        } else if (SEND_MESSAGES_TO_DLA_OPERATION.equals(name)) {
            return handleInvokeSendMessagesToDLA(parameters, emsBean);
        } else if (CHANGE_MESSAGE_PRIORITY_OPERATION.equals(name)) {
            return handleInvokeChangeMessagePriority(parameters, emsBean);
        } else if (CHANGE_MESSAGES_PRIORITY_OPERATION.equals(name)) {
            return handleInvokeChangeMessagesPriority(parameters, emsBean);
        } else if (LIST_MESSAGE_COUNTER_OPERATION.equals(name)) {
            return handleInvokeListMessageCounterOperation(emsBean);
        } else if (LIST_MESSAGE_COUNTER_AS_HTML_OPERATION.equals(name)) {
            return handleInvokeListMessageCounterAsHTML(emsBean);
        } else if (LIST_MESSAGE_COUNTER_HISTORY_OPERATION.equals(name)) {
            return handleInvokeListMessageCounterHistory(emsBean);
        } else if (LIST_MESSAGE_COUNTER_HISTORY_AS_HTML_OPERATION.equals(name)) {
            return handleInvokeListMessageCounterHistoryAsHTML(emsBean);
        } else if (LIST_CONSUMERS_AS_JSON_OPERATION.equals(name)) {
            return handleInvokeListConsumersAsJSON(emsBean);
        } else {
            return super.invokeOperation(name, parameters, emsBean);
        }
    }

    private OperationResult handleInvokeListConsumersAsJSON(EmsBean emsBean) throws Exception {
        EmsOperation operation = findOperation(emsBean, LIST_CONSUMERS_AS_JSON_OPERATION, 0);
        final Object opResult = operation.invoke();
        final OperationResult result = new OperationResult();
        result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
        return result;
    }

    private OperationResult handleInvokeListMessageCounterHistoryAsHTML(EmsBean emsBean) throws Exception {
        EmsOperation operation = findOperation(emsBean, LIST_MESSAGE_COUNTER_HISTORY_AS_HTML_OPERATION, 0);
        final Object opResult = operation.invoke();
        final OperationResult result = new OperationResult();
        result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
        return result;
    }

    private OperationResult handleInvokeListMessageCounterHistory(EmsBean emsBean) throws Exception {
        EmsOperation operation = findOperation(emsBean, LIST_MESSAGE_COUNTER_HISTORY_OPERATION, 0);
        final Object opResult = operation.invoke();
        final OperationResult result = new OperationResult();
        result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
        return result;
    }

    private OperationResult handleInvokeListMessageCounterAsHTML(EmsBean emsBean) throws Exception {
        EmsOperation operation = findOperation(emsBean, LIST_MESSAGE_COUNTER_AS_HTML_OPERATION, 0);
        final Object opResult = operation.invoke();
        final OperationResult result = new OperationResult();
        result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
        return result;
    }

    private OperationResult handleInvokeListMessageCounterOperation(EmsBean emsBean) throws Exception {
        EmsOperation operation = findOperation(emsBean, LIST_MESSAGE_COUNTER_OPERATION, 0);
        final Object opResult = operation.invoke();
        final OperationResult result = new OperationResult();
        result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
        return result;
    }

    private OperationResult handleInvokeChangeMessagesPriority(Configuration parameters, EmsBean emsBean) throws Exception {
        EmsOperation operation = findOperation(emsBean, CHANGE_MESSAGES_PRIORITY_OPERATION, 2);
        String filter = parameters.getSimpleValue("filter");
        String priority = parameters.getSimpleValue("newPriority");
        final Object opResult = operation.invoke(filter, Integer.valueOf(priority));
        final OperationResult result = new OperationResult();
        result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
        return result;
    }

    private OperationResult handleInvokeChangeMessagePriority(Configuration parameters, EmsBean emsBean) throws Exception {
        EmsOperation operation = findOperation(emsBean, CHANGE_MESSAGE_PRIORITY_OPERATION, 2);
        String mid = parameters.getSimpleValue("messageID");
        String priority = parameters.getSimpleValue("newPriority");
        final Object opResult = operation.invoke(Long.valueOf(mid), Integer.valueOf(priority));
        final OperationResult result = new OperationResult();
        result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
        return result;
    }

    private OperationResult handleInvokeSendMessagesToDLA(Configuration parameters, EmsBean emsBean) throws Exception {
        EmsOperation operation = findOperation(emsBean, SEND_MESSAGES_TO_DLA_OPERATION, 1);
        String selector = parameters.getSimpleValue("selector");
        final Object opResult = operation.invoke(selector);
        final OperationResult result = new OperationResult();
        result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
        return result;
    }

    private OperationResult handleInvokeSendMessageToDLA(Configuration parameters, EmsBean emsBean) throws Exception {
        EmsOperation operation = findOperation(emsBean, SEND_MESSAGE_TO_DLA_OPERATION, 1);
        String mid = parameters.getSimpleValue("messageID");
        final Object opResult = operation.invoke(Long.valueOf(mid));
        final OperationResult result = new OperationResult();
        result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
        return result;
    }

    private OperationResult handleInvokeMoveMessage(Configuration parameters, EmsBean emsBean) throws Exception {
        EmsOperation operation = findOperation(emsBean, MOVE_MESSAGE_OPERATION, 3);
        String mid = parameters.getSimpleValue("messageID");
        Long midLong = Long.valueOf(mid);
        String otherQueue = parameters.getSimpleValue("otherQueueName");
        String rejectDup = parameters.getSimpleValue("rejectDuplicates");
        Boolean isRejectDup = Boolean.valueOf(rejectDup);

        final Object opResult = operation.invoke(midLong, otherQueue, isRejectDup);
        final OperationResult result = new OperationResult();
        result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
        return result;
    }

    private OperationResult handleInvokeMoveMessages(Configuration parameters, EmsBean emsBean) throws Exception {
        EmsOperation operation = findOperation(emsBean, MOVE_MESSAGES_OPERATION, 4);
        String flush = parameters.getSimpleValue("flushLimit");
        Integer isFlush = Integer.valueOf(flush);
        String selector = parameters.getSimpleValue("filter");
        String otherQueue = parameters.getSimpleValue("otherQueueName");
        String rejectDup = parameters.getSimpleValue("rejectDuplicates");
        Boolean isRejectDup = Boolean.valueOf(rejectDup);

        final Object opResult = operation.invoke(isFlush, selector, otherQueue, isRejectDup);
        final OperationResult result = new OperationResult();
        result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
        return result;
    }

    private OperationResult handleInvokeRetryMessage(Configuration parameters, EmsBean emsBean) throws Exception {
        EmsOperation operation = findOperation(emsBean, RETRY_MESSAGE_OPERATION, 1);
        String value = parameters.getSimpleValue("messageID");
        Long mid = Long.valueOf(value);
        final Object opResult = operation.invoke(mid);
        final OperationResult result = new OperationResult();
        result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
        return result;
    }

    private OperationResult handleInvokeExpireMessage(Configuration parameters, EmsBean emsBean) throws Exception {
        EmsOperation operation = findOperation(emsBean, EXPIRE_MESSAGE_OPERATION, 1);
        String mid = parameters.getSimpleValue("messageID");
        final Object opResult = operation.invoke(mid);
        final OperationResult result = new OperationResult();
        result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
        return result;
    }

    private OperationResult handleInvokeExpireMessages(Configuration parameters, EmsBean emsBean) throws Exception {
        EmsOperation operation = findOperation(emsBean, EXPIRE_MESSAGES_OPERATION, 1);
        String selector = parameters.getSimpleValue("filter");
        final Object opResult = operation.invoke(selector);
        final OperationResult result = new OperationResult();
        result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
        return result;
    }

    private OperationResult handleInvokeRemoveMessages(Configuration parameters, EmsBean emsBean) throws Exception {
        EmsOperation operation = findOperation(emsBean, REMOVE_MESSAGES_OPERATION, 2);
        String flush = parameters.getSimpleValue("flushLimit");
        Integer flushLimit = Integer.valueOf(flush);
        String selector = parameters.getSimpleValue("filter");
        final Object opResult = operation.invoke(flushLimit, selector);
        final OperationResult result = new OperationResult();
        result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
        return result;
    }

    private OperationResult handleInvokeRemoveMessage(Configuration parameters, EmsBean emsBean) throws Exception {
        EmsOperation operation = findOperation(emsBean, REMOVE_MESSAGE_OPERATION, 1);
        String mid = parameters.getSimpleValue("messageID");
        final Object opResult = operation.invoke(Long.valueOf(mid));
        final OperationResult result = new OperationResult();
        result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
        return result;
    }

    private OperationResult handleInvokeCountMessages(Configuration parameters, EmsBean emsBean) throws Exception {
        EmsOperation operation = findOperation(emsBean, COUNT_MESSAGES_OPERATION, 1);
        String selector = parameters.getSimpleValue("filter");
        final Object opResult = operation.invoke(selector);
        final OperationResult result = new OperationResult();
        result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
        return result;
    }

    private OperationResult handleInvokeListDeliveringMessages(EmsBean emsBean) throws Exception {
        EmsOperation operation = findOperation(emsBean, LIST_DELIVERING_MESSAGES_OPERATION, 0);
        final Object opResult = operation.invoke();
        final OperationResult result = new OperationResult();
        final PropertyMap messagesMap = new PropertyMap("consumers");
        fillInMapMapArray(messagesMap, opResult);
        result.getComplexResults().put(messagesMap);
        return result;
    }

    private OperationResult handleInvokeListDeliveringMessagesAsJSON(EmsBean emsBean) throws Exception {
        EmsOperation operation = findOperation(emsBean, LIST_DELIVERING_MESSAGES_AS_JSON_OPERATION, 0);
        final Object opResult = operation.invoke();
        final OperationResult result = new OperationResult();
        result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
        return result;
    }

    private OperationResult handleInvokeListMessages(Configuration parameters, EmsBean emsBean) throws Exception {
        EmsOperation operation = findOperation(emsBean, LIST_MESSAGES_OPERATION, 1);
        String selector = parameters.getSimpleValue("filter");
        final Object opResult = operation.invoke(selector);
        final OperationResult result = new OperationResult();
        final PropertyList messageList = new PropertyList("messageList");
        fillInMapArray(messageList, opResult);
        result.getComplexResults().put(messageList);
        return result;
    }

    private OperationResult handleInvokeListScheduledMessages(EmsBean emsBean) throws Exception {
        EmsOperation operation = findOperation(emsBean, LIST_SCHEDULED_MESSAGES_OPERATION, 0);
        final Object opResult = operation.invoke();
        final OperationResult result = new OperationResult();
        final PropertyList messageList = new PropertyList("messageList");
        fillInMapArray(messageList, opResult);
        result.getComplexResults().put(messageList);
        return result;
    }

    private OperationResult handleInvokeListScheduledMessagesAsJSON(EmsBean emsBean) throws Exception {
        EmsOperation operation = findOperation(emsBean, LIST_SCHEDULED_MESSAGES_AS_JSON_OPERATION, 0);
        final Object opResult = operation.invoke();
        final OperationResult result = new OperationResult();
        result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
        return result;
    }

    private OperationResult handleInvokeListMessagesAsJSON(Configuration parameters, EmsBean emsBean) throws Exception {
        EmsOperation operation = findOperation(emsBean, LIST_MESSAGES_AS_JSON_OPERATION, 1);
        String selector = parameters.getSimpleValue("filter");
        final Object opResult = operation.invoke(selector);
        final OperationResult result = new OperationResult();
        result.getComplexResults().put(new PropertySimple("result", opResult.toString()));
        return result;
    }

    private OperationResult handleInvokeBrowse(Configuration parameters, EmsBean emsBean) throws Exception {
        // get selector
        final String selector = parameters.getSimpleValue("filter");

        // find the operation
        EmsOperation browseMessagesOp = findOperation(emsBean, BROWSE_OPERATION, 1);

        // invoke
        final Object opResult = browseMessagesOp.invoke(selector);

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
