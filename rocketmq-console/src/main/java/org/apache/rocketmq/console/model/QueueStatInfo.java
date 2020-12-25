/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.rocketmq.console.model;

import org.apache.rocketmq.common.admin.OffsetWrapper;
import org.apache.rocketmq.common.message.MessageQueue;
import org.springframework.beans.BeanUtils;

/**
 * 指定topic指定Consumer分组每个消费队列消费进度信息以及分配consumer客户端
 */
public class QueueStatInfo {
    /**
     * broker节点名称
     */
    private String brokerName;

    /**
     * broker节点内部消费队列Id
     */
    private int queueId;

    /**
     * broker节点内部消费队列存储消息的逻辑偏移
     */
    private long brokerOffset;

    /**
     * broker节点内部消费队列被consumer消费的逻辑偏移
     */
    private long consumerOffset;

    /**
     * 消费该broker节点内部消费队列对应客户端（IP+端口）
     * 消费该broker节点内部消费队列都会分配给Consumer分组内部一个Consumer
     */
    private String clientInfo;

    /**
     * 最后产生消息的时间
     */
    private long lastTimestamp;

    public static QueueStatInfo fromOffsetTableEntry(MessageQueue key, OffsetWrapper value) {
        QueueStatInfo queueStatInfo = new QueueStatInfo();
        BeanUtils.copyProperties(key, queueStatInfo);
        BeanUtils.copyProperties(value, queueStatInfo);
        return queueStatInfo;
    }

    public String getClientInfo() {
        return clientInfo;
    }

    public void setClientInfo(String clientInfo) {
        this.clientInfo = clientInfo;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public long getBrokerOffset() {
        return brokerOffset;
    }

    public void setBrokerOffset(long brokerOffset) {
        this.brokerOffset = brokerOffset;
    }

    public long getConsumerOffset() {
        return consumerOffset;
    }

    public void setConsumerOffset(long consumerOffset) {
        this.consumerOffset = consumerOffset;
    }

    public long getLastTimestamp() {
        return lastTimestamp;
    }

    public void setLastTimestamp(long lastTimestamp) {
        this.lastTimestamp = lastTimestamp;
    }
}
