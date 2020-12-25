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

import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

/**
 * 消费分组详细信息
 */
public class GroupConsumeInfo implements Comparable<GroupConsumeInfo> {
    /**
     * 消费分组名称
     */
    private String group;
    /**
     * 消费分组客户端版本号
     */
    private String version;
    /**
     * 消费分组客户端实例数量
     */
    private int count;
    /**
     * 消费分组客户端类型（推，拉）
     */
    private ConsumeType consumeType;
    /**
     * 消费分组消费模型（广播，集群）
     */
    private MessageModel messageModel;
    /**
     * 消费分组TPS
     */
    private int consumeTps;
    /**
     * 消费分组所有消息队列未消息进度（消息堆积情况）
     */
    private long diffTotal = -1;

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public ConsumeType getConsumeType() {
        return consumeType;
    }

    public void setConsumeType(ConsumeType consumeType) {
        this.consumeType = consumeType;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public long getDiffTotal() {
        return diffTotal;
    }

    public void setDiffTotal(long diffTotal) {
        this.diffTotal = diffTotal;
    }

    @Override
    public int compareTo(GroupConsumeInfo o) {
        if (this.count != o.count) {
            return Integer.compare(o.count, this.count);
        }
        return Long.compare(o.diffTotal, this.diffTotal);
    }

    public int getConsumeTps() {
        return consumeTps;
    }

    public void setConsumeTps(int consumeTps) {
        this.consumeTps = consumeTps;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}
