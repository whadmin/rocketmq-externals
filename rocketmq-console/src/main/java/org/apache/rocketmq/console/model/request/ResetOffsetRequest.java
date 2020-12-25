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
package org.apache.rocketmq.console.model.request;

import java.util.List;

/**
 * 重置指定消费分组的指定topic 的进度（按时间）
 */
public class ResetOffsetRequest {
    /**
     * 指定消费分组列表
     */
    private List<String> consumerGroupList;
    /**
     * topic名称
     */
    private String topic;
    /**
     * 时间
     */
    private long resetTime;
    /**
     *
     */
    private boolean force;

    public List<String> getConsumerGroupList() {
        return consumerGroupList;
    }

    public void setConsumerGroupList(List<String> consumerGroupList) {
        this.consumerGroupList = consumerGroupList;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getResetTime() {
        return resetTime;
    }

    public void setResetTime(long resetTime) {
        this.resetTime = resetTime;
    }

    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }
}
