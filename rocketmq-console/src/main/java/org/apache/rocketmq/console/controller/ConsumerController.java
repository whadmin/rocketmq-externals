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
package org.apache.rocketmq.console.controller;

import com.google.common.base.Preconditions;

import javax.annotation.Resource;

import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.console.model.ConnectionInfo;
import org.apache.rocketmq.console.model.request.ConsumerConfigInfo;
import org.apache.rocketmq.console.model.request.DeleteSubGroupRequest;
import org.apache.rocketmq.console.model.request.ResetOffsetRequest;
import org.apache.rocketmq.console.service.ConsumerService;
import org.apache.rocketmq.console.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/consumer")
public class ConsumerController {
    private Logger logger = LoggerFactory.getLogger(ConsumerController.class);

    @Resource
    private ConsumerService consumerService;


    /**
     * 获取订阅组列表信息
     *
     * @return
     */
    @RequestMapping(value = "/groupList.query")
    @ResponseBody
    public Object list() {
        return consumerService.queryGroupList();
    }

    /**
     * 获取指定消费组信息
     *
     * @param consumerGroup 消费分组
     * @return
     */
    @RequestMapping(value = "/group.query")
    @ResponseBody
    public Object groupQuery(@RequestParam String consumerGroup) {
        return consumerService.queryGroup(consumerGroup);
    }


    /**
     * 查询消费分组消费实例信息（终端）
     *
     * @param consumerGroup 消费分组
     * @return
     */
    @RequestMapping(value = "/consumerConnection.query")
    @ResponseBody
    public Object consumerConnection(@RequestParam(required = false) String consumerGroup) {
        ConsumerConnection consumerConnection = consumerService.getConsumerConnection(consumerGroup);
        consumerConnection.setConnectionSet(ConnectionInfo.buildConnectionInfoHashSet(consumerConnection.getConnectionSet()));
        return consumerConnection;
    }

    /**
     * 查询指定指定groupName（消费分组），指定客户端Id ConsumerRunningInfo(消费实例信息)
     *
     * @param consumerGroup 消费分组
     * @param clientId
     * @param jstack
     * @return
     */
    @RequestMapping(value = "/consumerRunningInfo.query")
    @ResponseBody
    public Object getConsumerRunningInfo(@RequestParam String consumerGroup, @RequestParam String clientId,
                                         @RequestParam boolean jstack) {
        return consumerService.getConsumerRunningInfo(consumerGroup, clientId, jstack);
    }


    /**
     * 查询指定groupName（消费分组）对应 TopicConsumerInfo列表
     * TopicConsumerInfo 内部存储指定topic指定groupName消费汇总信息
     *
     * @param consumerGroup 消费分组
     * @return
     */
    @RequestMapping(value = "/queryTopicByConsumer.query")
    @ResponseBody
    public Object queryConsumerByTopic(@RequestParam String consumerGroup) {
        return consumerService.queryConsumeStatsListByGroupName(consumerGroup);
    }

    /**
     * 查询消费分组在所有broker节点的配置
     *
     * @param consumerGroup
     * @return
     */
    @RequestMapping(value = "/examineSubscriptionGroupConfig.query")
    @ResponseBody
    public Object examineSubscriptionGroupConfig(@RequestParam String consumerGroup) {
        return consumerService.examineSubscriptionGroupConfig(consumerGroup);
    }

    /**
     * 重置指定消费分组的指定topic 的进度（按时间）
     *
     * @param resetOffsetRequest
     * @return
     */
    @RequestMapping(value = "/resetOffset.do", method = {RequestMethod.POST})
    @ResponseBody
    public Object resetOffset(@RequestBody ResetOffsetRequest resetOffsetRequest) {
        logger.info("op=look resetOffsetRequest={}", JsonUtil.obj2String(resetOffsetRequest));
        return consumerService.resetOffset(resetOffsetRequest);
    }


    /**
     * 向broker删除消费分组
     *
     * @param deleteSubGroupRequest
     * @return
     */
    @RequestMapping(value = "/deleteSubGroup.do", method = {RequestMethod.POST})
    @ResponseBody
    public Object deleteSubGroup(@RequestBody DeleteSubGroupRequest deleteSubGroupRequest) {
        return consumerService.deleteSubGroup(deleteSubGroupRequest);
    }

    /**
     * 向broker创建更新消费分组及其配置信息
     *
     * @param consumerConfigInfo
     * @return
     */
    @RequestMapping(value = "/createOrUpdate.do", method = {RequestMethod.POST})
    @ResponseBody
    public Object consumerCreateOrUpdateRequest(@RequestBody ConsumerConfigInfo consumerConfigInfo) {
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(consumerConfigInfo.getBrokerNameList()) || CollectionUtils.isNotEmpty(consumerConfigInfo.getClusterNameList()),
                "clusterName or brokerName can not be all blank");
        return consumerService.createAndUpdateSubscriptionGroupConfig(consumerConfigInfo);
    }

    /**
     * 查询消费分组关联broker名称集合
     *
     * @param consumerGroup
     * @return
     */
    @RequestMapping(value = "/fetchBrokerNameList.query", method = {RequestMethod.GET})
    @ResponseBody
    public Object fetchBrokerNameList(@RequestParam String consumerGroup) {
        return consumerService.fetchBrokerNameSetBySubscriptionGroup(consumerGroup);
    }


}
