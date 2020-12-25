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

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.console.model.request.SendTopicMessageRequest;
import org.apache.rocketmq.console.model.request.TopicConfigInfo;
import org.apache.rocketmq.console.service.ConsumerService;
import org.apache.rocketmq.console.service.TopicService;
import org.apache.rocketmq.console.util.JsonUtil;
import com.google.common.base.Preconditions;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.annotation.Resource;

import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/topic")
public class TopicController {
    private Logger logger = LoggerFactory.getLogger(TopicController.class);

    @Resource
    private TopicService topicService;

    @Resource
    private ConsumerService consumerService;

    /**
     * 获取当前系统集群所有 TopicList
     *
     * @param skipSysProcess
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    @RequestMapping(value = "/list.query", method = RequestMethod.GET)
    @ResponseBody
    public Object list(@RequestParam(value = "skipSysProcess", required = false) String skipSysProcess)
            throws MQClientException, RemotingException, InterruptedException {
        boolean flag = false;
        if ("true".equals(skipSysProcess)) {
            flag = true;
        }
        //获取当前系统集群所有 TopicList
        return topicService.fetchAllTopicList(flag);
    }

    /**
     * 获取TopicStatsTable状态信息
     * TopicStatsTable内部包括该Topic每个MessageQueue信息，以及每个MessageQueue逻辑偏移信息
     *
     * @param topic
     * @return
     */
    @RequestMapping(value = "/stats.query", method = RequestMethod.GET)
    @ResponseBody
    public Object stats(@RequestParam String topic) {
        return topicService.stats(topic);
    }

    /**
     * 获取TopicRouteData 主题路由信息
     * TopicRouteData内部包括Topic消息队列信息（包括分配规则和数据同步方式）列表，TopicBroker节点列表
     *
     * @param topic
     * @return
     */
    @RequestMapping(value = "/route.query", method = RequestMethod.GET)
    @ResponseBody
    public Object route(@RequestParam String topic) {
        return topicService.route(topic);
    }



    /**
     * 获取指定Topic当前集群在线的消费分组列表
     *
     * @param topic
     * @return
     */
    @RequestMapping(value = "/queryTopicConsumerInfo.query")
    @ResponseBody
    public Object queryTopicConsumerInfo(@RequestParam String topic) {
        return topicService.queryTopicConsumerInfo(topic);
    }

    /**
     * 查询指定topic所有消费分组消费汇总信息
     *
     * @param topic
     * @return
     */
    @RequestMapping(value = "/queryConsumerByTopic.query")
    @ResponseBody
    public Object queryConsumerByTopic(@RequestParam String topic) {
        return consumerService.queryConsumeStatsListByTopicName(topic);
    }




    @RequestMapping(value = "/examineTopicConfig.query")
    @ResponseBody
    public Object examineTopicConfig(@RequestParam String topic,
                                     @RequestParam(required = false) String brokerName) throws RemotingException, MQClientException, InterruptedException {
        return topicService.examineTopicConfig(topic);
    }


    /**
     * 向指定clusterName/brokerName 创建topic及其配置
     *
     * @param topicCreateOrUpdateRequest
     * @return
     */
    @RequestMapping(value = "/createOrUpdate.do", method = {RequestMethod.POST})
    @ResponseBody
    public Object topicCreateOrUpdateRequest(@RequestBody TopicConfigInfo topicCreateOrUpdateRequest) {
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(topicCreateOrUpdateRequest.getBrokerNameList()) || CollectionUtils.isNotEmpty(topicCreateOrUpdateRequest.getClusterNameList()),
                "clusterName or brokerName can not be all blank");
        logger.info("op=look topicCreateOrUpdateRequest={}", JsonUtil.obj2String(topicCreateOrUpdateRequest));
        topicService.createOrUpdate(topicCreateOrUpdateRequest);
        return true;
    }

    /**
     * 向指定clusterName集群删除topic及其配置
     *
     * @param clusterName
     * @param topic
     * @return
     */
    @RequestMapping(value = "/deleteTopic.do", method = {RequestMethod.POST})
    @ResponseBody
    public Object delete(@RequestParam(required = false) String clusterName, @RequestParam String topic) {
        return topicService.deleteTopic(topic, clusterName);
    }

    /**
     * 向指定brokerName节点删除topic及其配置
     *
     * @param brokerName brokerName节点名称
     * @param topic      topic名称
     * @return
     */
    @RequestMapping(value = "/deleteTopicByBroker.do", method = {RequestMethod.POST})
    @ResponseBody
    public Object deleteTopicByBroker(@RequestParam String brokerName, @RequestParam String topic) {
        return topicService.deleteTopicInBroker(brokerName, topic);
    }

    /**
     * 发送消息
     *
     * @param sendTopicMessageRequest
     * @return
     * @throws RemotingException
     * @throws MQClientException
     * @throws InterruptedException
     */
    @RequestMapping(value = "/sendTopicMessage.do", method = {RequestMethod.POST})
    @ResponseBody
    public Object sendTopicMessage(
            @RequestBody SendTopicMessageRequest sendTopicMessageRequest) throws RemotingException, MQClientException, InterruptedException {
        return topicService.sendTopicMessageRequest(sendTopicMessageRequest);
    }

}
