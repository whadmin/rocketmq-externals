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

package org.apache.rocketmq.console.service.impl;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.GroupList;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.console.config.RMQConfigure;
import org.apache.rocketmq.console.model.request.SendTopicMessageRequest;
import org.apache.rocketmq.console.model.request.TopicConfigInfo;
import org.apache.rocketmq.console.service.AbstractCommonService;
import org.apache.rocketmq.console.service.TopicService;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Topic服务实现类
 */
@Service
public class TopicServiceImpl extends AbstractCommonService implements TopicService {

    @Autowired
    private RMQConfigure rMQConfigure;

    /**
     * 获取当前系统集群所有 TopicList
     *
     * @param skipSysProcess 是否不对系统Topic在返回时候添加前置%SYS%
     * @return
     */
    @Override
    public TopicList fetchAllTopicList(boolean skipSysProcess) {
        try {
            //获取系统集群所有Topic 列表
            TopicList allTopics = mqAdminExt.fetchAllTopicList();

            //不对系统Topic在返回时候添加前置%SYS%直接返回
            if (skipSysProcess) {
                return allTopics;
            }
            //获取系统Topic列表
            TopicList sysTopics = getSystemTopicList();

            //临时存储返回结果
            Set<String> topics = new HashSet<>();

            //遍历allTopics,对于系统Topic列表添加前置%SYS%添加到topics
            for (String topic : allTopics.getTopicList()) {
                if (sysTopics.getTopicList().contains(topic)) {
                    topics.add(String.format("%s%s", "%SYS%", topic));
                } else {
                    topics.add(topic);
                }
            }
            //清理allTopics
            allTopics.getTopicList().clear();
            //添加topics
            allTopics.getTopicList().addAll(topics);
            //返回
            return allTopics;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * 获取TopicStatsTable状态信息
     * TopicStatsTable内部包括该Topic每个MessageQueue信息，以及每个MessageQueue逻辑偏移信息
     *
     * @param topic 消息topic
     * @return
     */
    @Override
    public TopicStatsTable stats(String topic) {
        try {
            return mqAdminExt.examineTopicStats(topic);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * 获取TopicRouteData 主题路由信息
     * TopicRouteData内部包括Topic消息队列信息（包括分配规则和数据同步方式）列表，TopicBroker节点列表
     *
     * @param topic
     * @return
     */
    @Override
    public TopicRouteData route(String topic) {
        try {
            return mqAdminExt.examineTopicRouteInfo(topic);
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }

    /**
     * 获取指定Topic当前集群在线的消费分组列表（还在线通信的）
     *
     * @param topic
     * @return
     */
    @Override
    public GroupList queryTopicConsumerInfo(String topic) {
        try {
            return mqAdminExt.queryTopicConsumeByWho(topic);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void createOrUpdate(TopicConfigInfo topicCreateOrUpdateRequest) {
        TopicConfig topicConfig = new TopicConfig();
        BeanUtils.copyProperties(topicCreateOrUpdateRequest, topicConfig);
        try {
            ClusterInfo clusterInfo = mqAdminExt.examineBrokerClusterInfo();
            for (String brokerName : changeToBrokerNameSet(clusterInfo.getClusterAddrTable(),
                    topicCreateOrUpdateRequest.getClusterNameList(), topicCreateOrUpdateRequest.getBrokerNameList())) {
                mqAdminExt.createAndUpdateTopicConfig(clusterInfo.getBrokerAddrTable().get(brokerName).selectBrokerAddr(), topicConfig);
            }
        } catch (Exception err) {
            throw Throwables.propagate(err);
        }
    }

    @Override
    public TopicConfig examineTopicConfig(String topic, String brokerName) {
        ClusterInfo clusterInfo = null;
        try {
            clusterInfo = mqAdminExt.examineBrokerClusterInfo();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        return mqAdminExt.examineTopicConfig(clusterInfo.getBrokerAddrTable().get(brokerName).selectBrokerAddr(), topic);
    }

    @Override
    public List<TopicConfigInfo> examineTopicConfig(String topic) {
        List<TopicConfigInfo> topicConfigInfoList = Lists.newArrayList();
        TopicRouteData topicRouteData = route(topic);
        for (BrokerData brokerData : topicRouteData.getBrokerDatas()) {
            TopicConfigInfo topicConfigInfo = new TopicConfigInfo();
            TopicConfig topicConfig = examineTopicConfig(topic, brokerData.getBrokerName());
            BeanUtils.copyProperties(topicConfig, topicConfigInfo);
            topicConfigInfo.setBrokerNameList(Lists.newArrayList(brokerData.getBrokerName()));
            topicConfigInfoList.add(topicConfigInfo);
        }
        return topicConfigInfoList;
    }

    @Override
    public boolean deleteTopic(String topic, String clusterName) {
        try {
            if (StringUtils.isBlank(clusterName)) {
                return deleteTopic(topic);
            }
            Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(mqAdminExt, clusterName);
            mqAdminExt.deleteTopicInBroker(masterSet, topic);
            Set<String> nameServerSet = null;
            if (StringUtils.isNotBlank(rMQConfigure.getNamesrvAddr())) {
                String[] ns = rMQConfigure.getNamesrvAddr().split(";");
                nameServerSet = new HashSet<String>(Arrays.asList(ns));
            }
            mqAdminExt.deleteTopicInNameServer(nameServerSet, topic);
        } catch (Exception err) {
            throw Throwables.propagate(err);
        }
        return true;
    }

    @Override
    public boolean deleteTopic(String topic) {
        ClusterInfo clusterInfo = null;
        try {
            clusterInfo = mqAdminExt.examineBrokerClusterInfo();
        } catch (Exception err) {
            throw Throwables.propagate(err);
        }
        for (String clusterName : clusterInfo.getClusterAddrTable().keySet()) {
            deleteTopic(topic, clusterName);
        }
        return true;
    }

    @Override
    public boolean deleteTopicInBroker(String brokerName, String topic) {

        try {
            ClusterInfo clusterInfo = null;
            try {
                clusterInfo = mqAdminExt.examineBrokerClusterInfo();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
            mqAdminExt.deleteTopicInBroker(Sets.newHashSet(clusterInfo.getBrokerAddrTable().get(brokerName).selectBrokerAddr()), topic);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        return true;
    }

    private TopicList getSystemTopicList() {
        RPCHook rpcHook = null;
        boolean isEnableAcl = !StringUtils.isEmpty(rMQConfigure.getAccessKey()) && !StringUtils.isEmpty(rMQConfigure.getSecretKey());
        if (isEnableAcl) {
            rpcHook = new AclClientRPCHook(new SessionCredentials(rMQConfigure.getAccessKey(), rMQConfigure.getSecretKey()));
        }
        DefaultMQProducer producer = new DefaultMQProducer(MixAll.SELF_TEST_PRODUCER_GROUP, rpcHook);
        producer.setInstanceName(String.valueOf(System.currentTimeMillis()));
        producer.setNamesrvAddr(rMQConfigure.getNamesrvAddr());
        try {
            producer.start();
            return producer.getDefaultMQProducerImpl().getmQClientFactory().getMQClientAPIImpl().getSystemTopicList(20000L);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        } finally {
            producer.shutdown();
        }
    }


    @Override
    public SendResult sendTopicMessageRequest(SendTopicMessageRequest sendTopicMessageRequest) {
        DefaultMQProducer producer = null;
        if (rMQConfigure.isACLEnabled()) {
            producer = new DefaultMQProducer(new AclClientRPCHook(new SessionCredentials(
                    rMQConfigure.getAccessKey(),
                    rMQConfigure.getSecretKey()
            )));
            producer.setProducerGroup(MixAll.SELF_TEST_PRODUCER_GROUP);
        } else {
            producer = new DefaultMQProducer(MixAll.SELF_TEST_PRODUCER_GROUP);
        }

        producer.setInstanceName(String.valueOf(System.currentTimeMillis()));
        producer.setNamesrvAddr(rMQConfigure.getNamesrvAddr());
        try {
            producer.start();
            Message msg = new Message(sendTopicMessageRequest.getTopic(),
                    sendTopicMessageRequest.getTag(),
                    sendTopicMessageRequest.getKey(),
                    sendTopicMessageRequest.getMessageBody().getBytes()
            );
            return producer.send(msg);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        } finally {
            producer.shutdown();
        }
    }

}
