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

import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.RollbackStats;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.body.GroupList;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.console.aspect.admin.annotation.MultiMQAdminCmdMethod;
import org.apache.rocketmq.console.model.ConsumerGroupRollBackStat;
import org.apache.rocketmq.console.model.GroupConsumeInfo;
import org.apache.rocketmq.console.model.QueueStatInfo;
import org.apache.rocketmq.console.model.TopicConsumerInfo;
import org.apache.rocketmq.console.model.request.ConsumerConfigInfo;
import org.apache.rocketmq.console.model.request.DeleteSubGroupRequest;
import org.apache.rocketmq.console.model.request.ResetOffsetRequest;
import org.apache.rocketmq.console.service.AbstractCommonService;
import org.apache.rocketmq.console.service.ConsumerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import static com.google.common.base.Throwables.propagate;

@Service
public class ConsumerServiceImpl extends AbstractCommonService implements ConsumerService {
    private Logger logger = LoggerFactory.getLogger(ConsumerServiceImpl.class);

    /**
     * 获取订阅组列表信息
     *
     * @return
     */
    @Override
    @MultiMQAdminCmdMethod
    public List<GroupConsumeInfo> queryGroupList() {
        //存储集群所有分组
        Set<String> consumerGroupSet = Sets.newHashSet();
        try {
            //1 查询集群信息（包含所有集群和关联broker节点，broker节点数据）
            ClusterInfo clusterInfo = mqAdminExt.examineBrokerClusterInfo();
            //2 获取当前集群所有BrokerData 节点数据
            for (BrokerData brokerData : clusterInfo.getBrokerAddrTable().values()) {
                //从该BrokerData 节点数据中选择一个borker实例查询所有消费分组信息
                SubscriptionGroupWrapper subscriptionGroupWrapper = mqAdminExt.getAllSubscriptionGroup(brokerData.selectBrokerAddr(), 3000L);
                //将所有分组添加到consumerGroupSet
                consumerGroupSet.addAll(subscriptionGroupWrapper.getSubscriptionGroupTable().keySet());
            }
        } catch (Exception err) {
            throw Throwables.propagate(err);
        }

        //查询每个消费分组信息
        List<GroupConsumeInfo> groupConsumeInfoList = Lists.newArrayList();
        for (String consumerGroup : consumerGroupSet) {
            groupConsumeInfoList.add(queryGroup(consumerGroup));
        }
        //排序
        Collections.sort(groupConsumeInfoList);
        //返回
        return groupConsumeInfoList;
    }

    /**
     * 获取指定消费组信息
     *
     * @param consumerGroup
     * @return
     */
    @Override
    @MultiMQAdminCmdMethod
    public GroupConsumeInfo queryGroup(String consumerGroup) {
        GroupConsumeInfo groupConsumeInfo = new GroupConsumeInfo();
        try {
            //查询指定groupName（消费分组）  ConsumeStats（消费分组状态信息）
            // ConsumeStats（消费分组状态信息） 内部包含了对应的所有MessageQueue(消息队列)以及对应的OffsetWrapper(消费进度)
            ConsumeStats consumeStats = null;
            try {

                consumeStats = mqAdminExt.examineConsumeStats(consumerGroup);
            } catch (Exception e) {
                logger.warn("examineConsumeStats exception, " + consumerGroup, e);
            }

            //查询指定指定groupName（消费分组） ConsumerConnection(消费组连接信息)
            ConsumerConnection consumerConnection = null;
            try {
                consumerConnection = mqAdminExt.examineConsumerConnectionInfo(consumerGroup);
            } catch (Exception e) {
                logger.warn("examineConsumerConnectionInfo exception, " + consumerGroup, e);
            }
            //设置分组名称
            groupConsumeInfo.setGroup(consumerGroup);

            //从ConsumeStats（消费分组状态信息）中获取消费分组TPS，计算当前所有消息队列未消息进度（消息堆积情况）
            if (consumeStats != null) {
                //设置消费分组TPS
                groupConsumeInfo.setConsumeTps((int) consumeStats.getConsumeTps());
                //设置消费分组所有消息队列未消息进度（消息堆积情况）
                groupConsumeInfo.setDiffTotal(consumeStats.computeTotalDiff());
            }

            //从ConsumerConnection(消费组连接信息)获取
            if (consumerConnection != null) {
                //设置消费分组客户端实例数量
                groupConsumeInfo.setCount(consumerConnection.getConnectionSet().size());
                //设置消费分组消费模型（广播，集群）
                groupConsumeInfo.setMessageModel(consumerConnection.getMessageModel());
                //设置消费分组客户端类型（推，拉）
                groupConsumeInfo.setConsumeType(consumerConnection.getConsumeType());
                //设置消费分组客户端版本号
                groupConsumeInfo.setVersion(MQVersion.getVersionDesc(consumerConnection.computeMinVersion()));
            }
        } catch (Exception e) {
            logger.warn("examineConsumeStats or examineConsumerConnectionInfo exception, "
                    + consumerGroup, e);
        }
        //返回
        return groupConsumeInfo;
    }

    /**
     * 查询指定groupName（消费分组）对应 TopicConsumerInfo列表
     * TopicConsumerInfo 内部存储指定topic指定groupName消费汇总信息
     *
     * @param groupName
     * @return
     */
    @Override
    public List<TopicConsumerInfo> queryConsumeStatsListByGroupName(String groupName) {
        return queryConsumeStatsList(null, groupName);
    }

    /**
     * 查询指定topic指定groupName（消费分组）对应 TopicConsumerInfo列表（只有一个）
     * TopicConsumerInfo 内部存储指定topic指定groupName消费汇总信息
     *
     * @param topic     指定topic
     * @param groupName 指定groupName（消费分组）
     * @return
     */
    @Override
    @MultiMQAdminCmdMethod
    public List<TopicConsumerInfo> queryConsumeStatsList(final String topic, String groupName) {
        List<TopicConsumerInfo> topicConsumerInfoList = Lists.newArrayList();

        //1 查询指定topic指定groupName（消费分组）  ConsumeStats（消费分组状态信息）
        //ConsumeStats（消费分组状态信息） 内部包含了对应的所有MessageQueue(消息队列)以及对应的OffsetWrapper(消费进度)
        ConsumeStats consumeStats = null;
        try {
            consumeStats = mqAdminExt.examineConsumeStats(groupName, topic);
        } catch (Exception e) {
            throw propagate(e);
        }
        //2 对于查询获取MessageQueue消息队列进行过滤验证是否属于当前topic，并排序
        List<MessageQueue> mqList = Lists.newArrayList(Iterables.filter(consumeStats.getOffsetTable().keySet(), new Predicate<MessageQueue>() {
            @Override
            public boolean apply(MessageQueue o) {
                return StringUtils.isBlank(topic) || o.getTopic().equals(topic);
            }
        }));
        Collections.sort(mqList);

        //临时的Topic+Consumer 消费汇总信息
        TopicConsumerInfo nowTopicConsumerInfo = null;

        //3 获取当前groupName订阅组的订阅所有Topic中内所有MessageQueue对应的到groupName订阅组客户端Id
        Map<MessageQueue, String> messageQueueClientMap = getClientConnection(groupName);

        //4 遍历所有MessageQueue消息队列，将相同topic组装成TopicConsumerInfo创建，添加到返回列表
        for (MessageQueue mq : mqList) {
            //对于不同Topic，创建一个Topic+Consumer 消费汇总信息
            if (nowTopicConsumerInfo == null || (!StringUtils.equals(mq.getTopic(), nowTopicConsumerInfo.getTopic()))) {
                nowTopicConsumerInfo = new TopicConsumerInfo(mq.getTopic());
                topicConsumerInfoList.add(nowTopicConsumerInfo);
            }
            //通过MessageQueue消息队列，及其消费进度构造QueueStatInfo
            QueueStatInfo queueStatInfo = QueueStatInfo.fromOffsetTableEntry(mq, consumeStats.getOffsetTable().get(mq));
            //从queueStatInfo获取其MessageQueue分配的客户端
            queueStatInfo.setClientInfo(messageQueueClientMap.get(mq));
            //将queueStatInfo添加到TopicConsumerInfo 并计算汇总信息
            nowTopicConsumerInfo.appendQueueStatInfo(queueStatInfo);
        }
        //返回nowTopicConsumerInfo
        return topicConsumerInfoList;
    }

    /**
     * 获取当前groupName订阅组的订阅所有Topic中内所有MessageQueue对应的到groupName订阅组客户端Id
     * <p>
     * groupName 订阅了A,B两个Topic，
     * A,B两个Topic内部分别有4个消费队列a1,a2,a3,a4,b1,b2,b3,b4
     * groupName订阅组内部有2个客户端服务 C1,C2
     * <p>
     * a1,a2,a3,a4 被C1,C2分配消费
     * <p>
     * a1-->c1
     * a2-->C1
     * a3-->C2
     * a4-->C2
     * <p>
     * b1,b2,b3,b4 被C1,C2分配消费
     * <p>
     * b1-->c1
     * b2-->C1
     * b3-->C2
     * b4-->C2
     *
     * @param groupName 消费分组
     * @return
     */
    private Map<MessageQueue, String> getClientConnection(String groupName) {
        Map<MessageQueue, String> results = Maps.newHashMap();
        try {
            //查询Consumer分组客户端信息
            ConsumerConnection consumerConnection = mqAdminExt.examineConsumerConnectionInfo(groupName);
            //遍历所有客户端信息
            for (Connection connection : consumerConnection.getConnectionSet()) {
                //获取每个客户端信息Id
                String clinetId = connection.getClientId();
                //查询Consumer分组指定客户端
                ConsumerRunningInfo consumerRunningInfo = mqAdminExt.getConsumerRunningInfo(groupName, clinetId, false);
                for (MessageQueue messageQueue : consumerRunningInfo.getMqTable().keySet()) {
//                    results.put(messageQueue, clinetId + " " + connection.getClientAddr());
                    results.put(messageQueue, clinetId);
                }
            }
        } catch (Exception err) {
            logger.error("op=getClientConnection_error", err);
        }
        return results;
    }

    /**
     * 查询指定topic所有消费分组消费汇总信息
     *
     * @param topic 指定topic
     * @return
     */
    @Override
    @MultiMQAdminCmdMethod
    public Map<String /*groupName*/, TopicConsumerInfo> queryConsumeStatsListByTopicName(String topic) {
        //临时存储每个消费分组消费汇总信息Map
        Map<String, TopicConsumerInfo> group2ConsumerInfoMap = Maps.newHashMap();
        try {
            // 获取指定Topic当前集群在线的消费分组列表
            GroupList groupList = mqAdminExt.queryTopicConsumeByWho(topic);
            // 遍历所有消费分组
            for (String group : groupList.getGroupList()) {
                List<TopicConsumerInfo> topicConsumerInfoList = null;
                try {
                    //查询指定topic指定groupName（消费分组）对应 TopicConsumerInfo列表（只有一个）
                    //TopicConsumerInfo 内部存储指定topic指定groupName消费汇总信息
                    topicConsumerInfoList = queryConsumeStatsList(topic, group);
                } catch (Exception ignore) {
                }
                //添加到临时存储每个消费分组消费汇总信息Map
                group2ConsumerInfoMap.put(group, CollectionUtils.isEmpty(topicConsumerInfoList) ? new TopicConsumerInfo(topic) : topicConsumerInfoList.get(0));
            }
            //返回
            return group2ConsumerInfoMap;
        } catch (Exception e) {
            throw propagate(e);
        }
    }

    /**
     * 重置指定消费分组的指定topic 的进度（按时间）
     * @param resetOffsetRequest
     * @return
     */
    @Override
    @MultiMQAdminCmdMethod
    public Map<String, ConsumerGroupRollBackStat> resetOffset(ResetOffsetRequest resetOffsetRequest) {
        Map<String, ConsumerGroupRollBackStat> groupRollbackStats = Maps.newHashMap();
        for (String consumerGroup : resetOffsetRequest.getConsumerGroupList()) {
            try {
                //重置指定消费分组的指定topic 的进度（按时间）
                Map<MessageQueue, Long> rollbackStatsMap =
                        mqAdminExt.resetOffsetByTimestamp(resetOffsetRequest.getTopic(), consumerGroup, resetOffsetRequest.getResetTime(), resetOffsetRequest.isForce());

                //组装groupRollbackStats
                ConsumerGroupRollBackStat consumerGroupRollBackStat = new ConsumerGroupRollBackStat(true);
                List<RollbackStats> rollbackStatsList = consumerGroupRollBackStat.getRollbackStatsList();
                for (Map.Entry<MessageQueue, Long> rollbackStatsEntty : rollbackStatsMap.entrySet()) {
                    RollbackStats rollbackStats = new RollbackStats();
                    rollbackStats.setRollbackOffset(rollbackStatsEntty.getValue());
                    rollbackStats.setQueueId(rollbackStatsEntty.getKey().getQueueId());
                    rollbackStats.setBrokerName(rollbackStatsEntty.getKey().getBrokerName());
                    rollbackStatsList.add(rollbackStats);
                }
                groupRollbackStats.put(consumerGroup, consumerGroupRollBackStat);
            } catch (MQClientException e) {
                if (ResponseCode.CONSUMER_NOT_ONLINE == e.getResponseCode()) {
                    try {
                        ConsumerGroupRollBackStat consumerGroupRollBackStat = new ConsumerGroupRollBackStat(true);
                        //使用oldAPI重置指定消费分组的指定topic 的进度（按时间）
                        List<RollbackStats> rollbackStatsList = mqAdminExt.resetOffsetByTimestampOld(consumerGroup, resetOffsetRequest.getTopic(), resetOffsetRequest.getResetTime(), true);
                        consumerGroupRollBackStat.setRollbackStatsList(rollbackStatsList);
                        groupRollbackStats.put(consumerGroup, consumerGroupRollBackStat);
                        continue;
                    } catch (Exception err) {
                        logger.error("op=resetOffset_which_not_online_error", err);
                    }
                } else {
                    logger.error("op=resetOffset_error", e);
                }
                groupRollbackStats.put(consumerGroup, new ConsumerGroupRollBackStat(false, e.getMessage()));
            } catch (Exception e) {
                logger.error("op=resetOffset_error", e);
                groupRollbackStats.put(consumerGroup, new ConsumerGroupRollBackStat(false, e.getMessage()));
            }
        }
        return groupRollbackStats;
    }


    /**
     * 向broker节点删除消费分组
     *
     * @param deleteSubGroupRequest
     * @return
     */
    @Override
    @MultiMQAdminCmdMethod
    public boolean deleteSubGroup(DeleteSubGroupRequest deleteSubGroupRequest) {
        try {
            //1 查询集群信息（包含所有集群和关联broker节点，broker节点数据）
            ClusterInfo clusterInfo = mqAdminExt.examineBrokerClusterInfo();
            //2 获取集群所有broker节点名称
            for (String brokerName : deleteSubGroupRequest.getBrokerNameList()) {
                logger.info("addr={} groupName={}", clusterInfo.getBrokerAddrTable().get(brokerName).selectBrokerAddr(), deleteSubGroupRequest.getGroupName());
                //选择broker节点一个实例（终端），删除指定消费分组
                mqAdminExt.deleteSubscriptionGroup(clusterInfo.getBrokerAddrTable().get(brokerName).selectBrokerAddr(), deleteSubGroupRequest.getGroupName());
            }
        } catch (Exception e) {
            throw propagate(e);
        }
        return true;
    }

    /**
     * 向broker更新创建消费分组及其配置信息
     *
     * @param consumerConfigInfo
     * @return
     */
    @Override
    public boolean createAndUpdateSubscriptionGroupConfig(ConsumerConfigInfo consumerConfigInfo) {
        try {
            //1 查询集群信息（包含所有集群和关联broker节点，broker节点数据）
            ClusterInfo clusterInfo = mqAdminExt.examineBrokerClusterInfo();
            //2 获取参数中指定集群，指定broker节点名称在存在broker节点名称，并遍历
            for (String brokerName : changeToBrokerNameSet(clusterInfo.getClusterAddrTable(),
                    consumerConfigInfo.getClusterNameList(), consumerConfigInfo.getBrokerNameList())) {
                //3 向broker更新消费分组的配置信息
                mqAdminExt.createAndUpdateSubscriptionGroupConfig(clusterInfo.getBrokerAddrTable().get(brokerName).selectBrokerAddr(), consumerConfigInfo.getSubscriptionGroupConfig());
            }
        } catch (Exception err) {
            throw Throwables.propagate(err);
        }
        return true;
    }

    /**
     * 查询消费分组关联broker名称集合
     *
     * @param group 消费分组
     * @return
     */
    @Override
    @MultiMQAdminCmdMethod
    public Set<String> fetchBrokerNameSetBySubscriptionGroup(String group) {
        //定义一个broker节点名称集合
        Set<String> brokerNameSet = Sets.newHashSet();
        try {
            //1 查询消费分组在所有broker节点的配置
            //这里会在某个broker节点选择一个broker实例查询其配置
            List<ConsumerConfigInfo> consumerConfigInfoList = examineSubscriptionGroupConfig(group);

            //遍历consumerConfigInfoList，将broker节点名称添加到brokerNameSet
            for (ConsumerConfigInfo consumerConfigInfo : consumerConfigInfoList) {
                brokerNameSet.addAll(consumerConfigInfo.getBrokerNameList());
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        //返回brokerNameSet
        return brokerNameSet;

    }

    /**
     * 查询指定指定groupName（消费分组） ConsumerConnection(消费组连接信息)
     * 包括:
     * <p>
     * 1 客户端连接信息（id,地址，语言，版本）
     * 2 消费topic以及topic对应的订阅配置信息
     * 3 客户端类型（推，拉）
     * 4 消费模型（广播，集群）
     * 5 消费策略
     */
    @Override
    public ConsumerConnection getConsumerConnection(String consumerGroup) {
        try {
            //查询指定指定groupName（消费分组） ConsumerConnection(消费组连接信息)
            return mqAdminExt.examineConsumerConnectionInfo(consumerGroup);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * 查询指定指定groupName（消费分组），指定实例Id ConsumerRunningInfo(消费实例信息)
     * 包括:
     * <p>
     * 1 Topic订阅配置信息列表
     * 2 当前消费实例分配的MessageQueue(消息)以及ProcessQueueInfo（处理消费队列信息）
     * 3 当前消费实例不同topic以及ConsumeStatus(消费实例消费状态（各种RT,TPS统计))
     *
     * @param consumerGroup
     * @param clientId
     * @param jstack
     * @return
     */
    @Override
    public ConsumerRunningInfo getConsumerRunningInfo(String consumerGroup, String clientId, boolean jstack) {
        try {
            //查询指定指定groupName（消费分组），指定客户端Id ConsumerRunningInfo(消费实例信息)
            return mqAdminExt.getConsumerRunningInfo(consumerGroup, clientId, jstack);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * 查询消费分组在所有broker节点的配置
     * <p>
     * 这里会在某个broker节点选择一个broker实例查询其配置
     *
     * @param group
     * @return
     */
    @Override
    @MultiMQAdminCmdMethod
    public List<ConsumerConfigInfo> examineSubscriptionGroupConfig(String group) {
        //临时存储消费分组列表
        List<ConsumerConfigInfo> consumerConfigInfoList = Lists.newArrayList();
        try {
            //1 查询集群信息（包含所有集群和关联broker节点，broker节点数据）
            ClusterInfo clusterInfo = mqAdminExt.examineBrokerClusterInfo();
            //2 获取所有节点名称
            for (String brokerName : clusterInfo.getBrokerAddrTable().keySet()) {
                // 从每个节点中选择一个broker实例
                String brokerAddress = clusterInfo.getBrokerAddrTable().get(brokerName).selectBrokerAddr();
                // 查询指定broker实例内部,指定 SubscriptionGroupConfig
                SubscriptionGroupConfig subscriptionGroupConfig = mqAdminExt.examineSubscriptionGroupConfig(brokerAddress, group);
                if (subscriptionGroupConfig == null) {
                    continue;
                }
                //组装消费分组配置添加到consumerConfigInfoList
                consumerConfigInfoList.add(new ConsumerConfigInfo(Lists.newArrayList(brokerName), subscriptionGroupConfig));
            }
        } catch (Exception e) {
            throw propagate(e);
        }
        //返回
        return consumerConfigInfoList;
    }
}
