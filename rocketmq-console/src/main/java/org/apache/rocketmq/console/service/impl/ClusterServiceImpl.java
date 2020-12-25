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

import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.tools.admin.MQAdminExt;
import org.apache.rocketmq.console.service.ClusterService;
import org.apache.rocketmq.console.util.JsonUtil;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Map;
import java.util.Properties;

@Service
public class ClusterServiceImpl implements ClusterService {
    private Logger logger = LoggerFactory.getLogger(ClusterServiceImpl.class);
    @Resource
    private MQAdminExt mqAdminExt;

    /**
     * 查询集群信息（包含所有集群和关联broker节点，broker节点内部属性）
     * 同时查询集群所有broker实例内部属性
     *
     * @return
     */
    @Override
    public Map<String, Object> list() {
        try {
            //临时存储结果
            Map<String, Object> resultMap = Maps.newHashMap();

            //临时存储每个borker节点内部不同类型broker实例(0:master,1:slave)的内部属性
            Map<String/*brokerName*/, Map<Long/* brokerId */, Object/* brokerDetail */>> brokerServer = Maps.newHashMap();

            //1 查询集群信息（包含所有集群和关联broker节点，broker节点内部属性）
            ClusterInfo clusterInfo = mqAdminExt.examineBrokerClusterInfo();
            //2 遍历集群所有broker节点数据
            for (BrokerData brokerData : clusterInfo.getBrokerAddrTable().values()) {
                Map<Long, Object> brokerMasterSlaveMap = Maps.newHashMap();
                //3 遍历broker节点所有broker实例
                for (Map.Entry<Long/* brokerId */, String/* broker address */> brokerAddr : brokerData.getBrokerAddrs().entrySet()) {
                    //获取每个broker节点实例内部属性
                    KVTable kvTable = mqAdminExt.fetchBrokerRuntimeStats(brokerAddr.getValue());
                    brokerMasterSlaveMap.put(brokerAddr.getKey(), kvTable.getTable());
                }
                brokerServer.put(brokerData.getBrokerName(), brokerMasterSlaveMap);
            }
            resultMap.put("clusterInfo", clusterInfo);
            resultMap.put("brokerServer", brokerServer);
            return resultMap;
        } catch (Exception err) {
            throw Throwables.propagate(err);
        }
    }


    /**
     * 查询指定broker实例内部属性
     *
     * @param brokerAddr
     * @return
     */
    @Override
    public Properties getBrokerConfig(String brokerAddr) {
        try {
            return mqAdminExt.getBrokerConfig(brokerAddr);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
