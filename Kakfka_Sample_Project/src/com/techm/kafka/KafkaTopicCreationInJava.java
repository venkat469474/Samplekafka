package com.techm.kafka;

import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.requests.MetadataResponse.TopicMetadata;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

public class KafkaTopicCreationInJava
{
    public static void main(String[] args) throws Exception {
        ZkClient zkClient = null;
        ZkUtils zkUtils = null;
        try {
            String zookeeperHosts = "hostip:2181"; // If multiple zookeeper then -> String zookeeperHosts = "ip1:2181,ip2:2181";
            int sessionTimeOutInMs = 15 * 1000; // 15 secs
            int connectionTimeOutInMs = 10 * 1000; // 10 secs

            zkClient = new ZkClient(zookeeperHosts, sessionTimeOutInMs, connectionTimeOutInMs, ZKStringSerializer$.MODULE$);
            zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperHosts), false);

            String topicName = "testTopic_srinivas";
            int noOfPartitions = 2;
            int noOfReplication = 2;
            Properties topicConfiguration = new Properties();

//            AdminUtils.createTopic(zkUtils, topicName, noOfPartitions, noOfReplication, topicConfiguration, RackAwareMode.Disabled$.MODULE$);
            TopicMetadata metadata=AdminUtils.fetchTopicMetadataFromZk(topicName, zkUtils);
//            System.out.println("Topic Exists: "+AdminUtils.topicExists(zkUtils, topicName));
            System.out.println("Partition count:"+metadata.partitionMetadata().get(1).partition());
//            System.out.println("Kafka Topic Created"); 
 
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }
}
