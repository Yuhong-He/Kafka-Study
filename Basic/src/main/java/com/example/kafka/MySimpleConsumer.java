package com.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class MySimpleConsumer {
    private final static String TOPIC_NAME = "my-replicated-topic";
    private final static String CONSUMER_GROUP_NAME = "testGroup";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.190.134:9092,172.16.190.134:9093,172.16.190.134:9094");

        // 消费分组名
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_NAME);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // 是否自动提交offset，默认就是true
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000"); // 自动提交offset的间隔时间
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 1000); // consumer给broker发送心跳的间隔时间
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10 * 1000); //kafka如果超过10秒没有收到消费者的心跳，则会把消费者踢出消费组，进行rebalance，把分区分配给其他消费者。
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500); // 一次poll最大拉取消息的条数，可以根据消费速度的快慢来设置，默认500
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 30 * 1000); // 如果两次poll的时间如果超出了30s的时间间隔，kafka会认为其消费能力过弱，将其踢出消费组。将分区分配给其他消费者。-rebalance

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // 如果是新的消费组，默认消费新的消息，这个配置可以从头开始消费

        //1.创建一个消费者的客户端
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //2. 消费者订阅主题列表
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        consumer.assign(Collections.singletonList(new TopicPartition(TOPIC_NAME, 0))); // 指定分区消费
        consumer.seekToBeginning(Collections.singletonList(new TopicPartition(TOPIC_NAME, 0))); // 从头消费
        consumer.seek(new TopicPartition(TOPIC_NAME, 0), 10); // 指定offset消费

//        // 指定时间开始消费
//        List<PartitionInfo> topicPartitions = consumer.partitionsFor(TOPIC_NAME);
//        long fetchDataTime = new Date().getTime() - 1000 * 60 * 60;
//        Map<TopicPartition, Long> map = new HashMap<>();
//        for (PartitionInfo par : topicPartitions) {
//            map.put(new TopicPartition(TOPIC_NAME, par.partition()), fetchDataTime);
//        }
//        Map<TopicPartition, OffsetAndTimestamp> parMap = consumer.offsetsForTimes(map);
//        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : parMap.entrySet()) {
//            TopicPartition key = entry.getKey();
//            OffsetAndTimestamp value = entry.getValue();
//            if (key == null || value == null) continue;
//            long offset = value.offset();
//            System.out.println("partition-" + key.partition() + "|offset-" + offset);
//            System.out.println();
//            //根据消费里的timestamp确定offset if (value != null) {
//                consumer.assign(Collections.singletonList(key));
//                consumer.seek(key, offset);
//            }
//        }

        // 自动提交
//        while (true) {
//            /*
//             * 3.poll() API 是拉取消息的⻓轮询
//             */
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
//            for (ConsumerRecord<String, String> record : records) {
//                //4.打印消息
//                System.out.printf("收到消息:partition = %d,offset = %d, key = %s, value = %s%n",
//                        record.partition(), record.offset(), record.key(), record.value());
//            }
//        }

        // 手动同步提交
        while (true) {
            /*
             * poll() API 是拉取消息的⻓轮询
             */
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("收到消息:partition = %d,offset = %d, key = %s, value = %s%n",
                        record.partition(), record.offset(), record.key(), record.value());
            }
            //所有的消息已消费完
            if (records.count() > 0) { //有消息
                // 手动同步提交offset，当前线程会阻塞直到offset提交成功
                // 一般使用同步提交，因为提交之后一般也没有什么逻辑代码了
                consumer.commitSync();//=======阻塞=== 提交成功
            }
        }

        // 手动异步提交
//        while (true) {
//            /*
//             * poll() API 是拉取消息的⻓轮询
//             */
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
//            for (ConsumerRecord<String, String> record : records) {
//                System.out.printf("收到消息:partition = %d,offset = %d, key = %s, value = %s%n",
//                        record.partition(), record.offset(), record.key(), record.value());
//            }
//            //所有的消息已消费完
//            if (records.count() > 0) {
//                // 手动异步提交offset，当前线程提交offset不会阻塞，可以继续处理后面 的程序逻辑
//                consumer.commitAsync(new OffsetCommitCallback() {
//                    @Override
//                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
//                        if (exception != null) {
//                            System.err.println("Commit failed for " + offsets);
//                            System.err.println("Commit failed exception: " + Arrays.toString(exception.getStackTrace()));
//                        }
//                    }
//                });
//            }
//        }
    }
}
