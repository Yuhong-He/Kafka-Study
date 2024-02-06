package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class MySimpleProducer {
    private final static String TOPIC_NAME = "my-replicated-topic";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //1.设置参数
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.190.134:9092,172.16.190.134:9093,172.16.190.134:9094");
        props.put(ProducerConfig.ACKS_CONFIG, "1"); // 配置ACK，默认为1
        props.put(ProducerConfig.RETRIES_CONFIG, 3); // 发送失败重试三次
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 300); // 发送失败重试间隔（300毫秒），默认100毫秒
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432); // 设置缓冲区，大小是32MB，用来存放要发送的消息，消息会先发到缓冲区，用以提高消息发送性能
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); // kafka本地线程会去缓冲区中一次拉16k的数据，发送到broker
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10); // 如果线程拉不到16k的数据，间隔10ms也会将已拉到的数据发到broker

        // 把发送的key从字符串序列化为字节数组
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //把发送消息value从字符串序列化为字节数组
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //2.创建生产消息的客户端，传入参数
        Producer<String,String> producer = new KafkaProducer<>(props);

        //3.创建消息
        // key:作用是决定了往哪个分区上发，value:具体要发送的消息内容
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>(TOPIC_NAME,"mykeyvalue","hellokafka");
        ProducerRecord<String,String> producerRecord2 = new ProducerRecord<>(TOPIC_NAME,0,"mykeyvalue","hello world"); // 指定向分区0发消息

        //4. 同步发送消息,得到消息发送的元数据并输出
        RecordMetadata metadata = producer.send(producerRecord).get();
        System.out.println("同步方式发送消息结果:" + "topic-" + metadata.topic() + "|partition-" + metadata.partition() + "|offset-" + metadata.offset());

        //5. 异步发送消息,得到消息发送的元数据并输出
        final CountDownLatch latch = new CountDownLatch(1);
        producer.send(producerRecord2, new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    System.err.println("发送消息失败:" + Arrays.toString(exception.getStackTrace()));
                }
                if (metadata != null) { System.out.println("异步方式发送消息结果:" + "topic-" + metadata.topic() + "|partition-"
                        + metadata.partition() + "|offset-" + metadata.offset());
                }
                latch.countDown();
            }
        });
        latch.await();
        producer.close();
    }
}
