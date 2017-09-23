package com.roncoo.eshop.storm.spout;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * kafka消费数据的spout
 */
public class AccessLogKafkaSpout extends BaseRichSpout {

    private static final long serialVersionUID = 8698470299234327074L;
    private static final Logger LOGGER = LoggerFactory.getLogger(AccessLogKafkaSpout.class);
    private ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(1000);

    private SpoutOutputCollector collector;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        startKafkaConsumer();
    }

    public void nextTuple() {
        if (queue.size() > 0) {
            try {
                String message = queue.take();
                collector.emit(new Values(message));
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            Utils.sleep(100);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }

    public void startKafkaConsumer() {
        //注意：这些配置信息，开发的时候都是放到springboot配置文件中
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop01:9092,hadoop02:9092,hadoop03:9092,hadoop04:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "eshop-cache-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "20000");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        String topics = "access-log";
        List<String> topicList = new ArrayList<>();
        String[] kafkaTopicsSplited = topics.split(",");
        topicList.addAll(Arrays.asList(kafkaTopicsSplited));

        // 可以同时向多个topic获取数据
        consumer.subscribe(topicList);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(2000);
            for (ConsumerRecord<String, String> record : records) {
                //将从kakfa获取的数据，交给线程处理
                new Thread(new KafkaMessageProcessor(record)).start();
            }
        }
    }

    public class KafkaMessageProcessor implements Runnable {

        private ConsumerRecord<String, String> record;

        public KafkaMessageProcessor(ConsumerRecord<String, String> record) {
            this.record = record;
        }

        public void run() {
            String message = record.value();
            LOGGER.info("【AccessLogKafkaSpout中的Kafka消费者接收到一条日志】message=" + message);
            try {
                queue.put(message);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
