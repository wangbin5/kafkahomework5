package com.test.kafka.lesson3;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MessageSender {
	
	Producer<String,String> producer;
	public MessageSender(){
		Properties props = new Properties();
		//props.put("zookeeper.servers", "192.168.231.128:2181");
		 props.put("bootstrap.servers", "192.168.231.129:9092");
		 //props.put("zk.connect", "192.168.231.128:2181");  
		 //props.put("acks", "all");
		 props.put("retries", 1);
		 props.put("batch.size", 16384);
		 props.put("linger.ms", 1);
		 props.put("buffer.memory", 33554432);
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		 props.put(ProducerConfig.ACKS_CONFIG, "all");
		 //props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);
		 //props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"123");
		 producer = new KafkaProducer<>(props);
		 
	}
	
	public void produce(){		
		//producer.beginTransaction();
		long start = System.currentTimeMillis();
		 for(int i=0;i<5;i++){
			 String text = Integer.toString(i);
			 System.out.println("send message " + text);
			 String topics = "hello1";//
			 producer.send(new ProducerRecord<String, String>("lesson3-topic2",text,text));
		 }
		 //producer.commitTransaction();
		 producer.flush();
		 System.out.println("flush ");		 
		 producer.close();
		 System.out.println(System.currentTimeMillis()-start);
	}
	
	public static void main(String[] args){
		new MessageSender().produce();
	}
}
