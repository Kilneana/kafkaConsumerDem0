import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConsumerGroupExample {
	private final ConsumerConnector consumer;
	private final String topic;
	private ExecutorService executor;
	
	public ConsumerGroupExample(String a_zookeeper, String a_groupid, String a_topic) {
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
				createConsumerConfig(a_zookeeper, a_groupid));
		this.topic = a_topic;
	}
	
	public void shutdown () {
		if (consumer != null) consumer.shutdown();
		if (executor != null) executor.shutdown();
		try {
			if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
				System.out.println("Timed out waiting for consumer threads to shut down, exiting!");
			}
		} catch (InterruptedException e) {
			System.out.println("Interrupted during shutdon, exiting!");
		}	
	}

	public void run(int a_numThreads) {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(a_numThreads));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
		
		//launch the Threads
		executor = Executors.newFixedThreadPool(a_numThreads);
		
		//create an object to consume the message
		int threadNumber = 0;
		for (final KafkaStream stream : streams) {
			executor.submit(new ConsumerTest(stream, threadNumber));
			threadNumber++;
			}
	}
	
	private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupid) {
		Properties props = new Properties();
		props.put("zookeeper.connect", a_zookeeper);
		props.put("group.id", a_groupid);
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		
		return new ConsumerConfig(props);
	}
	
	public static void main(String[] args) {
		String zooKeeper = args[0];
		String groupid = args[1];
		String topic = args[2];
		int threads = Integer.parseInt(args[3]);
		
		ConsumerGroupExample example = new ConsumerGroupExample(zooKeeper, groupid, topic);
		example.run(threads);
		
		try {
			Thread.sleep(10000);
		} catch (InterruptedException ie) {
		}
		example.shutdown();
		// TODO Auto-generated method stub
	}
}
