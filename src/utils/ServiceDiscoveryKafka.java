package utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;


public class ServiceDiscoveryKafka 	 {

	public final static String DATANODE_SERVICE_NAME = "Datanode";
	public final static String NAMENODE_SERVICE_NAME = "Namenode";
	public final static int TIMEOUT = 2000;
	public static final String kafkaAddr = "224.10.10.10";
	public static final int kafkaPort = 6665;
	KafkaConsumer<String, String> dataNodeConsumer;
	KafkaConsumer<String, String> nameNodeConsumer;
	Producer<String, String> producer;
	private static final int SESSION_TIMEOUT = 5000;
	private static final int CONNECTION_TIMEOUT = 1000;
	private static final String ZOOKEEPER_SERVER = "localhost:2181";
	private static final int REPLICATION_FACTOR = 1;


	public void createTopic(String topic) {
		try {
			ZkClient zkClient = new ZkClient(
					ZOOKEEPER_SERVER,
					SESSION_TIMEOUT,
					CONNECTION_TIMEOUT,
					ZKStringSerializer$.MODULE$);

			ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(ZOOKEEPER_SERVER), false);


			Properties topicConfig = new Properties();

			if(!AdminUtils.topicExists(zkUtils, topic))
			AdminUtils. createTopic(zkUtils, topic, 1, REPLICATION_FACTOR, topicConfig, RackAwareMode.Disabled$.MODULE$);
			
		} catch( TopicExistsException e ) {		
			System.err.println("Topic already exists...");
		}

	}

	//Subscirbes if not subscribed
	public ArrayList<String> listen (String topic) {
		createTopic(topic);
		ArrayList<String> list = new ArrayList<String>() ;
		ConsumerRecords<String, String> records;
		KafkaConsumer consumer;
		if (topic.equals(NAMENODE_SERVICE_NAME)) 
			consumer = nameNodeConsumer;

		else 
			consumer = dataNodeConsumer;

		if (consumer == null) {
			Properties props = new Properties();

			//Localização dos servidores kafka (lista de máquinas + porto)
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

			//Configura o modo de subscrição (ver documentação em kafka.apache.org)
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			props.put(ConsumerConfig.GROUP_ID_CONFIG, "grp" + new Long(1234));

			// Classe para serializar as chaves dos eventos (string)
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

			// Classe para serializar os valores dos eventos (string)
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

			// Cria um consumidor (assinante/subscriber)
			try  {
				consumer = new KafkaConsumer<>(props);
				// Subscreve uma lista de tópicos
				consumer.subscribe(Arrays.asList(topic));
				Logger.getAnonymousLogger().log(Level.INFO, "Ligado ao Kafka; Esperando por eventos...");
				//ciclo para receber os eventos


				//pede ao servidor uma lista de eventos, até esgotar o TIMEOUT

			} catch (Exception e) {

			}

			if (topic.equals(NAMENODE_SERVICE_NAME)) 
				nameNodeConsumer = consumer;

			else 
				dataNodeConsumer = consumer ;

		}

		records = consumer. poll(TIMEOUT);
		records.forEach(r -> {
			list.add(r.value());
		});


		return list;
	}


	public void write(String topic, String url) {
		createTopic(topic);
		try  {
			if (producer == null) {

				Properties props = new Properties();

				//Localização dos servidores kafka (lista de máquinas + porto)
				props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

				// Classe para serializar as chaves dos eventos (string)
				props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

				// Classe para serializar os valores dos eventos (string)
				props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

				// Cria um novo editor (publisher), com as propriedades indicadas acima.

				Logger.getAnonymousLogger().log(Level.INFO, "Ligado ao Kafka; Dando inicio ao envio de eventos...");
				producer = new KafkaProducer<>(props);
				
			}
			//publica eventos, cujo tópico (string) toma alternadamente o valor de: topic0 e topic1.
			producer.send(new ProducerRecord<String, String>(topic, url));

		}catch(Exception e) {
			System.out.println("ERRRRRROR");
		}
	}
}