package cs523.finalPackage.config;

import java.util.Properties;

public class Constants {
	public static final String OAUTH_CONSUMER_KEY = "flsfOlKjS9KR4xWwM8pztn4Um"; 
	public static final String OAUTH_CONSUMER_SECRET = "eAnP1TF9CF0obix4XndikAbAYPBmTEwW358ueQeY6Mkav5NYZp"; 
	public static final String ACCESS_TOKEN = "968452682775257088-kNLEtvuGX5MYGxo7oV99IyQtW0gDQ99"; 
	public static final String ACCESS_TOKEN_SECRET = "QT8MvNaS4rDQwuM2b1gpRkEz7GEO2Vxrn4cY9p8i3ybgu"; 
	public static final String KAFKA_TOPIC_NAME = "TwitterTopicAlperen";
	
	public static final Properties getProperties() {
		Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092");
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		return props;
	}
}
