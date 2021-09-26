package cs523.finalPackage.producer;

import java.util.Properties;
import java.util.Queue;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import twitter4j.FilterQuery;
import twitter4j.Status;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import cs523.finalPackage.config.Constants;

public class TweetProducer {
	
	private Queue<Status> queue;
	private ConfigurationBuilder cb;
	
	public TweetProducer(ConfigurationBuilder cb, Queue<Status> queue) {
		this.cb = cb;
		this.queue = queue;
	}
	
	@SuppressWarnings("resource")
	public void produceIndefinitely(String[] hashtags) throws InterruptedException {
		TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		StatusListener listener = new TweetStatusListener(queue);	
		twitterStream.addListener(listener);
		
		// Filter keywords
		FilterQuery query = new FilterQuery().track(hashtags);
		twitterStream.filter(query);

		Properties props = Constants.getProperties();

		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		int j = 0;

		while (true) {
			Status ret = queue.poll();

			if (ret == null) {
				Thread.sleep(100);
				// i++;
			} else {
				System.out.println("Tweet:" + ret);
				String source = ret.getSource();
				
				String createdAt = ret.getCreatedAt().toString();
				String username = ret.getUser().getName();
				String screenName = ret.getUser().getScreenName();
				int followersCount = ret.getUser().getFollowersCount();
				int friendsCount = ret.getUser().getFriendsCount();
				int userFavsCount = ret.getUser().getFavouritesCount();
				String location = getLocation(ret.getUser().getLocation());
				int retweetCount = ret.getRetweetCount();
				int favsCount = ret.getFavoriteCount();
				String lang = ret.getLang();
				String suffix = source.substring((source.indexOf('>',5) + 1), source.indexOf('<',5)); 
				
				String msg = String.format("%s, %s, %s, %d, %d, %d, %s, %d, %d, %s, %s", createdAt, username, screenName, followersCount, friendsCount, userFavsCount, location, retweetCount, favsCount, lang, suffix);
				producer.send(new ProducerRecord<String, String>(Constants.KAFKA_TOPIC_NAME, Integer.toString(j++), msg));
			}
		}
	}
	
	static String getLocation(String loc){
		if (loc == null) 
			return "null";
		return loc.split(",")[0];
	}
	
}