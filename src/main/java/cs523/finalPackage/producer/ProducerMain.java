package cs523.finalPackage.producer;

import java.util.concurrent.LinkedBlockingQueue;

import cs523.finalPackage.config.Constants;
import twitter4j.Status;
import twitter4j.conf.ConfigurationBuilder;

public class ProducerMain {
	public static void main(String[] args) throws Exception {
		final LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(1000);

		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
			.setOAuthConsumerKey(Constants.OAUTH_CONSUMER_KEY)
			.setOAuthConsumerSecret(Constants.OAUTH_CONSUMER_SECRET)
			.setOAuthAccessToken(Constants.ACCESS_TOKEN)
			.setOAuthAccessTokenSecret(Constants.ACCESS_TOKEN_SECRET);

		TweetProducer producer = new TweetProducer(cb, queue);
		producer.produceIndefinitely(new String[] { "#bigdata", "#data" });
	}

}
