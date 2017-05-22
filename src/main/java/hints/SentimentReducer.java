package hints;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utilities.TweetWritable;

/**
 * The SentimentReducer processes scored tweet data and finds the highest and the lowest scoring tweets.
 * 
 * 
 * The goal of the SentimentReducer is to find the maximum and minimum scoring tweets for each topic.
 * 
 * In this first version of Sentiment processing, we are only processing one topic - tweets about the Clippers on the
 * evening of May 1, 2017
 * 
 * 
 * @author elizabeth corey
 *
 */
public class SentimentReducer extends Reducer<Text, TweetWritable, Text, TweetWritable> {

	public static int maxScore = Integer.MIN_VALUE, minScore = Integer.MAX_VALUE;
	public static TweetWritable tweet, maxTweet, minTweet;
	// private static final Log LOG = LogFactory.getLog(SentimentReducer.class);
	public static boolean testing = true;

	/**
	 * Each time reduce runs it processes the scored tweets for one key
	 * 
	 * Keys are Tweet topics for a specific time-frame (an evening in May, for instance)
	 * 
	 */
	@Override
	protected void reduce(Text key, Iterable<TweetWritable> tweetList, Context context)
			throws IOException, InterruptedException {

		// Iterate through the TweetWritables in the tweetList
		/*
		 * TODO implement - start your loop here
		 */
		// find the maximum and minimum scores and their associated tweets

		Iterator<TweetWritable> itr = tweetList.iterator();

		while (itr.hasNext()) {
			TweetWritable t = itr.next();
			int d = (int) t.getScore();
			if (d >= maxScore) {
				// LOG.info(maxScore);
				maxScore = d;
				maxTweet = new TweetWritable(t);
			} else {
				// LOG.info(d);
				// LOG.info(minScore);
				if (d < minScore) {
					minScore = d;
					minTweet = new TweetWritable(t);
				}

			}

			/*
			 * TODO implement - set maxScore and maxTweet as needed TODO implement - set minScore and minTweet as needed
			 * 
			 * GOTCHA -- make sure that when you store a new maxTweet or minTweet, you actually create a new
			 * TweetWritable object. In other words: maxTweet = new TweetWritable(tweet);
			 * 
			 * This won't work: maxTweet = tweet; ************* because Iterable lists in Hadoop MR2 are WEIRD
			 * ******************
			 */
			/*
			 * TODO implement - end your loop here
			 */
		}

		/*-
		 * Use context.write to write out 
		 * 
		 * 		1) the topic and the maxTweet 
		 * 		2) the topic and the minTweet
		 * 
		 * No, your output doesn't have to look like mine, it just has to make sense
		 */
		String topic = key.toString();

		if (testing)
			System.err.println("Printing out max:  " + maxTweet.toString());
		key.set(topic + "(maximum score): ");
		context.write(key, maxTweet);

		if (testing)
			System.err.println("Printing out min:  " + minTweet.toString());
		key.set(topic + "(minimum score): ");
		context.write(key, minTweet);
	}

}
