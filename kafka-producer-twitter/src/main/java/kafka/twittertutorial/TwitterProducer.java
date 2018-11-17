package kafka.twittertutorial;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private String consumerKey = "PkGgyJVAvDlv1bBO3XJAm4QPs";
    private String consumerSecret = "haw1JAJ6kjB6J1VMd28VH1GojeScB72fXUM3WrG4o3GHpTmflw";
    private String token = "1062839760291614725-wGZyttNTiHzfjEX7HymnXmBNaDV2gT";
    private String tokenSecret = "0r6aFZ7487sQWkDVReGTLIa3d0m6K9ZsQBGX1p1pmzftX";

    private List<String> terms = Lists.newArrayList("bitcoin", "usa", "politics", "sport", "soccer");

    private Logger _logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    public TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    private void run() {

        _logger.info("Setting up the application!");

        /* Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

        // Create a Twitter client
        Client twitterHosebirdClient = createTwitterClient(msgQueue);

        // Attempts to establish a connection.
        twitterHosebirdClient.connect();

        // Create a Kafka producer
        KafkaProducer<String, String> twitterProducer = createTwitterProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            _logger.info("Stopping application...");
            _logger.info("Shutting down client from twitter...");
            twitterHosebirdClient.stop();

            _logger.info("Closing twitter producer...");
            twitterProducer.close();

            _logger.info("Done!! Closing application...");
        }));

        // Loop to send tweets to Kafka
        while (!twitterHosebirdClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                twitterHosebirdClient.stop();
            }

            if (msg != null) {

                _logger.info(msg);

                //create a Producer Record
                ProducerRecord<String, String> record = new ProducerRecord<>("twitter_tweets", null, msg);

                // send data - asynchronous
                twitterProducer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e != null){
                            _logger.error("Something bad happened", e);
                        }

                    }
                });
            }
        }

        _logger.info("End of application!");
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {

        /* Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        // Creating the client
        ClientBuilder builder = new ClientBuilder()
                .name("TwitterProducer-Hosebird-Client")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        return hosebirdClient;
    }

    private KafkaProducer<String, String> createTwitterProducer() {

        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create a safe Producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // as we're using Kafka 2.0, we can keep this as 5. Use 1 otherwise

        // high throughput settings for the producer (compression, batch size and linger miliseconds)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));    // 32 KB batch size
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        return producer;
    }
}
