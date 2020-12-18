import java.util.Properties
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import com.google.common.collect.Lists
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.core.{Client, Constants, HttpHosts}
import com.twitter.hbc.httpclient.auth.OAuth1
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.slf4j.LoggerFactory

object TwitterProducerScala extends Runnable{
  private val logger = LoggerFactory.getLogger(this.getClass())

  private val CONSUMER_KEY = "NuryW7dhuTHRSAtGWIQaT02HB"
  private val CONSUMER_SECRET = "6divAgBKHT4POW9ygI5J1htEH71MmMTcx4VmeTjRemTSZe9Zpa"
  private val TOKEN = "1224412933372960769-uf9DNE4afuIG42Zm2BkoZP5BhAYJNe"
  private val SECRET = "8W7YSLEViU2agi87hDiKUOKi3ctQEP8h23jY1b9ylxnkx"

  private val terms = Lists.newArrayList("ukraine", "kyiv", "kiev")

  def main(args: Array[String]): Unit = {
    TwitterProducerScala.run()
  }

  def run(): Unit = {
    logger.info("Welcome to Kafka Twitter App!")
    logger.info("Setting up...")

    // Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
    val msgQueue = new LinkedBlockingQueue[String](1000)

    //create a twitter client
    val client = createTwitterClient(msgQueue)

    //establish connection
    client.connect()

    //create kafka producer
    val producer = createKafkaProducer()

    //add shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(
      () => {
        logger.info("stopping the application...")
        logger.info("shutting down client from twitter...")
        client.stop()
        logger.info("closing producer...")
        producer.close()
        logger.info("The application is closed! Have a great day!")
      }
    ))

    //loop to send tweets to kafka
    while(!client.isDone()){
      var msg: String = null

      try{
        msg = msgQueue.poll(5, TimeUnit.SECONDS)
      } catch {
        case e: InterruptedException => {
          e.printStackTrace()
          client.stop()
        }
      }

      if(msg != null){
        //logger.info(msg)
        producer.send(new ProducerRecord("twitter_tweets", null, msg))
      }
    }
    logger.info("Application is closing...")
  }

  private def createKafkaProducer(): KafkaProducer[String, String] = {
    val bootstrapServer = "127.0.0.1:9092"
    val serializerClassName = "org.apache.kafka.common.serialization.StringSerializer"

    val properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializerClassName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializerClassName)

    //create safe producer
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all")
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
    properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")

    //create a high throughput producer (at expense of a bit latency and CPU usage)
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20")
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024))

    new KafkaProducer[String, String](properties)
  }

  private def createTwitterClient(msgQueue: LinkedBlockingQueue[String]): Client = {
    /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
    val hosebirdHosts = new HttpHosts(Constants.STREAM_HOST)
    val hosebirdEndpoint = new StatusesFilterEndpoint
    hosebirdEndpoint.trackTerms(terms)

    // These secrets should be read from a config file
    val hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET)

    val builder = new ClientBuilder()
      .name("Hosebird-Client-01")
      .hosts(hosebirdHosts)
      .authentication(hosebirdAuth)
      .endpoint(hosebirdEndpoint)
      .processor(new StringDelimitedProcessor(msgQueue))

    val hosebirdClient = builder.build
    hosebirdClient
  }
}
