import com.google.cloud.pubsub.PubSub;
import com.google.cloud.pubsub.PubSubOptions;
import com.google.cloud.pubsub.Topic;
import com.google.cloud.pubsub.TopicInfo;
import com.google.cloud.pubsub.Subscription;
import com.google.cloud.pubsub.SubscriptionInfo;
import com.google.cloud.pubsub.Message;

object pubsubHelloWorld {
	def main(args: Array[String]) {

		// Instantiates a client with default authentication. 
		//Requires $ gcloud beta auth application-default login
		val pubsub = PubSubOptions.getDefaultInstance().getService();
	
		// The name for the new topic
		val topicName = "my-new-topic";
		val subscriptionName = "my-new-subscription";

		// Creates the new topic
		val topic = pubsub.create(TopicInfo.of(topicName));

		// Creates the new subscription. Defaults to automatic ack (?confirm this?). 
		// To change default ack policies and settings use a Subscription.Builder
		val sub = pubsub.create(SubscriptionInfo.of(topicName, subscriptionName))

		// Publish message to topic
		topic.publish(Message.of("this is a message with no attributes. to add attributes, use a message builder"))

		// Pull message from subscription. Parameter is the max number of messages to pull
		val messages = sub.pull(1) // returns an iterator
		val message = messages.next()

		message.ack //acknowledge receival (not needed it automatic ack is on (?confirm this?)
		
		println("Message received: \"" + message.getPayloadAsString() + "\"")
		

	}
}
