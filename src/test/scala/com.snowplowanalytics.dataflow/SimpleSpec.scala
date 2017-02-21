import org.specs2._
import com.snowplowanalytics.dataflow.streaming.SimpleEvent

class SimpleSpec extends Specification { def is = s2"""

	Spec to test JSON parsing
	
	Example JSON event string:
	
	'{	
  		"timestamp": "2015-06-05T12:54:43.064528",
  		"type": "Green",
  		"id": "4ec80fb1-0963-4e35-8f54-ce760499d974"
	}'

	
	The previous JSON string should
		have a `type` field equal to 'Green'					$e1
		have a timestamp equal to '2015-06-05T12:54:43.064528'	$e2
																"""

	val json_event = "{\"timestamp\": \"2015-06-05T12:54:43.064528\",\"type\": \"Green\",\"id\": \"4ec80fb1-0963-4e35-8f54-ce760499d974\"}"
	val se = SimpleEvent.fromJson(json_event)

	def e1 = se.`type` must beEqualTo("Green")
	def e2 = se.timestamp must beEqualTo("2015-06-05T12:54:43.064528") 


}
