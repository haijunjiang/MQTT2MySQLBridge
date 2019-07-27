import paho.mqtt.client as mqtt
import traceback
import pymysql.cursors

broker_source = "127.0.0.1"
broker_source_port = 1883

client_source = mqtt.Client("jhj3")
client_source.username_pw_set("jhj", "jhj")

# publish 消息
def on_publish(topic, payload, qos):
  client_source.publish(topic, payload, qos)

def setup():

	"Runs the setup procedure for the client"

	print("Setting up the onMessage handler")

	client_source.on_message = on_message

	print("Connecting to source")

	client_source.connect(broker_source, broker_source_port)

	client_source.subscribe("#", qos=1)

	print("Setup finished, waiting for messages...")



try:

	setup()

	client_source.loop_forever()
  on_publish("yz1t", "Hello Python!", 1)

except Exception as e:

	traceback.print_exc()

finally:

	connection.close()
