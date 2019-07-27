import paho.mqtt.client as mqtt
import traceback
import pymysql.cursors

broker_source = "127.0.0.1"
broker_source_port = 1883

client_source = mqtt.Client("YourClientId")
client_source.username_pw_set("YourUsername", "YourPassword")

DatabaseHostName = 'YourHost'
DatabaseUserName = 'YourUser'
DatabasePassword = 'YourPassword'
DatabaseName = 'mqtt'
DatabasePort = 3306

print("Connecting to database") #连接数据库
connection = pymysql.connect(
	host = DatabaseHostName,
	user = DatabaseUserName,
	password = DatabasePassword,
	db = DatabaseName,
	charset = 'utf8mb4',
	cursorclass = pymysql.cursors.DictCursor,
	port = DatabasePort
)

def insertIntoDatabase(message): #插入数据
	"Inserts the mqtt data into the database"
	with connection.cursor() as cursor:
		print("Inserting data: " + str(message.topic) + ";" + str(message.payload)[2:][:-1] + ";" + str(message.qos))
		cursor.callproc('InsertIntoMQTTTable', [str(message.topic), str(message.payload)[2:][:-1], int(message.qos)])
		connection.commit()

def on_message(client, userdata, message): #收到数据时触发操作
	"Evaluated when a new message is received on a subscribed topic"
	print("Received message '" + str(message.payload)[2:][:-1] + "' on topic '"
		+ message.topic + "' with QoS " + str(message.qos))
	insertIntoDatabase(message)
	
def setup():
	"Runs the setup procedure for the client"
	print("Setting up the onMessage handler")
	client_source.on_message = on_message #mqtt收到数据时触发 on_message函数
	print("Connecting to source")
	client_source.connect(broker_source, broker_source_port)
	client_source.subscribe("#", qos=1)
	print("Setup finished, waiting for messages...")

try:
	setup() #建立连接
	client_source.loop_forever() #开启服务
except Exception as e:
	traceback.print_exc()
finally:
	connection.close()
