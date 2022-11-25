from kafka import KafkaConsumer
import mysql.connector
try:
    mydb = mysql.connector.connect(host = 'localhost' , user = 'root' , password = '' , database = 'naturalnumbers')
except mysql.connector.Error as e:
    print("mysql error",e)
mycursor=mydb.cursor()
bootstrap_server = ["localhost: 9092"]

topic = "Naturalnumbers"

consumer =  KafkaConsumer(topic, bootstrap_servers = bootstrap_server)

for i in consumer:
    print(str(i.value.decode()))
    random_even=int(i.value.decode())
    if(random_even % 2)==0:
        sql="INSERT INTO `naturalno`(`evenno`) VALUES (%s)"
        data=(random_even,)
        mycursor.execute(sql,data)
        mydb.commit()
        print("even number added to db",random_even)