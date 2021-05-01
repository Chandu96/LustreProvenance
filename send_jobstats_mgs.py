import pika
import os
import subprocess
parameters=pika.URLParameters('amqp://admin:root123@192.168.24.6:5672/')
connection=pika.BlockingConnection(parameters)
channel=connection.channel()
channel.queue_declare(queue='hello')
cmd_mds_stats='lctl get_param mdt.*.job_stats'
channel.basic_publish(exchange='',routing_key='hello',body=os.popen(cmd_mds_stats).read())
#.rstrip("\n"))
print(" [x] Sent - mdt stats ")
connection.close()
