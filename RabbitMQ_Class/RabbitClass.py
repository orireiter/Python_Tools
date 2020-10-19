import pika, uuid

class Rabbit:
      '''
            A class to represent a connection to RabbitMQ server. 
            Allowing easier use of publishing and consuming messages with the same object, giving the object modularity.
      
            Attributes
            ----------
            rabbit_parameters: pika.ConnectionParameters
                  An object containing parameters such as hostname
                  of rabbit server or credentials to connect.
                  These are defined upon creating an object and are 
                  used when sending, receiving messages.
            connection: pika.BlockingConnection object
                  Upon creation of this object with the 
                  use of the parameters above, 
                  a connection to RabbitMQ server is made.
                  Allowing to create a channel.
            channel: connection.channel
                  Upon execution, the channel can be used to 
                  declare/delete queues, publish/consume messages and more.
            channel.qos:
                  Decides how theis object will consume messages in temrs of quality.
                  IN THIS CASE it is used to tell the consumer to take 1 message at a time.
                  As opposed to taking 10 messages messages from the server at once, and 
                  still dealing with them one message at a time.
            callback_queue: str
                  The name of an auto-genereated queue, which is also 
                  declared to the server upon init, that will be used if 
                  the object executes send_n_receive.
                  Meaning - The object needs to have a private queue to get an answer from.
            channel.basic_consume:
                  Upon execution it will define the properties of the consumption
                  of the private queue (for use of send_n_receive).
                  When purely consuming without using the callback_queue, 
                  this will overridden and defined again with the relevant queue.
            msg_id: str(uuid4)
                  When using send_n_receive(), the message send is 
                  tagged with this property, as well as this object,
                  so when this object awaits an answer (which will be tagged the same)
                  it will know when the dedicated answer arrives.
            response: None/str
                  (For the use of send_n_receive) This attribute = None 
                  until an answer with the right id arrives. 
                  And then response = body of that message.
            
            Methods 
            -------
            declare_queue(queue_name: str, passive=False, durable=False, exclusive=False, auto_delete=False, arguments=None):
                  Declares a queue to the server to be used later on.
            declare_exhange(exchange_name: str, exchange_type='direct', passive=False, durable=False, auto_delete=False, internal=False, arguments=None):
                  Declares an exchange to the server to be used later on.
            close_connection:
                  Closes the connection to the server.
            check_corr_id:
                  Compares the ID given to the message sent, 
                  and the one of message answered.
                  If they're the same, self.response=answer's body.
            send_one:
                  Send one message to the server (doesn't wait for any response).
            consume_one
      '''
      def __init__(self,host='localhost', callback_queue='',credentials = None):
            if credentials == None:
                  self.rabbit_parameters = pika.ConnectionParameters(host)
            else:
                  self.rabbit_parameters = pika.ConnectionParameters(host=host,credentials=credentials)
            
            try:
                  self.connection = pika.BlockingConnection(parameters=self.rabbit_parameters)
                  self.channel = self.connection.channel()
            except pika.exceptions.AMQPConnectionError:
                  raise Exception("Couldn't initialize object.\n           RabbitMQ server could not be contacted...")

            self.channel.basic_qos(prefetch_count=1)
            
            # declaring queue for returning answers 
            queue_default_name = self.channel.queue_declare(queue=callback_queue, exclusive=True, auto_delete=True)
            self.callback_queue = queue_default_name.method.queue

            # creating basic consume basic consume for returning answers
            # it listens to the return queue, and activates the id check
            self.channel.basic_consume(
                  queue=self.callback_queue,
                  on_message_callback=self.check_corr_id,
                  auto_ack=True)
            
            

      def declare_queue(self, queue_name: str, passive=False, durable=False, exclusive=False, auto_delete=False, arguments=None):
            self.channel.queue_declare(queue=queue_name, 
                  passive=passive, 
                  durable=durable, 
                  exclusive=exclusive, 
                  auto_delete=auto_delete, 
                  arguments=arguments)
      def declare_exchange(self, exchange_name: str, exchange_type='direct', passive=False, durable=False, auto_delete=False, internal=False, arguments=None):
            self.channel.exchange_declare(exchange=exchange_name, 
                  exchange_type=exchange_type, 
                  passive=passive, 
                  durable=durable, 
                  auto_delete=auto_delete, 
                  internal=internal, 
                  arguments=arguments)
      def close_connection(self):
            self.connection.close()
      def check_corr_id(self, ch, method, properties, body):
            if self.msg_id == properties.correlation_id:
                  self.response = body

      def send_one(self, queue_name, message, **kwargs):
            #first check if queue exists
            try:
                  self.declare_queue(queue_name, True)

            except:
                  return(f"ERROR: Queue named {queue_name} not found. \n       Consider declaring it first.")


            self.channel.basic_publish(exchange='',
                      routing_key=queue_name,
                      body=message,
                      properties=pika.BasicProperties(delivery_mode=2,))

            return(f"SUCCESS: Message sent to queue named => {queue_name}")

      def send_n_receive(self, queue_name, message):
            #first check if queue exists
            try:
                  self.declare_queue(queue_name, True)
            except:
                  return(f"ERROR: Queue named {queue_name} not found. \n       Consider declaring it first.")

            self.response = None
            self.msg_id = str(uuid.uuid4())
            publish_params = pika.BasicProperties(correlation_id=self.msg_id, reply_to=self.callback_queue,)

            self.channel.basic_publish(exchange='',
            routing_key=queue_name,
            properties=publish_params,
            body=str(message))

            while self.response == None:
                  self.connection.process_data_events()
            return str(self.response)
      def receive_n_send_one(self, queue_name, func):
            #first check if queue exists
            try:
                  self.declare_queue(queue_name, True)
            except:
                  return(f"ERROR: Queue named {queue_name} not found. \n       Consider declaring it first.")   

            def callback(ch, method, properties, body):
                  try:
                        result = func(body)
                  except:
                        print(f"ERROR: Couldn't execute the function named {func.__name__} on the given message")
                        return f"ERROR: Couldn't execute the function named {func.__name__} on the given message"
                       
                  ch.basic_publish(exchange='',
                        routing_key=properties.reply_to,
                        properties=pika.BasicProperties(correlation_id = \
                                                         properties.correlation_id),
                        body=str(result))
                  ch.basic_ack(delivery_tag=method.delivery_tag)
                  ch.stop_consuming() 
                  return result
            self.channel.basic_consume(queue=queue_name, on_message_callback=callback)
            self.channel.start_consuming()
      def receive_n_send_many(self, queue_name, func):
            #first check if queue exists
            try:
                  self.declare_queue(queue_name, True)
            except:
                  return(f"ERROR: Queue named {queue_name} not found. \n       Consider declaring it first.")   

            def callback(ch, method, properties, body):
                  try:
                        result = func(body)
                  except:
                        print(f"ERROR: Couldn't execute the function named {func.__name__} on the given message")
                        return f"ERROR: Couldn't execute the function named {func.__name__} on the given message"
                       
                  ch.basic_publish(exchange='',
                        routing_key=properties.reply_to,
                        properties=pika.BasicProperties(correlation_id = \
                                                         properties.correlation_id),
                        body=str(result))
                  ch.basic_ack(delivery_tag=method.delivery_tag)
                  return result
            self.channel.basic_consume(queue=queue_name, on_message_callback=callback)
            self.channel.start_consuming()


      def consume_one(self, queue_name, func):
            #first check if queue exists
            try:
                  self.declare_queue(queue_name, True)
            except:
                  return(f"ERROR: Queue named {queue_name} not found. \n       Consider declaring it first.")   

            def callback(ch, method, properties, body):
                  try:
                        result = func(body)
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                  except:
                        print(f"ERROR: Couldn't execute the function named {func.__name__} on the given message")
                        return f"ERROR: Couldn't execute the function named {func.__name__} on the given message"
                  ch.stop_consuming()      
                  return result
                  '''
                        try: 
                              ch.basic_publish(exchange='',
                                    routing_key=properties.reply_to,
                                    properties=pika.BasicProperties(correlation_id = \
                                                                        properties.correlation_id),
                                    body=str(result))
                        except:
                              print(f"ERROR: Couldn't send the  message to routing key named {properties.reply_to} or a corr_ID error")
                              return f"ERROR: Couldn't send the  message to routing key named {properties.reply_to} or a corr_ID error"
                        
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                  '''
            self.channel.basic_consume(queue=queue_name, on_message_callback=callback)
            self.channel.start_consuming()
      def consume_many(self, queue_name, func):
            #first check if queue exists
            try:
                  self.declare_queue(queue_name, True)
            except:
                  return(f"ERROR: Queue named {queue_name} not found. \n       Consider declaring it first.")   

            def callback(ch, method, properties, body):
                  try:
                        result = func(body)
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                  except:
                        print(f"ERROR: Couldn't execute the function named {func.__name__} on the given message")
                        return f"ERROR: Couldn't execute the function named {func.__name__} on the given message"   
                  return result
                  
            self.channel.basic_consume(queue=queue_name, on_message_callback=callback)
            self.channel.start_consuming()


'''
      use of send_n_receive:

      when an object is created, it creates a returning queue.
      and is configured upon listening to consume that queue.
      
      when sending a message, it will first clean the self.response value
      as to wait for a new response on the sent message.
      the object will then assign itself a msg id.

      then it will actually send the message, alongside the id 
      and it will specify that the consumer should answer to the returning queue.

      then this function will wait start consuming until it gets 
      the right message that would fill self.response
      which can be returned to the client
      '''