using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var factory = new ConnectionFactory { HostName = "localhost" };
using var connection = factory.CreateConnection();
using var channel = connection.CreateModel();

channel.QueueDeclare(queue: "hello",
                     durable: false,
                     exclusive: false,
                     autoDelete: false,
                     arguments: null);

Console.WriteLine(" [*] Waiting for messages.");

var consumer = new EventingBasicConsumer(channel);
consumer.Received += (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);

    Thread.Sleep(3000);
    Console.WriteLine($" [x] Received {message}");

    // Note: we are manually acknowledging rabbitmq that message processing is done.
    // What happens here is if a consumer dies before acknowleding, rabbitmq will requeue it.
    // To test it, comment out line 29, publish and message, and turn off consumer application to see the message is requeued.
    //channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
};
channel.BasicConsume(queue: "hello",
                    //  autoAck: true,
                    // Note: Telling rabbitmq to not auto delete the message from queue on consumer received, rather wait for manual acknowledgement.
                    // So that If any exception occurs and consumer fails, to finish its task, the message wont be lost.
                     autoAck: false,
                     consumer: consumer);

Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();