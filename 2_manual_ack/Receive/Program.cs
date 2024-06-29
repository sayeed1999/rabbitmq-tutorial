using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var factory = new ConnectionFactory { HostName = "localhost" };
using var connection = factory.CreateConnection();
using var channel = connection.CreateModel();

channel.QueueDeclare(queue: "hello",
// durable: true => the queue will survive a RabbitMQ node restart!
// The messages won't be lost. :)
                     durable: true,
                     exclusive: false,
                     autoDelete: false,
                     arguments: null);

// If two consumers are running, and all the even tasks take 1s and all the old tasks take 10s, round robin becomes a problem.
// Fair Dispatch: prefetchCount: 1 says don't give more than 1 task to a queue at a time,
// It means if consumer C1 is busy, rabbitmq looks for consumer C2, C3, in case if consumers are horizonally scaled (multiple instances deployed).
channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

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