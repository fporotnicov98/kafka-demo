const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'order-consumer-log',
  brokers: ['localhost:29092']
});

const consumer = kafka.consumer({ groupId: 'order-2' });

const run = async () => {
  await consumer.connect();

  await consumer.subscribe({ topic: 'send-order-event', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const order = JSON.parse(message.value.toString());
      console.log('Received order:', order);
    },
  });
};

run().catch(console.error);
