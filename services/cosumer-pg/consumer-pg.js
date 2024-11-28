const { Kafka } = require('kafkajs');
const { Client } = require('pg');

const kafka = new Kafka({
  clientId: 'order-consumer-db',
  brokers: ['localhost:29092']
});

const consumer = kafka.consumer({ groupId: 'order-1' });
const dbClient = new Client({
  user: 'user',
  host: 'localhost',
  database: 'orders_database',
  password: 'password',
  port: 15432,
});

const run = async () => {
  await consumer.connect();
  await dbClient.connect();

  await consumer.subscribe({ topic: 'send-order-event', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const order = JSON.parse(message.value.toString());
      console.log('Saving order to DB', order);

      await dbClient.query(
        'INSERT INTO orders(product_name, barcode, quantity, price) VALUES($1, $2, $3, $4)',
        [order.productName, order.barCode, order.quantity, order.price]
      );
    },
  });
};

run().catch(console.error);
