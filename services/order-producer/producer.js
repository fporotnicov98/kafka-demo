const { Kafka } = require('kafkajs');
const express = require('express');
const bodyParser = require('body-parser');

const kafka = new Kafka({
  clientId: 'order-producer',
  brokers: ['localhost:29092']
});

const producer = kafka.producer();

const app = express();
app.use(bodyParser.json());

app.post('/api/v1/orders', async (req, res) => {
  const order = req.body;
  await producer.connect();
  await producer.send({
    topic: 'send-order-event',
    messages: [
      { key: order.barCode, value: JSON.stringify(order) },
    ],
  });
  res.status(200).send('Order sent to Kafka');
});

app.listen(8083, () => {
  console.log('Order Producer is running on port 8083');
});
