const { Kafka } = require('kafkajs');
const config = require('./config');
const logger = require('./logger');

class KafkaConsumer {
    constructor(groupId) {
        this.kafka = new Kafka({
            clientId: config.kafka.clientId,
            brokers: config.kafka.brokers
        });

        this.consumer = this.kafka.consumer({ groupId });
    }

    async connect() {
        try {
            await this.consumer.connect();
            await this.consumer.subscribe({ topic: 'send-order-event', fromBeginning: true });
            logger.info(`Kafka Consumer ${groupId} connected`);
        } catch (error) {
            logger.error(`Consumer ${groupId} connection failed`, { error });
        }
    }

    async run() {
        try {
            await this.consumer.run({
                eachMessage: async ({ topic, partition, message }) => {
                    try {
                        const order = JSON.parse(message.value.toString());
                        logger.info(`Message received from partition ${partition}`, { order });
                    } catch (error) {
                        logger.error('Error processing message', { error });
                    }
                }
            });
        } catch (error) {
            logger.error(`Consumer ${groupId} failed to run`, { error });
        }
    }

    async disconnect() {
        try {
            await this.consumer.disconnect();
            logger.info(`Kafka Consumer ${groupId} disconnected`);
        } catch (error) {
            logger.error(`Consumer ${groupId} disconnect failed`, { error });
        }
    }
}

module.exports = KafkaConsumer;
