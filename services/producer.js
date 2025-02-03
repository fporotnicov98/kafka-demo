const { Kafka } = require('kafkajs');
const config = require('./config');
const logger = require('./logger');

class KafkaProducer {
    constructor() {
        this.kafka = new Kafka({
            clientId: config.kafka.clientId,
            brokers: config.kafka.brokers
        });
        this.producer = this.kafka.producer();
    }

    async connect() {
        try {
            await this.producer.connect();
            logger.info('Kafka Producer connected');
        } catch (error) {
            logger.error('Producer connection failed', { error });
        }
    }

    async sendOrder(order) {
        try {
            await this.producer.send({
                topic: 'send-order-event',
                messages: [{ key: order.barCode, value: JSON.stringify(order) }]
            });
            logger.info('Order sent', { order });
        } catch (error) {
            logger.error('Failed to send order', { error });
        }
    }

    async disconnect() {
        try {
            await this.producer.disconnect();
            logger.info('Kafka Producer disconnected');
        } catch (error) {
            logger.error('Producer disconnect failed', { error });
        }
    }
}

module.exports = KafkaProducer;
