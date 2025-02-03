const KafkaConsumer = require('../services/consumer');
const logger = require('../services/logger');

jest.mock('../services/logger');

describe('Kafka Consumer', () => {
    let consumer;

    beforeEach(() => {
        consumer = new KafkaConsumer('test-group');
        consumer.consumer.connect = jest.fn().mockResolvedValue();
        consumer.consumer.subscribe = jest.fn().mockResolvedValue();
        consumer.consumer.run = jest.fn().mockResolvedValue();
        consumer.consumer.disconnect = jest.fn().mockResolvedValue();
    });

    test('should connect successfully', async () => {
        await consumer.connect();
        expect(consumer.consumer.connect).toHaveBeenCalled();
        expect(logger.info).toHaveBeenCalledWith('Kafka Consumer test-group connected');
    });

    test('should run consumer', async () => {
        await consumer.run();
        expect(consumer.consumer.run).toHaveBeenCalled();
    });

    test('should disconnect successfully', async () => {
        await consumer.disconnect();
        expect(consumer.consumer.disconnect).toHaveBeenCalled();
        expect(logger.info).toHaveBeenCalledWith('Kafka Consumer test-group disconnected');
    });
});
