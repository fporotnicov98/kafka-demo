const logger = require('../services/logger');
const KafkaProducer = require('../services/producer');

jest.mock('../services/logger');

describe('Kafka Producer', () => {
    let producer;

    beforeEach(() => {
        producer = new KafkaProducer();
        producer.producer.connect = jest.fn().mockResolvedValue();
        producer.producer.send = jest.fn().mockResolvedValue();
        producer.producer.disconnect = jest.fn().mockResolvedValue();
    });

    test('should connect successfully', async () => {
        await producer.connect();
        expect(producer.producer.connect).toHaveBeenCalled();
        expect(logger.info).toHaveBeenCalledWith('Kafka Producer connected');
    });

    test('should send order successfully', async () => {
        const order = { productName: 'Test', barCode: '123', quantity: 1, price: 10 };
        await producer.sendOrder(order);
        expect(producer.producer.send).toHaveBeenCalledWith({
            topic: 'send-order-event',
            messages: [{ key: order.barCode, value: JSON.stringify(order) }]
        });
        expect(logger.info).toHaveBeenCalledWith('Order sent', { order });
    });

    test('should disconnect successfully', async () => {
        await producer.disconnect();
        expect(producer.producer.disconnect).toHaveBeenCalled();
        expect(logger.info).toHaveBeenCalledWith('Kafka Producer disconnected');
    });
});
