module.exports = {
  kafka: {
      brokers: ['localhost:29092'],
      clientId: 'order-app'
  },
  postgres: {
      user: 'username',
      password: 'password',
      host: 'localhost',
      port: 15432,
      database: 'orders_database'
  }
};
