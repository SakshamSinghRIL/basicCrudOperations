const amqp = require("amqplib");
const oracledb = require("oracledb");
const dbConfig = require('./configs/dbconfig')

// RabbitMQ connection settings
const rabbitmqConfig = {
  url: "amqp://localhost",
  queue: "customer_queue",
};

amqp
  .connect(rabbitmqConfig.url)
  .then((connection) => connection.createChannel())
  .then((channel) => {
    channel.assertQueue(rabbitmqConfig.queue, { durable: true });

    channel.consume(rabbitmqConfig.queue, async (message) => {
      try {
        const customer = JSON.parse(message.content.toString());

        const connection = await oracledb.getConnection(dbConfig);

        const result = await connection.execute(
          `INSERT INTO SEFTTXTEST6.CUSTOMERSDATASELECTION (CUSTOMER_ID,FIRST_NAME,LAST_NAME,GENDER) VALUES ( :id, :firstname, :lastname, :gender)`,
          {
            id: customer.id,
            firstname: customer.firstname,
            lastname: customer.lastname,
            gender: customer.gender,
          },
          { autoCommit: true }
        );

        await connection.close();

        console.log("Customer data stored successfully:", customer);
      } catch (error) {
        console.error("Error storing customer data:", error);
      } 
    });
  })
  .catch((error) => {
    console.error("Error connecting to RabbitMQ:", error);
  });
