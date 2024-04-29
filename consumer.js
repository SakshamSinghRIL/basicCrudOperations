
const amqp = require('amqplib/callback_api');
const oracledb = require('oracledb');
const dbConfig = require('./configs/dbconfig')

const RABBITMQ_URL = 'amqp://localhost';
const QUEUE_NAME = 'customer_queue';

amqp.connect(RABBITMQ_URL, function(connection) {
 
  connection.createChannel(function(channel) {
   
    const queue = QUEUE_NAME;
    channel.assertQueue(queue, {
      durable: true
    });
    
    channel.consume(queue, function(msg) {
      const data = JSON.parse(msg.content.toString());
      oracledb.getConnection(dbConfig)
        .then(connection => {
          return connection.execute(
            `INSERT INTO SEFTTXTEST6.CUSTOMERSDATASELECTION (CUSTOMER_ID,FIRST_NAME,LAST_NAME,GENDER) VALUES (:id, :firstname, :lastname, :gender)`,
            {
              id: data.id,
              firstname: data.firstname,
              lastname: data.lastname,
              gender: data.gender
            },
            { autoCommit: true }
          );
        })
        .then(() => {
          console.log("Customer data stored successfully");
        })
        .catch(error => {
          console.error("Error storing customer data:", error);
        });
     
    }); 
  });
});
