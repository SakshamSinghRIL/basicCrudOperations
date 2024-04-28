const express = require("express");
const bodyParser = require("body-parser");
const app = express();
const redis = require('redis');
 const client = require("./configs/redisconfig");
const oracledb = require("oracledb");
const amqp = require('amqplib');
const dbConfig = require('./configs/dbconfig'); 


const util = require('util');
const { promisify } = require('util');


app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());
app.use(express.json());


// Create a Redis client
const redisClient = redis.createClient();

// Promisify Redis methods
const redisGetAsync = promisify(redisClient.get).bind(redisClient);
const redisSetAsync = promisify(redisClient.set).bind(redisClient);

const rabbitmqConfig = {
  url: 'amqp://localhost',
  queue: 'customer_queue'
};


app.get("/", (req, res) => {
  res.send("Welcome to my Crud Operations server");
});

app.get("/allcustomers", async (req, res) => {
  async function fetchDataCustomers() {
    try {
      

      const connection = await oracledb.getConnection(dbConfig);
      const results = await connection.execute(
        "SELECT  * FROM SEFTTXTEST6.CUSTOMERSDATASELECTION"
      );

      return results.rows;
    } catch (error) {
      console.log("Db not connected", error);
    }
  }

  fetchDataCustomers()
    .then((dbRes) => {
      res.send(dbRes);
    })
    .catch((err) => {
      res.send("not recieved ", err);
    });
});

app.get("/customers", async (req, res) => {
  async function fetchDataCustomers() {
    try {

      const { id } = req.body;

      // const cachedCustomerDetails = await redisGetAsync(`customer:${id}`);
      
      // if (cachedCustomerDetails) {
      //   // If details exist in Redis, return them directly
      //   return JSON.parse(cachedCustomerDetails);
      // }else{

      const connection = await oracledb.getConnection(dbConfig);
      const results = await connection.execute(
        `SELECT  * FROM SEFTTXTEST6.CUSTOMERSDATASELECTION WHERE CUSTOMER_ID  = ${id}`
      );
      // await redisSetAsync(`customer:${id}`, JSON.stringify(results.rows));
      return results.rows;
    }
    catch (error) {
      console.log("Db not connected", error);
    }
  }

  fetchDataCustomers()
    .then((dbRes) => {
      res.send(dbRes);
    })
    .catch((err) => {
      res.send("not recieved ", err);
    });
});

/////////////////////normal post operation/////////////////////////////////////

// app.post('/customers',async (req ,res)=>{
//   try {
//       const { id, firstname, lastname, gender } = req.body;
  
//       const connection = await oracledb.getConnection(dbConfig);
  
//       const result = await connection.execute(
//         `INSERT INTO SEFTTXTEST6.CUSTOMERSDATASELECTION (CUSTOMER_ID,FIRST_NAME,LAST_NAME,GENDER) VALUES ( :id, :firstname, :lastname, :gender)`,
//         {
//           id,
//           firstname,
//           lastname,
//           gender,
//         },
//         { autoCommit: true }
//       );
  
//       res.status(201).json({ message: "Customer data stored successfully" });
//     } catch (error) {
//       console.error("Error storing customer data:", error);
//       res.status(500).json({ error: "An error occurred while storing customer data" });
//     }
// })


///////////////////// post operation with rabbitmq/////////////////////////////////////
amqp.connect(rabbitmqConfig.url)
  .then(connection => connection.createChannel())
  .then(channel => {
  
    channel.assertQueue(rabbitmqConfig.queue, { durable: true });
    
    app.post('/customers', async (req, res) => {
      try {
 
        const { id, firstname, lastname, gender } = req.body;
        const customer = { id, firstname, lastname, gender };

     
        channel.sendToQueue(rabbitmqConfig.queue, Buffer.from(JSON.stringify(customer)), { persistent: true });

      
        res.status(201).json({ message: 'Customer data stored successfully:' });
      } catch (error) {
   
        console.error('Error sending customer data to RabbitMQ:', error);
        res.status(500).json({ error: 'An error occurred while sending customer data to RabbitMQ' });
      }
    });
  })
  .catch(error => {
    console.error('Error connecting to RabbitMQ:', error);
  });

app.listen(3000, function () {
  console.log(" server is listening to post 3000");
});
