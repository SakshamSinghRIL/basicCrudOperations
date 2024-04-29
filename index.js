const express = require("express");
const bodyParser = require("body-parser");
const app = express();
const oracledb = require("oracledb");
const amqp = require("amqplib/callback_api");
const dbConfig = require("./configs/dbconfig");
const client = require("./configs/redisconfig");

app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());
app.use(express.json());

const RABBITMQ_URL = "amqp://localhost";
const QUEUE_NAME = "customer_queue";

// const rabbitmqConfig = {
//   url: "amqp://localhost",
//   queue: "customer_queue",
// };

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

      const cahcheValue = await client.get(`customer:${id}`);

      if (cahcheValue) {
        return cahcheValue;
      }
      const connection = await oracledb.getConnection(dbConfig);
      const results = await connection.execute(
        `SELECT  * FROM SEFTTXTEST6.CUSTOMERSDATASELECTION WHERE CUSTOMER_ID  = ${id}`
      );

      client.set(`customer:${id}`, results.rows);
      return results.rows;
    } catch (error) {
      console.log("Db not connected", error);
    }
  }

  fetchDataCustomers()
    .then((dbRes) => {
      res.status(200).send(dbRes);
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

app.post("/customers", async (req, res) => {
  try {
    const { id, firstname, lastname, gender } = req.body;

    // Connect to RabbitMQ
    amqp.connect(RABBITMQ_URL, function (error0, connection) {
    
      connection.createChannel(function (error1, channel) {
      
        const queue = QUEUE_NAME;
        const msg = JSON.stringify({ id, firstname, lastname, gender });

        
        channel.assertQueue(queue, {
          durable: true,
        });
        channel.sendToQueue(queue, Buffer.from(msg), {
          persistent: true,
        });
    

      });
    });

    res.status(201).json({ message: "Customer data saved sucessfully" });
  } catch (error) {
    console.error("Error sending customer data to RabbitMQ:", error);
    res.status(500).json({error: "An error occurred while sending customer data to RabbitMQ",});
  }
});

app.listen(3000, function () {
  console.log(" server is listening to post 3000");
});
