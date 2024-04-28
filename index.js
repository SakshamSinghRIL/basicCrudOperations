const express = require("express");
const bodyParser = require("body-parser");
const app = express();
const client = require("./configs/redisconfig");
const oracledb = require("oracledb");

//     const cahcheValue = await client.get("todos");
// if (cahcheValue) return res.json(JSON.parse(cahcheValue));

// const { data } = await axios.get(
//   "https://jsonplaceholder.typicode.com/todos"
// );
// await client.set("todos",  JSON.stringify(data));
// //await client.expire("todos",30)
// return res.json(data);
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());
app.use(express.json());

const dbConfig = {
  user: "system",
  password: "gDf4he4Rtt36Ed",
  connectString: "localhost:1521/ORCLPDB1",
};

app.get("/", (req, res) => {
  res.send("Welcome to my Crud Operations server");
});

app.post('/customers',async (req ,res)=>{
    try {
        const { id, firstname, lastname, gender } = req.body;
    
        const connection = await oracledb.getConnection(dbConfig);
    
        const result = await connection.execute(
          `INSERT INTO SEFTTXTEST6.CUSTOMERSDATASELECTION (CUSTOMER_ID,FIRST_NAME,LAST_NAME,GENDER) VALUES ( :id, :firstname, :lastname, :gender)`,
          {
            id,
            firstname,
            lastname,
            gender,
          },
          { autoCommit: true }
        );
    
        res.status(201).json({ message: "Customer data stored successfully" });
      } catch (error) {
        console.error("Error storing customer data:", error);
        res
          .status(500)
          .json({ error: "An error occurred while storing customer data" });
      }
})

app.get("/allcustomers", async (req, res) => {
  async function fetchDataCustomers() {
    try {
      // const cahcheValue = await client.get("allcustomers");
      // if (cahcheValue){
      //   return res.json(cahcheValue);
      // }

      const connection = await oracledb.getConnection(dbConfig);
      const results = await connection.execute(
        "SELECT  * FROM SEFTTXTEST6.CUSTOMERSDATASELECTION"
      );

      // await client.set("allcustomers", results.rows);
      //  await client.expire("allcustomers",30)

      // return res.json(data);
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
      const connection = await oracledb.getConnection(dbConfig);
      const results = await connection.execute(
        `SELECT  * FROM SEFTTXTEST6.CUSTOMERSDATASELECTION WHERE CUSTOMER_ID  = ${id}`
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


app.listen(3000, function () {
  console.log(" server is listening to post 3000");
});
