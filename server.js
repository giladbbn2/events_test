const EventManager = require("./event_manager");
const EventsDal = require("./events_dal");
const express = require("express");

const app = express();

const SERVER_PORT = 8000;
const AUTHORIZATION_SECRET = "secret";

// startup

EventManager.initEventWriter();

EventsDal.init();

app.use(express.json());

app.post("/liveEvent", async (req, res) => {
    // first validate the Authorization http header

    const {method, url, headers, body} = req;

    //console.log([method, url, headers, body]);

    try {
        let token = req.headers.authorization;

        if (token !== AUTHORIZATION_SECRET) {
            // monitor unautorized access (save to external log)
            console.log(`unauthorized access`);
            res.status(401).send("Unauthorized request");
            return;
        }

        // save event
        await EventManager.saveEvent(body);

        res.status(200).send("");
        //res.status(200).json();
    } catch (error) {
        // monitor server errors (save to external log)
        console.log(`server error: ${error.message}`);
        res.status(500).send("Error");
        return;
    }
});

app.get('/userEvents/:userid', async (req, res) => {
  if (typeof req.params.userid === "undefined" || req.params.userid === null || req.params.userid === "") {
    // missing userid parameter
    res.status(500).send("Error");
    return;
  }

  const revenue = await EventsDal.getUserRevenue(req.params.userid);

  // return from db info about the user
  res.json({
    userId: req.params.userid,
    revenue: revenue
  });
});

app.get('/update', async (req, res) => {
  await EventsDal.updateUserRevenue("userId1", 1000);

  res.status(200).send("");
});

app.listen(SERVER_PORT, () => {
  console.log(`server started, listening on port ${SERVER_PORT}`);
});


