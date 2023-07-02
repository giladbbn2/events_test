# Finonex Exam

## Instructions

Before running the exam please run in the root folder:

```npm install```

This will install two packages:
* express
* pg (node-postgresql driver)

Then please run the db.sql file inside your installed postgresql instance.
The credentials used for postgres:
HOST: localhost
PORT: 5432
USER: postgres
PASSWORD: qwerty123456
DB: postgres
You can change the credentials in the events_dal.js file

To run the SERVER script:
Run ```node server.js```

To run the CLIENT script:
First delete the file_cursor.json file - we use it to store the progress inside the client's local event file (events.jsonl)
Then run ```node client.js```

To run the DATA PROCESSOR script:
Run ```node data_processor.js```