/*
ASSUMPTIONS:
-- the '\n' char can appear only between events in the events.jsonl file and not inside the event JSON
-- there is importance to the ordering of events in the local events file (assuming our users cannot have negative revenue)
-- we are fetching one event at a time from the local events file, send it to the server and wait for an OK, only then we can continue to the next event,
        the server does not receive a batch of events but rather one event at a time
-- there should be only one instance of client.js running at the same time (there is only one consumer for the local events file at events.jsonl)
        we can enforce this single consumer paradigm but it is not mentioned in the instructions and therefore out of the scope for this exercise
-- the local events file (events.jsonl) is never deleted. a better strategy would be to create more files with local events such as: events.{today's unix timestamp}.jsonl
        it will be relatively easy to adjust client.js to consume all local events files created in a specific folder (rather than just one specific file like events.jsonl)
*/

const fsPromises = require("fs/promises");
const http = require("http");

const EventManager = require("./event_manager");

const LOCAL_EVENTS_FILE_PATH = "events.jsonl";
const SERVER_LIVE_EVENT_HOST = "localhost";
const SERVER_LIVE_EVENT_PORT = 8000;
const SERVER_LIVE_EVENT_PATH = "/liveEvent";
const AUTHORIZATION_SECRET = "secret";

EventManager.init();

const processNextLocalEvent = async () => {
    const eventFile = __dirname + "/" + LOCAL_EVENTS_FILE_PATH;

    const [_, evt, newCursorFileId] = await EventManager.readNextEvent(eventFile);

    const fileCursorId = newCursorFileId;

    return Promise.resolve([evt, fileCursorId]);
};

const authorizedHttpPost = (host, port, path, dataStr) => {
    return new Promise((res, rej) => {
        const result = {
            err: null,
            statusCode: null,
            headers: null,
            body: null
        };

        try {
            const req = http.request({
                host: host,
                port: port,
                path: path,
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    "Content-Length": Buffer.byteLength(dataStr),
                    "Authorization": AUTHORIZATION_SECRET
                }
            }, (resp) => {
                resp.setEncoding("utf8");
                resp.on("data", function (data) {
                    result.body = data;
                });
                resp.on("end", () => {
                    result.statusCode = resp.statusCode;
                    result.headers = resp.headers;
                    res(result);
                });
            });
            req.on("error", (err) => {
                result.err = err;
                res(result);
            });

            req.write(dataStr);
            req.end();
        } catch (error) {
            console.error(error);
            rej();
        }
    });
}

const sendEventToServer = async (evtStr) => {
    try {
        // lets make sure our event is a valid JSON (and get rid of irrelevant whitespaces before sending to server)

        evtStr = JSON.stringify(JSON.parse(evtStr));

        // validate our event that we have the following fields: userId, name, value
        // what should we do if our event doesn't contain at least one of these fields?

        const result = await authorizedHttpPost(SERVER_LIVE_EVENT_HOST, SERVER_LIVE_EVENT_PORT, SERVER_LIVE_EVENT_PATH, evtStr);
        if (result.err !== null) {
            return Promise.resolve(false);
        }
        return Promise.resolve(result.statusCode === 200);
    } catch (error) {
        return Promise.resolve(false);
    }
};

const main = async () => {
    console.log(`client.js started at ${(new Date()).getTime()}`);
    
    const eventFile = __dirname + "/" + LOCAL_EVENTS_FILE_PATH;

    try {
        let evt, fileCursorId;
    
        do {
            console.log(`\nprocessing new local events...`);
    
            [evt, fileCursorId] = await processNextLocalEvent();
    
            if (evt !== null) {
                console.log(`found new event!\n${evt}\nsending to server...`);
    
                // if client.js dies at this line then the next time the client.js will run it will process the same event it has just found (retry the same event)

                const isSuccess = await sendEventToServer(evt);
    
                // only when the server responds with success we can advance to the next event in the local events file

                // if client.js dies at this line then we may send to the server a duplication of the same event
                // dedups can be carried out by a unique event id, for example by adding another field to the event JSON called "eventId" holding a GUID
                // and deal with the dedups on the server side

                if (isSuccess) {
                    console.log(`server received event successfully!`);
    
                    EventManager.finishProcessingEvent(eventFile, fileCursorId);
                } else {
                    console.log(`server is unresponsive, terminating!`);
    
                    break;
                }
            } else {
                console.log(`no new local events found, terminating!`);
    
                break;
            }
        } while (true);
    } catch (error) {
        console.error(error);
    }
};

main();