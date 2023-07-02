const fs = require("fs");
const fsPromises = require("fs/promises");

const EVENTS_FOLDER_NAME = __dirname + "/server_events";
const HOUR_TOTAL_MS = 3600000; // 1000 * 60 * 60
const WRITE_MAX_TRIES = 10;
const STREAM_BUFFER_SIZE = 256;

const EventManager = (function(){
    let lastEventFileWritableStream = null;
    let lastEventFilePath = null;
    let eventFiles = [];

    const getRoundedHourUTS = () => {
        const uts = new Date().getTime();
        return uts - (uts % HOUR_TOTAL_MS);
    };

    const getCurrentEventFilePath = () => {
        return `${EVENTS_FOLDER_NAME}/events_${getRoundedHourUTS()}.jsonl`;
    };

    const getCurrentEventFileWritableStream = () => {
        const currentEventFilePath = getCurrentEventFilePath();

        if (lastEventFilePath !== null) {
            if (lastEventFilePath === currentEventFilePath) {
                return lastEventFileWritableStream;
            } else {
                // close last stream
                endCurrentEventFileWritableStream();
            }
        }

        lastEventFilePath = currentEventFilePath;

        lastEventFileWritableStream = fs.createWriteStream(currentEventFilePath, {
            flags: 'a',
            defaultEncoding: 'utf8',
            autoClose: true
        });

        return lastEventFileWritableStream;
    };

    const endCurrentEventFileWritableStream = () => {
        lastEventFileWritableStream.end();

        lastEventFilePath = null;
    };

    const writeToStream = (data) => {
        return new Promise((res, rej) => {
            let i = WRITE_MAX_TRIES;

            const writeFunc = () => {
                i--;

                if (i < 0) {
                    rej();
                }

                const writer = getCurrentEventFileWritableStream();

                if (!writer.write(data + "\n")) {
                    writer.once("drain", writeFunc)
                } else {
                    res();
                }
            };

            writeFunc();
        });
    };

    const saveEvent = async (evt) => {
        if (evt === null || evt === "") {
            return Promise.reject();
        }

        return new Promise(async (res, rej) => {
            try {
                const data = JSON.stringify(evt);
    
                console.log("writing new event to current server event file");
    
                await writeToStream(data);

                await endCurrentEventFileWritableStream();

                res();
            } catch (error) {
                console.error(error);

                rej();
            }
        });
    };

    const init = async () => {
        // create events folder

        if (!fs.existsSync(EVENTS_FOLDER_NAME)) {
            fs.mkdirSync(EVENTS_FOLDER_NAME);
        }

        EventStore.init();
    };

    const initEventWriter = async () => {
        await init();
    };

    const populateEventFiles = async () => {
        return new Promise((res, rej) => {
            fs.readdir(EVENTS_FOLDER_NAME, (err, files) => {
                if (err) {
                    rej();
                }

                let newEventFiles = [];

                for (let i = 0; i < files.length; i++) {
                    const eventFile = `${EVENTS_FOLDER_NAME}/${files[i]}`;

                    newEventFiles.push(eventFile);
                }

                newEventFiles.sort();

                eventFiles = newEventFiles;

                res();
            });
        });
    };

    const readNextEvent = async (eventFile) => {
        // returns [eventFile, evt, newCursorFileId]
        if (typeof eventFile === "undefined") {
            if (eventFiles.length === 0) {
                await populateEventFiles();
    
                console.log(eventFiles);

                if (eventFiles.length === 0) {
                    return Promise.resolve([null, null, null]);
                }
            }

            eventFile = eventFiles[0];
        }

        return new Promise(async (res, rej) => {
            let fd = null;
    
            try {
                // will throw if file doesn't exist
                fd = await fsPromises.open(eventFile, "r");
        
                let isProceed = true;
        
                let readStreamStartId = EventStore.getLastFileCursorId(eventFile);
        
                // jump to our next event
                const readStream = fd.createReadStream({encoding: "utf-8", start: readStreamStartId});
        
                if (!readStream.readable) {
                    throw new Error("local events file is not readable");
                }
        
                readStream.on("readable", async () => {
                    if (!isProceed) {
                        return Promise.resolve();
                    }
                    
                    let data;
                    let evtArr = [];    // js strings are immutable so we need to test performance of string += string vs array allocation
                    let isFoundEvtStart = false;
                    let isFoundEvtEnd = false;
                    let fileCursorId = readStreamStartId;
                    
                    do {
                        data = readStream.read(STREAM_BUFFER_SIZE);
    
                        if (data === null) {
                            // if the amount of remining bytes in stream is less than STREAM_BUFFER_SIZE we may not receive them
                            data = readStream.read();
                        }
    
                        if (data === null) {
                            isProceed = false;
                            break;
                        }
                        
                        for (let i = 0; isProceed && i < data.length; i++) {
                            //console.log(data[i]);
                            switch (data[i]) {
                                case '{':
                                    // find first occurrence of opening brackets
                                    if (!isFoundEvtStart) {
                                        isFoundEvtStart = true;
                                        evtArr.push('{');
                                    }
                                    break;
        
                                case '\r':
                                    // line separator can be \n or \r\n 
                                    // do nothing
                                    break;
        
                                case '\n':
                                    // found ending of evt
    
                                    if (!isFoundEvtStart) {
                                        continue;
                                    }
    
                                    isFoundEvtEnd = true;

                                    res([eventFile, evtArr.join(""), fileCursorId + i + 1]);

                                    isProceed = false;
                                    break;
                                    
                                default:
                                    if (isFoundEvtStart) {
                                        evtArr.push(data[i]);
                                    }
                                    break;
                            }
                        }
        
                        fileCursorId += STREAM_BUFFER_SIZE;
                    } while (isProceed);
    
                    if (fd !== null) {
                        try {
                            fd.close();
                        } catch (error) {
                            console.log(error.message);
                        }
                    }
    
                    if (!isFoundEvtEnd) {
                        if (evtArr.length > 0 && evtArr[evtArr.length - 1] === '}') {
                            res([eventFile, evtArr.join(""), fileCursorId - 1]);
                        } else {
                            res([eventFile, null]);
                        }
                    }
                });
            } catch (error) {
                console.error(error);
                rej();
            }
        });
    };

    const finishProcessingEvent = (eventFile, newCursorFileId) => {
        EventStore.setLastFileCursorId(eventFile, newCursorFileId);
    };

    const deleteEventFile = async (eventFile) => {
        await fsPromises.unlink(eventFile);

        EventStore.removeLastFileCursor(eventFile);

        eventFiles = [];

        return Promise.resolve();
    };

    const initEventReader = async () => {
        await init();

        await populateEventFiles();

        return Promise.resolve();
    };

    const EventStore = (function(){
        // remember where the last event ended inside the local events file - this will be our starting point for next time this file is run
    
        let store = {};

        const saveLastFileCursorToDisk = () => {
            fs.writeFileSync("file_cursors.json", JSON.stringify(store));
        }

        return {
            init: () => {
                if (fs.existsSync("file_cursors.json")) {
                    const storeStr = fs.readFileSync("file_cursors.json", { encoding: 'utf8', flag: 'r' });

                    store = JSON.parse(storeStr);
                }
            },
            getLastFileCursorId: (eventFile) => {
                let result;

                if (typeof store[eventFile] !== "undefined") {
                    result = store[eventFile];
                } else {
                    result = 0;
                }

                return result;
            },
            setLastFileCursorId: (eventFile, newFileCursorId) => {
                store[eventFile] = newFileCursorId;

                saveLastFileCursorToDisk();
            },
            removeLastFileCursor: (eventFile) => {
                delete store[eventFile];

                saveLastFileCursorToDisk();
            }
        };
    })();

    const getLastFileCursorId = (eventFile) => {
        return EventStore.getLastFileCursorId(eventFile);
    };

    return {
        init,
        initEventWriter,
        initEventReader,
        getLastFileCursorId,
        saveEvent,
        readNextEvent,
        finishProcessingEvent,
        deleteEventFile
    };
})();

module.exports = EventManager;