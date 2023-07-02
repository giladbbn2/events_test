const EventManager = require("./event_manager");
const EventsDal = require("./events_dal");

const main = async () => {

    EventsDal.init();

    await EventManager.initEventReader();

    let isProceed = true;
    let usersRevenue = {};
    let lastEventFile = null;

    do {
        const [eventFile, evtStr, newCursorFileId] = await EventManager.readNextEvent();
        
        if (lastEventFile !== null) {
            if (lastEventFile !== eventFile) {
                // we finished processing an event file so lets update the db

                for (let userId in usersRevenue) {
                    if (usersRevenue.hasOwnProperty(userId)) {
                        await EventsDal.updateUserRevenue(userId, usersRevenue[userId]);
                    }
                }

                usersRevenue = {};
            }
        }

        lastEventFile = eventFile;

        if (eventFile === null) {
            // no more events

            isProceed = false;

            break;
        }

        if (evtStr === null && EventManager.getLastFileCursorId(eventFile) > 0) {
            // delete the eventFile

            await EventManager.deleteEventFile(eventFile);

            continue;
        }

        // process event in db

        //console.log([eventFile, evtStr, newCursorFileId]);

        console.log(`processing event: ${evtStr}`);

        const evt = JSON.parse(evtStr);

        if (typeof evt.userId !== "undefined" && typeof evt.name !== "undefined" && typeof evt.value !== "undefined") {
            let addRevenue = 0;

            if (evt.name === "add_revenue") {
                addRevenue = evt.value;
            } else {
                addRevenue = -evt.value;
            }
            
            if (typeof usersRevenue[evt.userId] === "undefined") {
                usersRevenue[evt.userId] = 0;
            }

            // the instructions clearly state that we should process the revenue for every user before we insert into the DB
            //await EventsDal.updateUserRevenue(evt.userId, addRevenue);

            usersRevenue[evt.userId] += addRevenue;
        }

        EventManager.finishProcessingEvent(eventFile, newCursorFileId);

    } while (isProceed);

    // we finished processing an event file so lets update the db
    for (let userId in usersRevenue) {
        if (usersRevenue.hasOwnProperty(userId)) {
            await EventsDal.updateUserRevenue(userId, usersRevenue[userId]);
        }
    }
};

main();
