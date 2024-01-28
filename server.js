const config = require('./local-config.js');
const WebSocket = require('ws');
const cors = require('cors');
const fs = require('fs');

const { v4: uuidv4 } = require('uuid');

const LOGFILE = 'vbsqueryserverlog.json'

const DISTINCTIVE_L2DIST1 = 10.0;
const DISTINCTIVE_L2DIST2 = 15.0;
const CLIPSERVERURLV3C = 'ws://' + config.config_CLIP_SERVER_V3C; 
console.log(CLIPSERVERURLV3C);
const CLIPSERVERURLMVK = 'ws://' + config.config_CLIP_SERVER_MVK; 
console.log(CLIPSERVERURLMVK);
const CLIPSERVERURLLHE = 'ws://' + config.config_CLIP_SERVER_LHE; 
console.log(CLIPSERVERURLLHE);

const wss = new WebSocket.Server({ noServer: true }); //web socket to client
let clipWebSocketV3C = null;
let clipWebSocketMVK = null;
let clipWebSocketLHE = null;

const mongouri = 'mongodb://' + config.config_MONGODB_SERVER; // Replace with your MongoDB connection string
const MongoClient = require('mongodb').MongoClient;
let mongoclient = null;
connectMongoDB();

// Variables to store the parameter values
let text, concept, object, place, year, month, day, weekday, filename, similarto;

class QuerySettings {
    constructor(combineCLIPwithMongo = false, combineCLIPwithCLIP = 0, videoFiltering = 'all') {
        this.combineCLIPwithMongo = combineCLIPwithMongo;
        this.combineCLIPwithCLIP = combineCLIPwithCLIP;
        this.videoFiltering = videoFiltering;
    }
}

let settingsMap = new Map();

//////////////////////////////////////////////////////////////////
// Connection to client
//////////////////////////////////////////////////////////////////
const http = require('http');
const express = require('express');
const { LocalConfig } = require('./local-config');
const app = express();
app.use(cors());  // Enable CORS for all routes
const port = 8080
const server = app.listen(port, () => {
    console.log('WebSocket server is running on port ' + port);
});

//const server = http.createServer(app);
server.on('upgrade', (request, socket, head) => {
    //console.log('connection upgrade');
    wss.handleUpgrade(request, socket, head, (ws) => {
        //console.log('handle connection upgrade');
        wss.emit('connection', ws, request);
    });
});

function generateUniqueClientId() {
    return uuidv4();
}


let clients = new Map(); // This map stores the associations between client IDs and their WebSocket connections
wss.on('connection', (ws) => {
    // WebSocket connection handling logic
    
    let clientId = generateUniqueClientId(); // You would need to implement this function
    clients.set(clientId, ws);
    console.log('client connected: %s', clientId);
    let clientSettings = new QuerySettings();
    settingsMap.set(clientId, clientSettings);

    //check CLIPserver connection
    if (clipWebSocketV3C === null) {
        console.log('clipWebSocketV3C is null, try to re-connect');
        connectToCLIPServerV3C();
    }
    if (clipWebSocketMVK === null) {
        console.log('clipWebSocketMVK is null, try to re-connect');
        connectToCLIPServerMVK();
    }
    if (clipWebSocketLHE === null) {
        console.log('clipWebSocketLHE is null, try to re-connect');
        connectToCLIPServerLHE();
    }

    ws.on('message', (message) => {
        console.log('received from client: %s (%s)', message, clientId);
        // Handle the received message as needed

        msg = JSON.parse(message);

        //logging
        fs.appendFile(LOGFILE, JSON.stringify(msg), function (err) {
            if (err) {
                console.log('Error writing file', err)
            }
        });

        let clipWebSocket = null;
        if (msg.content.dataset == 'v3c') {
            clipWebSocket = clipWebSocketV3C;
        } else if (msg.content.dataset == 'mvk') {
            clipWebSocket = clipWebSocketMVK;
        } else if (msg.content.dataset == 'lhe') {
            clipWebSocket = clipWebSocketLHE;
        }

        //check video filtering
        videoFiltering = msg.content.videofiltering;
        clientSettings.videoFiltering = videoFiltering;
        if (videoFiltering == 'first') {
            msg.content.resultsperpage = msg.content.maxresults;
            msg.content.selectedpage = 1;
        }

        if (msg.content.type === 'clusters') {
            queryClusters(clientId);
        } else if (msg.content.type === 'videoinfo') {
            getVideoInfo(clientId, msg.content);
        } else if (msg.content.type === 'videofps') {
            getVideoFPS(clientId, msg.content, msg.correlationId);
        } else if (msg.content.type === 'videosummaries') {
            getVideoSummaries(clientId, msg.content);
        } else if (msg.content.type === 'ocr-text') {
            queryOCRText(clientId, msg.content);
        } else if (msg.content.type === 'metadata') {
            queryMetadata(clientId, msg.content);
        } else if (msg.content.type === 'speech') {
            querySpeech(clientId, msg.content);
        } else if (msg.content.type === 'videoid') {
            queryVideoID(clientId, msg.content);
        } else if (msg.content.type === 'clusters') {
            queryClusters(clientId);
        } else if (msg.content.type === 'cluster') {
            queryCluster(clientId, msg.content);
        } else {
            //check CLIPserver connection
            if (clipWebSocket === null) {
                console.log('clipWebSocket is null');
            } else {
                // Append jsonString to the file
                msg.clientId = clientId; //give client a unique id on the node server (and set it for every msg)
                
                if (msg.content.type === 'textquery') {
                    lenBefore = msg.content.query.trim().length;
                    clipQuery = parseParameters(msg.content.query)
 
                    if (clipQuery.trim().length > 0) {
                        msg.content.query = clipQuery
                        msg.content.clientId = clientId

                        if (clipQuery.length !== lenBefore) { //msg.content.query.trim().length || isOnlyDateFilter()) {
                            msg.content.resultsperpage = msg.content.maxresults;
                        }

                        console.log('sending to CLIP server: "%s" len=%d content-len=%d (rpp=%d, max=%d) - %d %d %d', clipQuery, clipQuery.length, msg.content.query.length, msg.content.resultsperpage, msg.content.maxresults, clipQuery.length, msg.content.query.trim().length, lenBefore);
                        
                        let clipQueries = Array();
                        let tmpClipQuery = clipQuery;
                        if (tmpClipQuery.includes('<')) {
                            let idxS = -1;
                            do {
                                idxS = tmpClipQuery.indexOf('<');
                                if (idxS > -1) {
                                    clipQueries.push(tmpClipQuery.substring(0,idxS));
                                    tmpClipQuery = tmpClipQuery.substring(idxS+1);
                                } else {
                                    clipQueries.push(tmpClipQuery); //last one
                                }
                            } while (idxS > -1);
                            console.log('found ' + clipQueries.length + ' temporal queries:');
                            for (let i=0; i < clipQueries.length; i++) {
                                console.log(clipQueries[i]);
                            }
                        }

                        if (clipQueries.length > 0) {
                            clientSettings.combineCLIPwithCLIP = clipQueries.length;
                            for (let i=0; i < clipQueries.length; i++) {
                                let tmsg = msg;
                                tmsg.content.query = clipQueries[i];
                                tmsg.content.resultsperpage = tmsg.content.maxresults;
                                clipWebSocket.send(JSON.stringify(tmsg));
                            }
                            clipQueries = Array();
                        } else {
                            //C L I P   +   D B   Q U E R Y  <---- NO, not working now
                            //clientSettings.combineCLIPwithMongo = true
                            //msg.content.resultsperpage = msg.content.maxresults;
                            clipWebSocket.send(JSON.stringify(msg));
                        }   
                    }
                } else if (msg.content.type === 'similarityquery') {
                    clipWebSocket.send(JSON.stringify(msg));
                } else if (msg.content.type === 'file-similarityquery') {
                    clipWebSocket.send(JSON.stringify(msg));
                }
            }
        }
    });
    
    ws.on('close', function close() {
        console.log('client disconnected');
        // Close the MongoDB connection when finished
        //mongoclient.close();
    });
});


//////////////////////////////////////////////////////////////////
// Parameter Parsing
//////////////////////////////////////////////////////////////////

function parseParameters(inputString) {
    // Define the regex pattern to match parameters and their values
    const regex = /-([a-zA-Z]+)\s(\S+)/g;
    
    text = concept = object = place = year = month = day = weekday = filename = similarto = '';

    // Iterate over matches
    let match;
    while ((match = regex.exec(inputString.trim()))) {
        const [, parameter, value] = match; // Destructure the matched values

        // Assign the value to the corresponding variable
        switch (parameter) {
            case 't':
                text = value;
                /*if (value === '"') {
                    const endQuoteIndex = value.indexOf('"', 1); // Find the index of the next double-quote starting from index 1
                    if (endQuoteIndex !== -1) {
                        const extractedString = value.substring(1, endQuoteIndex); // Extract the string between the first pair of double-quotes
                        const remainingString = value.substring(endQuoteIndex + 1); // Get the remaining string after the extracted substring
                    }
                }*/
                break;
            case 'c':
                concept = value;
                break;
            case 'o':
                object = value;
                break;
            case 'p':
                place = value;
                break;
            case 'wd':
                weekday = value;
                break;
            case 'd':
                day = value;
                break;
            case 'm':
                month = value;
                break;
            case 'fn':
                filename = value;
                break;
            case 'sim':
                similarto = value;
                break;
            case 'y':
                year = value;
                break;
        }
    }

    console.log('filters: text=%s concept=%s object=%s place=%s weekday=%s day=%s month=%s year=%s filename=%s', text, concept, object, place, weekday, day, month, year, filename);

    // Extract and remove the matched parameters from the input string
    const updatedString = inputString.replace(regex, '').trim();

    return updatedString.trim();
} 



//////////////////////////////////////////////////////////////////
// Connection to CLIP server
//////////////////////////////////////////////////////////////////
function connectToCLIPServerV3C() {
    let dataset = 'V3C';
    try {
        console.log('trying to connect to CLIP ' + dataset + ' ...');
        clipWebSocketV3C = new WebSocket(CLIPSERVERURLV3C);

        clipWebSocketV3C.on('open', () => {
            console.log('connected to CLIP ' + dataset + ' server');
        })
        
        clipWebSocketV3C.on('close', (event) => {
            // Handle connection closed
            clipWebSocketV3C.close();
            clipWebSocketV3C = null;
            console.log('Connection to CLIP ' + dataset + ' closed', event.code, event.reason);
        });
        
        pendingCLIPResults = Array();

        clipWebSocketV3C.on('message', (message) => {
            handleCLIPResponse(message);
        })

        clipWebSocketV3C.on('error', (event) => {
            console.log('Connection to CLIP ' + dataset + ' refused');
        });

    } catch(error) {
        console.log("Cannot connect to CLIP ' + dataset + ' server");   
    }
}

function connectToCLIPServerMVK() {
    let dataset = 'MVK';
    try {
        console.log('trying to connect to CLIP ' + dataset + ' ...');
        clipWebSocketMVK = new WebSocket(CLIPSERVERURLMVK);

        clipWebSocketMVK.on('open', () => {
            console.log('connected to CLIP ' + dataset + ' server');
        })
        
        clipWebSocketMVK.on('close', (event) => {
            // Handle connection closed
            clipWebSocketMVK.close();
            clipWebSocketMVK = null;
            console.log('Connection to CLIP ' + dataset + ' closed', event.code, event.reason);
        });
        
        pendingCLIPResults = Array();

        clipWebSocketMVK.on('message', (message) => {
            handleCLIPResponse(message);
        })

        clipWebSocketMVK.on('error', (event) => {
            console.log('Connection to CLIP ' + dataset + ' refused');
        });

    } catch(error) {
        console.log("Cannot connect to CLIP ' + dataset + ' server");   
    }
}

function connectToCLIPServerLHE() {
    let dataset = 'LHE';
    try {
        console.log('trying to connect to CLIP ' + dataset + ' ...');
        clipWebSocketLHE = new WebSocket(CLIPSERVERURLLHE);

        clipWebSocketLHE.on('open', () => {
            console.log('connected to CLIP ' + dataset + ' server');
        })
        
        clipWebSocketLHE.on('close', (event) => {
            // Handle connection closed
            clipWebSocketLHE.close();
            clipWebSocketLHE = null;
            console.log('Connection to CLIP ' + dataset + ' closed', event.code, event.reason);
        });
        
        pendingCLIPResults = Array();

        clipWebSocketLHE.on('message', (message) => {
            handleCLIPResponse(message);
        })

        clipWebSocketLHE.on('error', (event) => {
            console.log('Connection to CLIP ' + dataset + ' refused');
        });

    } catch(error) {
        console.log("Cannot connect to CLIP ' + dataset + ' server");   
    }
}

function handleCLIPResponse(message) {
    //console.log('received from CLIP server: ' + message);
    msg = JSON.parse(message);
    numbefore = msg.results.length;
    clientId = msg.clientId;
    clientWS = clients.get(clientId);
    let clientSettings = settingsMap.get(clientId);

    console.log('received %s results from CLIP server', msg.num);

    if (clientSettings.combineCLIPwithMongo === true) {
        //========================
        //currently not used by the client!
        //========================
        console.log('combined query');
        let combinedResults = [];

        const database = mongoclient.db(config.config_MONGODB); // Replace with your database name
        const collection = database.collection('videos'); // Replace with your collection name
        var { query, projection } = getMongoQuery(year, month, day, weekday, text, concept, object, place, filename); 
        console.log('(1) mongodb query: %s', JSON.stringify(query));
        const sortCriteria = { filepath: 1 }; //-1 for desc
        collection.find(query, projection).sort(sortCriteria).toArray((error, documents) => {
            if (error) {
                return;
            }

            console.log('got %d results from mongodb', documents.length);
            let processingInfo = {"type": "info",  "num": 1, "totalresults": 1, "message": documents.length + " results in database, now filtering..."};
            clientWS.send(JSON.stringify(processingInfo));

            const dateSet = new Set();

            for (let i = 0; i < msg.results.length; i++) {
                const elem = msg.results[i];

                for (let k = 0; k < documents.length; k++) {
                    if (elem === documents[k].filepath) {
                        combinedResults.push(elem);
                        break;
                    } else if (elem < documents[k].filepath) {
                        break;
                    }
                }
            }

            msg.results = combinedResults;
            msg.totalresults = combinedResults.length;
            msg.num = combinedResults.length;

            console.log('forwarding %d combined results to client %s', msg.totalresults, clientId);
            //console.log(JSON.stringify(msg));
            clientWS.send(JSON.stringify(msg));

            // Append jsonString to the file
            msg.clientId = clientId;
            fs.appendFile(LOGFILE, JSON.stringify(msg), function (err) {
                if (err) {
                    console.log('Error writing file', err)
                }
            });

        });

    } 
    else if (clientSettings.combineCLIPwithCLIP > 0) {
        pendingCLIPResults.push(msg);
        clientSettings.combineCLIPwithCLIP--;
        if (clientSettings.combineCLIPwithCLIP == 0) {
            let jointResults = Array();
            let jointResultsIdx = Array();
            let jointScores = Array();
            let videoIds = Array();

            for (let r = 1; r < pendingCLIPResults.length; r++) {
                let tresPrev = pendingCLIPResults[r-1].results;
                let tres = pendingCLIPResults[r].results;
                let tresIdx = pendingCLIPResults[r].resultsidx;
                let tresScores = pendingCLIPResults[r].scores;

                for (let i = 0; i < tres.length; i++) {
                    let vid = tres[i].substring(0,11);
                    let frame = parseInt(tres[i].substring(12,tres[i].indexOf('.')));
                    
                    for (let j = 0; j < tresPrev.length; j++) {
                        let vidP = tresPrev[j].substring(0,11);
                        let frameP = parseInt(tresPrev[j].substring(12,tres[i].indexOf('.')));

                        if (vid == vidP && frame > frameP) {

                            let videoid = getVideoId(tres[i]);
                            if (clientSettings.videoFiltering === 'first' && videoIds.includes(videoid)) {
                                continue;
                            }
                            videoIds.push(videoid);

                            jointResults.push(tres[i]);
                            jointResultsIdx.push(tresIdx[i]);
                            jointScores.push(tresScores[i]);
                            //console.log('found: ' + tres[i] + ': ' + vid + ' ' + frame + " > " + vidP + " " + frameP);
                            break;
                        }
                    }
                }
            }
            msg.results = jointResults;
            msg.resultsidx = jointResultsIdx;
            msg.scores = jointScores;
            msg.totalresults = jointResults.length;
            msg.num = jointResults.length;
            console.log('forwarding %d joint results to client %s', msg.totalresults, clientId);
            pendingCLIPResults = Array();
            clientWS.send(JSON.stringify(msg));
        }
        
    }
    else {
        let filteredResults = Array();
        let filteredResultsIdx = Array();
        let filteredScores = Array();
        let videoIds = Array();
        for (let i = 0; i < msg.results.length; i++) {
            let videoid = getVideoId(msg.results[i]);
            if (clientSettings.videoFiltering === 'first' && videoIds.includes(videoid)) {
                continue;
            }
            videoIds.push(videoid);
            filteredResults.push(msg.results[i]);
            filteredResultsIdx.push(msg.resultsidx[i]);
            filteredScores.push(msg.scores[i]);
        }

        //msg.totalresults = filteredResults.length;
        msg.results = filteredResults;
        msg.resultsidx = filteredResultsIdx;
        msg.scores = filteredScores;
        
        numafter = msg.results.length;
        if (numafter !== numbefore) {
            msg.totalresults = msg.totalresults - (numafter - numbefore); //msg.results.length;
            msg.num = msg.results.length;
        }
        console.log('forwarding %d results (current before=%d after=%d) to client %s', msg.totalresults, numbefore, numafter, clientId);
        //console.log(JSON.stringify(msg));
        clientWS.send(JSON.stringify(msg));

        // Append jsonString to the file
        msg.clientId = clientId;
        fs.appendFile(LOGFILE, JSON.stringify(msg), function (err) {
            if (err) {
                console.log('Error writing file', err)
            }
        });
    }
}

connectToCLIPServerV3C();
connectToCLIPServerMVK();
connectToCLIPServerLHE();


function getVideoId(result) {
    const elem = result;
    let filename = elem.split('/');
    let videoid = filename[0];
    return videoid;
}


//////////////////////////////////////////////////////////////////
// MongoDB Queries
//////////////////////////////////////////////////////////////////

function connectMongoDB() {
    mongoclient = new MongoClient(mongouri);

    //connect to mongo
    mongoclient.connect((err) => {
        if (err) {
            console.error('error connecting to mongodb: ', err);
            return;
        }
    });

    mongoclient.on('close', () => {
        console.log('mongodb connection closed');
    });
}

function getMongoQuery(yearValue, monthValue, dayValue, weekdayValue, textValue, conceptValue, objectValue, placeValue, filenameValue) {
    let query = {};

    if (yearValue.toString().trim().length > 0) {
        query.year = parseInt(yearValue);
    }

    if (monthValue.toString().trim().length > 0) {
        query.month = parseInt(monthValue);
    }

    if (dayValue.toString().trim().length > 0) {
        query.day = parseInt(dayValue);
    }

    if (weekdayValue.toString().trim().length > 0) {
        query.weekday = weekdayValue;
    }

    if (textValue.toString().trim().length > 0) {
        if (textValue.includes(',')) {
            let texts = textValue.split(",");
            let text = { $all: texts };
            query['texts.text'] = text;
        } else {
            let text = { $elemMatch: { "text": { $regex: textValue, $options: 'i' } } };
            query.texts = text;
        }
    }

    if (conceptValue.toString().trim().length > 0) {
        if (conceptValue.includes(',')) {
            let concepts = conceptValue.split(",");
            let concept = { $all: concepts };
            query['concepts.concept'] = concept;
        } else {
            conceptValue = '^' + conceptValue + '$';
            let concept = { $elemMatch: { "concept": { $regex: conceptValue, $options: 'i' } } };
            query.concepts = concept;
        }
    }

    if (objectValue.toString().trim().length > 0) {
        if (objectValue.includes(',')) {
            let objects = objectValue.split(",");
            let obj = { $all: objects };
            query['objects.object'] = obj;
        } else {
            objectValue = '^' + objectValue + '$';
            let obj = { $elemMatch: { "object": { $regex: objectValue, $options: 'i' } } };
            query.objects = obj;
        }
    }

    if (placeValue.toString().trim().length > 0) {
        if (placeValue.includes(',')) {
            let places = placeValue.split(",");
            let place = { $all: places };
            query['places.place'] = place;
        } else {
            placeValue = '^' + placeValue + '$';
            let place = { $elemMatch: { "place": { $regex: placeValue, $options: 'i' } } };
            query.places = place;
        }
    }

    if (filenameValue.toString().trim().length > 0) {
        query.filename = { $regex: filenameValue, $options: 'i' };
    }

    //if (queryMode === 'distinctive') {
    //    query.l2dist = { $gt: DISTINCTIVE_L2DIST1 };
    //} else if (queryMode == 'moredistinctive') {
    //    query.l2dist = { $gt: DISTINCTIVE_L2DIST2 };
    //}

    console.log(JSON.stringify(query));

    const projection = { filepath: 1 };

    return { query, projection };
}

  
async function queryClusters(clientId) {
    try {
        let clientSettings = settingsMap.get(clientId);
        const database = mongoclient.db(config.config_MONGODB); // Replace with your database name
        const collection = database.collection('clusters'); // Replace with your collection name
    
        const cursor = collection.find().sort({'members': -1});
        let results = [];
        await cursor.forEach(document => {
            results.push(document);
        });
        
        let response = { "type": "concepts", "num": results.length, "results": results };
        clientWS = clients.get(clientId);
        clientWS.send(JSON.stringify(response));
        //console.log('sent back: ' + JSON.stringify(response));
    } catch (error) {
        console.log("error with mongodb: " + error);
        await mongoclient.close();
    }
}

async function getVideoFPS(clientId, queryInput, correlationId) {
    try {
        let clientSettings = settingsMap.get(clientId);
        //console.log('received '+ JSON.stringify(queryInput));
        const database = mongoclient.db(config.config_MONGODB); // Replace with your database name
        const collection = database.collection('videos'); // Replace with your collection name

        let projection = {fps: 1, duration: 1};

        let query = {};
        query = {'videoid': queryInput.videoid};

        const cursor = collection.find(query, {projection: projection});
        let results = [];
        await cursor.forEach(document => {
            results.push(document);
        });

        let response = { "type": "videofps", "synchronous": query.synchronous, "videoid": queryInput.videoid, "fps": results[0].fps, "duration": results[0].duration, "correlationId": correlationId };
        clientWS = clients.get(clientId);
        clientWS.send(JSON.stringify(response));
        //console.log('sent back fps info: ' + JSON.stringify(response))

    }  catch (error) {
        console.log("error with mongodb: " + error);
        await mongoclient.close();
    } finally {
      // Close the MongoDB connection when finished
      //await mongoclient.close();
    }
}


async function getVideoInfo(clientId, queryInput) {
    try {
        let clientSettings = settingsMap.get(clientId);
        const database = mongoclient.db(config.config_MONGODB); // Replace with your database name
        const collection = database.collection('videos'); // Replace with your collection name

        let query = {};
        query = {'videoid': queryInput.videoid};

        const cursor = collection.find(query);
        let results = [];
        await cursor.forEach(document => {
            results.push(document);
        });

        let response = { "type": "videoinfo", "content": results };
        clientWS = clients.get(clientId);
        clientWS.send(JSON.stringify(response));
        //console.log('sent back: ' + JSON.stringify(response))

    }  catch (error) {
        console.log("error with mongodb: " + error);
        await mongoclient.close();
    } finally {
      // Close the MongoDB connection when finished
      //await mongoclient.close();
    }
}

async function getVideoSummaries(clientId, queryInput) {
    try {
        let clientSettings = settingsMap.get(clientId);
        const database = mongoclient.db(config.config_MONGODB); // Replace with your database name
        const collection = database.collection('videos'); // Replace with your collection name

        let query = {};
        query = {'videoid': queryInput.videoid};

        const cursor = collection.find(query).project({_id:0,videoid:1,summaries:1});
        let results = [];
        await cursor.forEach(document => {
            results.push(document);
        });

        let response = { "type": "videosummaries", "content": results };
        clientWS = clients.get(clientId);
        clientWS.send(JSON.stringify(response));
        //console.log('sent back: ' + JSON.stringify(response))

    }  catch (error) {
        console.log("error with mongodb: " + error);
        await mongoclient.close();
    } 
}

async function queryOCRText(clientId, queryInput) {
    try {
        let clientSettings = settingsMap.get(clientId);
        const database = mongoclient.db(config.config_MONGODB); // Replace with your database name
        const collection = database.collection('texts'); // Replace with your collection name

        // Find the document with the matching text
        //const document = await collection.findOne({ text: {"$regex": queryInput.query, "$options": "i" }});
        const document = await collection.findOne({ text: {$regex: new RegExp(queryInput.query, "i")} });
        let response = { "type": "ocr-text", "num": 0, "results": [], "totalresults": 0, "scores": [], "dataset": "v3c" };
        
        if (document) {
            response.num = document.frames.length;
            response.results = document.frames;
            response.totalresults = response.num;   

            if (clientSettings.videoFiltering === 'first') {
                let filteredFrames = [];
                let videoIds = Array();
                for (const frame of document.frames) {
                    let videoid = getVideoId(frame);
                    if (videoIds.includes(videoid)) {
                        continue;
                    }
                    videoIds.push(videoid);
                    filteredFrames.push(frame);
                }
                response.num = filteredFrames.length;
                response.results = filteredFrames;
            }

            response.scores  = new Array(document.frames.length).fill(1);
        }

        clientWS = clients.get(clientId);
        clientWS.send(JSON.stringify(response));

    }  catch (error) {
        console.log("error with mongodb: " + error);
        await mongoclient.close();
    } 
}

async function queryVideoID(clientId, queryInput) {
    try {
        let clientSettings = settingsMap.get(clientId);
        const database = mongoclient.db(config.config_MONGODB); // Replace with your database name
        const collection = database.collection('videos'); // Replace with your collection name

        // Find the document with the matching text
        const cursor = await collection.find({ videoid: { $regex: queryInput.query, $options: "i" } });
        
        let response = { "type": "videoid", "num": 0, "results": [], "totalresults": 0, "scores": [], "dataset": "v3c" };
        
        /*let results = [];
        let scores = [];
        if (document) {
            for(const shot of document.shots) {
                results.push(document.videoid + '/' + shot.keyframe);
                scores.push(1);
            }
        }*/
        if (cursor) {
            let results = [];
            let scores = [];
            let videoIds = Array();
            await cursor.forEach(document => {
                for(const shot of document.shots) {
                    if (clientSettings.videoFiltering === 'first' && videoIds.includes(document.videoid)) {
                        continue;
                    }
                    videoIds.push(document.videoid);
                    results.push(document.videoid + '/' + shot.keyframe);
                    scores.push(1);
                }
            });
            
            response.num = results.length;
            response.totalresults = results.length;
            response.scores = scores;
            response.results = results;
        
        }

        clientWS = clients.get(clientId);
        clientWS.send(JSON.stringify(response));

    }  catch (error) {
        console.log("error with mongodb: " + error);
        await mongoclient.close();
    } 
}

async function queryMetadata(clientId, queryInput) {
    try {
        let clientSettings = settingsMap.get(clientId);
        const database = mongoclient.db(config.config_MONGODB); // Replace with your database name
        const collection = database.collection('videos'); // Replace with your collection name

        const regexQuery = new RegExp(queryInput.query, "i"); // Create a case-insensitive regular expression

        const cursor = await collection.find({
            $or: [
                { description: { $regex: regexQuery } },
                { channel: { $regex: regexQuery } },
                { title: { $regex: regexQuery } },
                { tags: { $regex: regexQuery } }
            ]
        });
        
        let results = [];
        let scores = [];
        let videoIds = Array();
        await cursor.forEach(document => {
            for(const shot of document.shots) {
                if (clientSettings.videoFiltering === 'first' && videoIds.includes(document.videoid)) {
                    continue;
                }
                videoIds.push(document.videoid);
                results.push(document.videoid + '/' + shot.keyframe);
                scores.push(1);
            }
        });
        

        let response = { "type": "metadata", "num": results.length, "results": results, "totalresults": results.length, "scores": scores, "dataset": "v3c" };

        clientWS = clients.get(clientId);
        clientWS.send(JSON.stringify(response));

    }  catch (error) {
        console.log("error with mongodb: " + error);
        await mongoclient.close();
    } 
}

async function querySpeech(clientId, queryInput) {
    try {
        let clientSettings = settingsMap.get(clientId);
        const database = mongoclient.db(config.config_MONGODB); // Replace with your database name
        const collection = database.collection('videos'); // Replace with your collection name

        const regexQuery = new RegExp(queryInput.query, "i"); // Create a case-insensitive regular expression

        const cursor = await collection.find({
            $or: [
                { "speech.text": { $regex: regexQuery } },
                { "speech.keywords": { $regex: regexQuery } },
            ]
        });
        
        let results = [];
        let scores = [];
        let videoIds = Array();
        await cursor.forEach(document => {
            for(const shot of document.shots) {
                if (clientSettings.videoFiltering === 'first' && videoIds.includes(document.videoid)) {
                    continue;
                }
                videoIds.push(document.videoid);
                results.push(document.videoid + '/' + shot.keyframe);
                scores.push(1);
            }
        });
        

        let response = { "type": "speech", "num": results.length, "results": results, "totalresults": results.length, "scores": scores, "dataset": "v3c" };

        clientWS = clients.get(clientId);
        clientWS.send(JSON.stringify(response));

    }  catch (error) {
        console.log("error with mongodb: " + error);
        await mongoclient.close();
    } 
}

async function queryClusters(clientId) {
    try {
        let clientSettings = settingsMap.get(clientId);
        const database = mongoclient.db(config.config_MONGODB); // Replace with your database name
        const collection = database.collection('clusters'); // Replace with your collection name

        // Fetch the clusters and sort them by the size of 'memberss' array (in descending order)
        const cursor = collection.find({}).sort({ "count": -1 }).project({'cluster_id': 1, 'name': 1, 'count': 1});
        
        // Converting cursor to array (You can also use forEach to avoid loading all into memory)
        const clusters = await cursor.toArray();

        let response = { "type": "clusters", "num": clusters.length, "results": clusters, "scores": new Array(clusters.length).fill(1), "dataset": "v3c" };

        clientWS = clients.get(clientId);
        clientWS.send(JSON.stringify(response));

    }  catch (error) {
        console.log("error with mongodb: " + error);
        await mongoclient.close();
    } 
}


async function queryCluster(clientId, queryInput) {
    try {
        let clientSettings = settingsMap.get(clientId);
        const database = mongoclient.db(config.config_MONGODB); // Replace with your database name
        const collection = database.collection('clusters'); // Replace with your collection name

        // Fetch the clusters and sort them by the size of 'memberss' array (in descending order)
        const document = await collection.findOne({'cluster_id': queryInput.query});
        let results = [];
        let scores = [];
        if (document) {
            for(const member of document.members) {
                results.push(member);
                scores.push(1);
            }
        }

        let response = { "type": "cluster", "num": results.length, "results": results, "scores": scores, "dataset": "v3c" };

        clientWS = clients.get(clientId);
        clientWS.send(JSON.stringify(response));

    }  catch (error) {
        console.log("error with mongodb: " + error);
        await mongoclient.close();
    } 
}

