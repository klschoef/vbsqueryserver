const config = require('./local-config.js');
const WebSocket = require('ws');
const cors = require('cors');
const fs = require('fs');

const { v4: uuidv4 } = require('uuid');

const LOGFILE = 'vbsqueryserverlog.json'

const DISTINCTIVE_L2DIST1 = 10.0;
const DISTINCTIVE_L2DIST2 = 15.0;
const CLIPSERVERURL = 'ws://' + config.config_CLIP_SERVER; //'ws://extreme00.itec.aau.at:8002';
console.log(CLIPSERVERURL);
const wss = new WebSocket.Server({ noServer: true });
let clipWebSocket = null;

const mongouri = 'mongodb://' + config.config_MONGODB_SERVER; // Replace with your MongoDB connection string
const MongoClient = require('mongodb').MongoClient;
let mongoclient = null;
connectMongoDB();

// Variables to store the parameter values
let text, concept, object, place, year, month, day, weekday, filename, similarto;
let combineCLIPWithMongo = false, filterCLIPResultsByDate = false, queryMode = 'all';

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

function isOnlyDateFilter() {
    if (weekday.toString().trim().length > 0 ||
        text.toString().trim().length > 0 ||
        filename.toString().trim().length > 0 ||
        concept.toString().trim().length > 0 ||
        object.toString().trim().length > 0 ||
        place.toString().trim().length > 0 ||
        similarto.toString().trim().length > 0) {
            return false;
        }
    else {
        return true;
    }
}

function generateUniqueClientId() {
    return uuidv4();
}


let clients = new Map(); // This map stores the associations between client IDs and their WebSocket connections
wss.on('connection', (ws) => {
    // WebSocket connection handling logic
    
    let clientId = generateUniqueClientId(); // You would need to implement this function
    clients.set(clientId, ws);

    console.log('client connected: %s', clientId);

    //check CLIPserver connection
    if (clipWebSocket === null) {
        console.log('clipWebSocket is null, try to re-connect');
        connectToCLIPServer();
    }

    ws.on('message', (message) => {
        console.log('received from client: %s (%s)', message, clientId);
        // Handle the received message as needed

        msg = JSON.parse(message);

        if (msg.content.type === 'clusters') {
            queryClusters(clientId);
        } else if (msg.content.type === 'videoinfo') {
            getVideoInfo(clientId, msg.content);
        } else if (msg.content.type === 'videosummaries') {
            getVideoSummaries(clientId, msg.content);
        } else if (msg.content.type === 'ocr-text') {
            queryOCRText(clientId, msg.content);
        } else if (msg.content.type === 'metadata') {
            queryMetadata(clientId, msg.content);
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
                fs.appendFile(LOGFILE, JSON.stringify(msg), function (err) {
                    if (err) {
                        console.log('Error writing file', err)
                    }
                });

                if (msg.content.type === 'textquery') { // || msg.content.type == 'file-similarityquery'

                    nodequery = msg.content.query;

                    queryMode = msg.content.queryMode;

                    lenBefore = msg.content.query.trim().length;
                    clipQuery = parseParameters(msg.content.query)
                    combineCLIPWithMongo = false;
                    filterCLIPResultsByDate = false;

                    //special hack for file-similarity
                    /*if (similarto !== '') {
                        msg.query = similarto;
                        msg.pathprefix = '';
                        msg.type = 'file-similarityquery';
                        clipQuery = 'non-empty-string';
                    }*/
                    
                    if (clipQuery.trim().length > 0) {
                        msg.content.query = clipQuery
                        msg.content.clientId = clientId

                        if (clipQuery.length !== lenBefore) { //msg.content.query.trim().length || isOnlyDateFilter()) {
                            msg.content.resultsperpage = msg.content.maxresults;
                        }

                        console.log('sending to CLIP server: "%s" len=%d content-len=%d (rpp=%d, max=%d) - %d %d %d', clipQuery, clipQuery.length, msg.content.query.length, msg.content.resultsperpage, msg.content.maxresults, clipQuery.length, msg.content.query.trim().length, lenBefore);
                        

                        if (isOnlyDateFilter() && queryMode !== 'distinctive' && queryMode !== 'moredistinctive') {
                            //C L I P   Q U E R Y   +   F I L T E R
                            filterCLIPResultsByDate = true;
                            //msg.content.resultsperpage = msg.content.maxresults;
                            clipWebSocket.send(JSON.stringify(msg));
                        } else {
                            //C L I P   +   D B   Q U E R Y
                            combineCLIPWithMongo = true;
                            //msg.content.resultsperpage = msg.content.maxresults;
                            clipWebSocket.send(JSON.stringify(msg));
                        }

                        
                    } else {
                        //D B   Q U E R Y
                        console.log('querying Node server');
                        queryImages(year, month, day, weekday, text, concept, object, place, filename, clientId).then((queryResults) => {
                            console.log("query* finished");
                            if ("results" in queryResults) {
                                console.log('sending %d results to client', queryResults.results.length);
                                ws.send(JSON.stringify(queryResults));

                                // Append jsonString to the file
                                queryResults.clientId = clientId;
                                fs.appendFile(LOGFILE, JSON.stringify(queryResults), function (err) {
                                    if (err) {
                                        console.log('Error writing file', err)
                                    }
                                });
                            }
                        });
                    }
                } else if (msg.content.type === 'similarityquery') {
                    combineCLIPWithMongo = false;
                    filterCLIPResultsByDate = false;
                    clipWebSocket.send(JSON.stringify(msg));
                } else if (msg.content.type === 'file-similarityquery') {
                    combineCLIPWithMongo = false;
                    filterCLIPResultsByDate = false;
                    clipWebSocket.send(JSON.stringify(msg));
                } else if (msg.content.type === 'metadataquery') {
                    queryImage(msg.content.imagepath).then((queryResults) => {
                        console.log("query finished");
                        if (queryResults != undefined && "results" in queryResults) {
                            console.log('sending %d results to client', queryResults.results.length);
                            ws.send(JSON.stringify(queryResults));

                            // Append jsonString to the file
                            queryResults.clientId = clientId;
                            fs.appendFile(LOGFILE, JSON.stringify(queryResults), function (err) {
                                if (err) {
                                    console.log('Error writing file', err)
                                }
                            });
                        }
                    });
                } 
                else if (msg.content.type === 'objects') {
                    queryObjects(clientId);
                }
                else if (msg.content.type === 'concepts') {
                    queryConcepts(clientId);
                }
                else if (msg.content.type === 'places') {
                    queryPlaces(clientId);
                }
                else if (msg.content.type === 'texts') {
                    queryTexts(clientId);
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
function connectToCLIPServer() {
    try {
        console.log('trying to connect to CLIP...');
        clipWebSocket = new WebSocket(CLIPSERVERURL);
        console.log('after connection trial')

        clipWebSocket.on('open', () => {
            console.log('connected to CLIP server');
        
            // Start sending ping messages at the specified interval
            /*setInterval(() => {
                if (clipWebSocket.readyState === WebSocket.OPEN) {
                    let ping = { "content": {"ping":true} }
                    clipWebSocket.send(JSON.stringify(ping));
                }
            }, pingInterval);
            */
        })
        
        clipWebSocket.on('close', (event) => {
            // Handle connection closed
            clipWebSocket.close();
            clipWebSocket = null;
            console.log('Connection to CLIP closed', event.code, event.reason);
        });
        
        const weekdays = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
        


        clipWebSocket.on('message', (message) => {
            
            //console.log('received from CLIP server: ' + message);
        
            msg = JSON.parse(message);
            numbefore = msg.results.length;
            clientId = msg.clientId;
            clientWS = clients.get(clientId);

            console.log('received %s results from CLIP server', msg.num);

            if (combineCLIPWithMongo === true) {

                console.log('combined query');
                let combinedResults = [];

                const database = mongoclient.db('lsc'); // Replace with your database name
                const collection = database.collection('images'); // Replace with your collection name
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
                                if (queryMode === 'first') {
                                    let eyear = elem.substring(0,4);
                                    let emonth = elem.substring(4,6);
                                    let eday = elem.substring(7,9);
                                    let dateStr = eyear + emonth + eday;
                                    if (dateSet.has(dateStr) === false) {
                                        dateSet.add(dateStr);
                                        combinedResults.push(elem);
                                    }
                                } else {
                                    combinedResults.push(elem);
                                }
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
            else {

                if (filterCLIPResultsByDate === true || queryMode !== 'all') {
            
                    console.log('filter query');
                    let ly = year.toString().trim().length;
                    let lm = month.toString().trim().length;
                    let ld = day.toString().trim().length;
                    let lw = weekday.toString().trim().length;

                    const dateSet = new Set();
                
                    if (ly > 0 || lm > 0 || ld > 0 || lw > 0 || queryMode !== 'all') {
                        for (let i = 0; i < msg.results.length; i++) {
                            const elem = msg.results[i];
                            let eyear = elem.substring(0,4);
                            let emonth = elem.substring(4,6);
                            let eday = elem.substring(7,9);
                
                            if (ly > 0 && eyear !== year) {
                                msg.results.splice(i--, 1);
                            }
                            else if (lm > 0 && emonth !== month) {
                                msg.results.splice(i--, 1);
                            }
                            else if (ld > 0 && eday !== day) {
                                msg.results.splice(i--, 1);
                            }
                            else if (queryMode === 'first') {
                                let dateStr = eyear + emonth + eday;
                                if (dateSet.has(dateStr)) {
                                    msg.results.splice(i--,1);
                                } else {
                                    dateSet.add(dateStr);
                                }
                            }
                            else if (lw > 0) {
                                let dstr = eyear + '-' + emonth + '-' + eday;
                                let edate = new Date(dstr);
                                let wd = edate.getDay();
                                
                                if (weekdays[wd] === weekday) {
                                    msg.results.splice(i--, 1);
                                }
                            }
                        }
                    }
                }

                numafter = msg.results.length;
                if (numafter !== numbefore) {
                    msg.totalresults = msg.results.length;
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
            

        })

        clipWebSocket.on('error', (event) => {
            console.log('Connection to CLIP refused');
        });

    } catch(error) {
        console.log("Cannot connect to CLIP server");   
    }
}

connectToCLIPServer();




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



async function queryImages(yearValue, monthValue, dayValue, weekdayValue, textValue, conceptValue, objectValue, placeValue, filenameValue, clientId) {
  try {
    if (!mongoclient.isConnected()) {
        console.log('mongodb not connected!');
        connectMongoDB();
    } else {
        const database = mongoclient.db('lsc'); // Replace with your database name
        const collection = database.collection('images'); // Replace with your collection name

        clientWS = clients.get(clientId);

        const sortCriteria = { minute_id: 1 }; //-1 for desc
        var { query, projection } = getMongoQuery(yearValue, monthValue, dayValue, weekdayValue, textValue, conceptValue, objectValue, placeValue, filenameValue); //-1 for desc

        if (JSON.stringify(query) === "{}") {
            console.log('empty query not allowed');
            let queryResults = { "num": 0, "totalresults": 0, "results": []  };
            clientWS.send(JSON.stringify(queryResults));

            // Append jsonString to the file
            queryResults.clientId = clientId;
            fs.appendFile(LOGFILE, JSON.stringify(queryResults), function (err) {
                if (err) {
                    console.log('Error writing file', err)
                }
            });

            return queryResults;
        }

        console.log('mongodb query: %s', JSON.stringify(query));
        const cursor = collection.find(query, projection); //use sort(sortCriteria); //will give an array
        const count = await cursor.count();
        console.log('%d results to client %s', count, clientId);

        let processingInfo = {"type": "info",  "num": 1, "totalresults": 1, "message": count + " results in database, loading from server..."};
        clientWS.send(JSON.stringify(processingInfo));

        let queryResults = { "num": count, "totalresults": count };
        let results = [];

        const dateSet = new Set();

        await cursor.forEach(document => {
            // Access the filename field in each document
            const filename = document.filepath;

            if (queryMode === 'first') {
                let eyear = filename.substring(0,4);
                let emonth = filename.substring(4,6);
                let eday = filename.substring(7,9);
                let dateStr = eyear + emonth + eday;
                if (dateSet.has(dateStr) === false) {
                    results.push(filename);
                    dateSet.add(dateStr);
                } 
            } else {
                results.push(filename);
            }
            //console.log(filename);
        });

        queryResults.results = results;
        return queryResults;
    }
  } /*catch (error) {
    console.log("error with mongodb: " + error);
    await mongoclient.close();
  }*/ finally {
    // Close the MongoDB connection when finished
    //await mongoclient.close();
  }
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

    if (queryMode === 'distinctive') {
        query.l2dist = { $gt: DISTINCTIVE_L2DIST1 };
    } else if (queryMode == 'moredistinctive') {
        query.l2dist = { $gt: DISTINCTIVE_L2DIST2 };
    }

    console.log(JSON.stringify(query));

    const projection = { filepath: 1 };

    return { query, projection };
}

async function queryImage(url) {
    try {
        if (!mongoclient.isConnected()) {
            console.log('mongodb not connected!');
            connectMongoDB();
        } else {
            const database = mongoclient.db('vbs2023'); // Replace with your database name
            const collection = database.collection('images'); // Replace with your collection name
        
            let query = { "filepath": url }; 
        
            console.log('mongodb query: %s', JSON.stringify(query));
            const cursor = collection.find(query);
        
            let queryResults = { "type": "metadata", "num": 1, "totalresults": 1 };
            let results = [];
        
            await cursor.forEach(document => {
                // Access the filename field in each document
                results.push(document);
                //console.log(filename);
            });
        
            queryResults.results = results;
            return queryResults;
        }
  
    } catch (error) {
        console.log("error with mongodb: " + error);
        await mongoclient.close();
    } finally {
      // Close the MongoDB connection when finished
      //await mongoclient.close();
    }
}


async function queryObjects(clientId) {
    try {
        if (!mongoclient.isConnected()) {
            console.log('mongodb not connected!');
            connectMongoDB();
        } else {
            const database = mongoclient.db('vbs2023'); // Replace with your database name
            const collection = database.collection('objects'); // Replace with your collection name
        
            const cursor = collection.find({},{name:1}).sort({name: 1});
            let results = [];
            await cursor.forEach(document => {
                results.push(document);
            });
            
            let response = { "type": "objects", "num": results.length, "results": results };
            clientWS = clients.get(clientId);
            clientWS.send(JSON.stringify(response));
            //console.log('sent back: ' + JSON.stringify(response));
        }
  
    } catch (error) {
        console.log("error with mongodb: " + error);
        await mongoclient.close();
    } finally {
      // Close the MongoDB connection when finished
      //await mongoclient.close();
    }
  }

  async function queryConcepts(clientId) {
    try {
        if (!mongoclient.isConnected()) {
            console.log('mongodb not connected!');
            connectMongoDB();
        } else {
            const database = mongoclient.db('vbs2023'); // Replace with your database name
            const collection = database.collection('concepts'); // Replace with your collection name
        
            const cursor = collection.find({},{name:1}).sort({name: 1});
            let results = [];
            await cursor.forEach(document => {
                results.push(document);
            });
            
            let response = { "type": "concepts", "num": results.length, "results": results };
            clientWS = clients.get(clientId);
            clientWS.send(JSON.stringify(response));
            //console.log('sent back: ' + JSON.stringify(response));
        }
  
    } catch (error) {
        console.log("error with mongodb: " + error);
        await mongoclient.close();
    } finally {
      // Close the MongoDB connection when finished
      //await mongoclient.close();
    }
  }

  async function queryPlaces(clientId) {
    try {
        if (!mongoclient.isConnected()) {
            console.log('mongodb not connected!');
            connectMongoDB();
        } else {
            const database = mongoclient.db('vbs2023'); // Replace with your database name
            const collection = database.collection('places'); // Replace with your collection name
        
            const cursor = collection.find({},{name:1}).sort({name: 1});
            let results = [];
            await cursor.forEach(document => {
                results.push(document);
            });
            
            let response = { "type": "places", "num": results.length, "results": results };
            clientWS = clients.get(clientId);
            clientWS.send(JSON.stringify(response));
            //console.log('sent back: ' + JSON.stringify(response));
        }
  
    } catch (error) {
        console.log("error with mongodb: " + error);
        await mongoclient.close();
    } finally {
      // Close the MongoDB connection when finished
      //await mongoclient.close();
    }
  }

  
  async function queryClusters(clientId) {
    try {
        const database = mongoclient.db('vbs2023'); // Replace with your database name
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


async function getVideoInfo(clientId, queryInput) {
    try {
        const database = mongoclient.db('vbs2023'); // Replace with your database name
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
        const database = mongoclient.db('vbs2023'); // Replace with your database name
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
        const database = mongoclient.db('vbs2023'); // Replace with your database name
        const collection = database.collection('texts'); // Replace with your collection name

        // Find the document with the matching text
        //const document = await collection.findOne({ text: {"$regex": queryInput.query, "$options": "i" }});
        const document = await collection.findOne({ text: {$regex: new RegExp(queryInput.query, "i")} });
        let response = { "type": "ocr-text", "num": 0, "results": [], "scores": [], "dataset": "v3c" };
        if (document) {
            response.num = document.frames.length;
            response.results = document.frames;
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
        const database = mongoclient.db('vbs2023'); // Replace with your database name
        const collection = database.collection('videos'); // Replace with your collection name

        // Find the document with the matching text
        const document = await collection.findOne({ videoid: queryInput.query });
        let results = [];
        let scores = [];
        if (document) {
            for(const shot of document.shots) {
                results.push(document.videoid + '/' + shot.keyframe);
                scores.push(1);
            }
        }

        let response = { "type": "videoid", "num": results.length, "results": results, "scores": scores, "dataset": "v3c" };
        

        clientWS = clients.get(clientId);
        clientWS.send(JSON.stringify(response));

    }  catch (error) {
        console.log("error with mongodb: " + error);
        await mongoclient.close();
    } 
}

async function queryMetadata(clientId, queryInput) {
    try {
        const database = mongoclient.db('vbs2023'); // Replace with your database name
        const collection = database.collection('videos'); // Replace with your collection name

        const regexQuery = new RegExp(queryInput.query, "i"); // Create a case-insensitive regular expression

        const cursor = await collection.find({
            $or: [
                { description: { $regex: regexQuery } },
                { "speech.text": { $regex: regexQuery } },
                { "speech.keywords": { $regex: regexQuery } },
                { channel: { $regex: regexQuery } },
                { title: { $regex: regexQuery } },
                { tags: { $regex: regexQuery } }
            ]
        });
        
        let results = [];
        let scores = [];
        await cursor.forEach(document => {
            for(const shot of document.shots) {
                results.push(document.videoid + '/' + shot.keyframe);
                scores.push(1);
            }
        });
        

        let response = { "type": "metadata", "num": results.length, "results": results, "scores": scores, "dataset": "v3c" };

        clientWS = clients.get(clientId);
        clientWS.send(JSON.stringify(response));

    }  catch (error) {
        console.log("error with mongodb: " + error);
        await mongoclient.close();
    } 
}

async function queryClusters(clientId) {
    try {
        const database = mongoclient.db('vbs2023'); // Replace with your database name
        const collection = database.collection('clusters'); // Replace with your collection name

        // Fetch the clusters and sort them by the size of 'memberss' array (in descending order)
        const cursor = collection.find({}).sort({ "members": -1 }).project({'cluster_id': 1, 'name': 1, 'count': 1});
        
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
        const database = mongoclient.db('vbs2023'); // Replace with your database name
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

