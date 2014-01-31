var WebSocket = require('ws'),
    fs = require('graceful-fs'),
    csv = require('csv');


// as the variables come in store them
module.exports = function (URL, FILE, OUTPUT_FILE, USERNAME, APIKEY){

    /*
    * Create the Websocket
    */
    var ws = new WebSocket('ws://' + URL),
        data_length = 0,
        lines_written = 0,
        messages_sent = 0;




    var sendMessage = function(line, callback){
        // convert line to json object
        var line_object = {};
        for (var j=0; j < line.length; j++){
            line_object[headers[j]] = line[j];
        }
        // line object is the csv line in JSON format

        multiplier = lines_written ? lines_written : 100;

        setTimeout(function(){
            ws.send(JSON.stringify(line_object));
            callback(false);
        }, Math.floor(messages_sent/multiplier));
        messages_sent++;
    };

    
    /* 
    * When the websocket is open..
    */
    ws.on('open', function() {
        // send the Username and API Key
        ws.send(JSON.stringify({ username: USERNAME, apikey: APIKEY }));

        // read the csv to an array
        csv().from.path(FILE, { delimiter: ',', escape: '"' }).to.array(function(data){
            headers = data[0];
            data_length = data.length;

            // iterate through lines
            async.eachLimit(data, 100, sendMessage, function(err){
                if (err){ console.log(err); }
            });
        });
    });

    ws.on('message', function(data, flags) {
        // convert data to json
        data = JSON.parse(data);

        // convert data to csv format
        var res = [];
        Object.keys(data).forEach(function(key) {
            res.push(data[key]);
        });
        var csv_out_line = res.join(",") + '\n';

        // if file exists append the output line
        // else create file and add line
        if (fs.existsSync(OUTPUT_FILE)) {
            fs.appendFile(OUTPUT_FILE, csv_out_line, function (err) {
                if (err) { console.log(err); }
                //console.log('wrote line');
            });
        } else {
            fs.writeFile(OUTPUT_FILE, csv_out_line, function (err) {
                if (err) { console.log(err); }
                //console.log('created file');
            });
        }
        lines_written++;
        if (lines_written == data_length){
            process.exit(0);
        }
    });
};