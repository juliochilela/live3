'use strict';
let app = require('express')();
let http = require('http').Server(app);
let io = require('socket.io')(http);
var Kafka = require('no-kafka');


io.on('connection', (socket) => {
    console.log('USER CONNECTED');

    socket.on('disconnect', function() {
        console.log('USER DISCONNECTED');
    });

});

http.listen(8098, () => {
    console.log('started on port 8098');
    var consumer = new Kafka.SimpleConsumer({
        connectionString: 'IP ADDRESS:PORT',
        clientId: 'no-kafka-client'
    });

    // data handler function can return a Promise 
    var dataHandler = function(messageSet, topic, partition) {
        messageSet.forEach(function(m) {
            console.log(topic, partition, m.offset, m.message.value.toString('utf8'));
            if (topic == "topico20") {
                io.emit('topico20', { x: (new Date()).getTime(), y: m.message.value.toString('utf8') });
            } else {
                io.emit('outros', { x: (new Date()).getTime(), y: m.message.value.toString('utf8') });
            }
        });
    };

    return consumer.init().then(function() {
        // Subscribe partitons 0 and 1 in a topic: 
        var v20 = consumer.subscribe('topico20', [0, 1], dataHandler);
        var outros = consumer.subscribe('outros', [0, 1], dataHandler);
        var arr = [];
        arr.push([v20, outros]);

        return arr;

    });
});



