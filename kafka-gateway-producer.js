"use strict";

class KafkaGateway_Producer {
    constructor(gateway, DedicateName, verboseMode) {
        this.requires = {
            gateway: gateway,
        };
        this.packetCounter = 0;
        this.DedicateName = DedicateName;
        this.verboseMode = verboseMode;

        var options = {
            // // Configuration for when to consider a message as acknowledged, default 1 
            requireAcks: 1,
            // // The amount of time in milliseconds to wait for all acks before considered, default 100ms 
            ackTimeoutMs: 100,
            // // Partitioner type (default = 0, random = 1, cyclic = 2, keyed = 3, custom = 4), default 0 
            partitionerType: 2
        };

        this.Producer = new this.requires.gateway.requires.HighLevelProducer(this.requires.gateway.KafkaClient, options);//,MyPartitioner);
        this.Producer.on('ready', function () {
            gateway.emit('gateway-producer-ready');
        });
    }

    send(topic, event, param, ack, callback) {
        var id = this.packetCounter = this.packetCounter + 1;
        // console.trace("Here I am!")
        var time = new Date().getTime();

        var packet = {
            param: param,
            event: event,
            id: id,
            sender: this.DedicateName,
            stamp: time
        };

        if (event == 'gateway-ping')
            param[0]['Ping_send_time'] = time;

        // if (packet.event == 'gateway-callback')
        //     if (param[1])
        //         param[1]['pong_send_time'] = time;

        if (callback) {
            packet['callback'] = true;
            this.requires.gateway.emit('startCallback', id, callback);
        }

        if (ack) {
            packet['ack'] = true;
            this.requires.gateway.emit('startCallback', id + '_', ack);
        }
        const that = this;

        this.Producer.send([{ topic: topic, messages: [JSON.stringify(packet)] }], function (err, data) {
            if (err) {
                if (that.verboseMode)
                    console.error('Cluster:Producer: send message to %s failed , message: ' + JSON.stringify(packet), topic, err);
            }
            else {
                if (that.verboseMode) {
                    console.log("Cluster:Producer: sent message to %s  , message: " + JSON.stringify(packet), topic);
                }
            }
        });
    }

    createTopic(topicsArray, callback) {
        this.Producer.createTopics(topicsArray, true, function (err, data) {
            if (callback) callback(err, data)
        });
    }
}

exports = module.exports = KafkaGateway_Producer;