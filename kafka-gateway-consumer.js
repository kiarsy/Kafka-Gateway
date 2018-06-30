"use strict";

class KafkaGateway_Consumer {
    constructor(gateway, groupName, topic, activeMode, verboseMode) {
        this.requires = {
            gateway: gateway
        };
        const that = this;
        this.verboseMode = verboseMode;
        this.activeMode = activeMode;
        this.lastOffset = []
        this.proccessedOffset = [];
        var options = {
            autoCommit: true,
            autoCommitIntervalMs: 500,
            fromOffset: activeMode
        };
        if (groupName)
            options['groupId'] = groupName;

        if (this.activeMode) {
            var offset = new this.requires.gateway.requires.kafka.Offset(this.requires.gateway.Client);
            var partition = 3;

            offset.fetchLatestOffsets([topic], function (error, offsets) {
                if (error) {
                    console.error('fetchLatestOffsets', error);
                    startConsumer(that);
                    return;
                }
                console.log(offsets);
                for (var index = 0; index < partition; index++) {
                    var offsetNumber = offsets[topic][index];
                    if (offsetNumber) {
                        that.lastOffset[index] = offsetNumber;
                    }
                }
                // console.log('offset of "%s" is %s', topic, offsetNumber,offsets);
                startConsumer(that);
            });

        }
        else {
            startConsumer(that);
        }

        function startConsumer(that) {

            var kafkaHost = that.requires.gateway.options.kafkaHosts;
            var zookeeper = that.requires.gateway.options.zookeeper;
            console.log(kafkaHost, zookeeper);

            var options = {
                host: zookeeper,  // zookeeper host omit if connecting directly to broker (see kafkaHost below)
                kafkaHost: kafkaHost, // connect directly to kafka broker (instantiates a KafkaClient)
                zk: undefined,   // put client zk settings if you need them (see Client)
                batch: undefined, // put client batch settings if you need them (see Client)
                ssl: true, // optional (defaults to false) or tls options hash
                groupId: 'ExampleTestGroup',
                sessionTimeout: 15000,
                // An array of partition assignment protocols ordered by preference.
                // 'roundrobin' or 'range' string for built ins (see below to pass in custom assignment protocol)
                protocol: ['roundrobin'],

                // Offsets to use for new groups other options could be 'earliest' or 'none' (none will emit an error if no offsets were saved)
                // equivalent to Java client's auto.offset.reset
                fromOffset: 'latest', // default
                commitOffsetsOnFirstJoin: true, // on the very first time this consumer group subscribes to a topic, record the offset returned in fromOffset (latest/earliest)
                // how to recover from OutOfRangeOffset error (where save offset is past server retention) accepts same value as fromOffset
                outOfRangeOffset: 'earliest', // default
                migrateHLC: false,    // for details please see Migration section below
                migrateRolling: true,
                // Callback to allow consumers with autoCommit false a chance to commit before a rebalance finishes
                // isAlreadyMember will be false on the first connection, and true on rebalances triggered after that
                onRebalance: (isAlreadyMember, callback) => { callback(); } // or null
            };

            //   var consumerGroup = new ConsumerGroup(options, ['RebalanceTopic', 'RebalanceTest']);

            // Or for a single topic pass in a string

            that.consumer = new that.requires.gateway.requires.ConsumerGroup(options, topic);


            // that.consumer = new that.requires.gateway.requires.HighLevelConsumer(
            //     that.requires.gateway.Client,
            //     [{ topic: topic }],
            //     options
            // );

            that.consumer.on('error', function (err) {
                // if (err.name === 'TopicsNotExistError') { }
                console.log('consumer.on(error', topic, err);
                gateway.emit('gateway-callback-error', err);
            });

            that.consumer.on('message', function (message) {
                try {

                    //check Duplicate Message
                    if (!that.proccessedOffset[message.partition] || message.offset > that.proccessedOffset[message.partition]) {
                        that.proccessedOffset[message.partition] = message.offset;
                    }
                    else {
                        if (that.verboseMode) {
                            console.error("Cluster:Consumer: recieved Duplicate , message :", JSON.stringify(message));
                        }

                        return;
                    }

                    //check Old message
                    if (message.offset < that.lastOffset[message.partition] && that.activeMode) {
                        if (that.verboseMode) {
                            console.error("Cluster:Consumer: recieved message is old, message :", JSON.stringify(message), that.lastOffset[message.partition]);
                        }
                        return;
                    }

                    //Message
                    if (that.verboseMode) {
                        console.error("Cluster:Consumer: recieved message, message :", JSON.stringify(message));
                    }

                    var time = new Date().getTime();
                    // console.log('message:', time);

                    var packet = JSON.parse(message.value);

                    if (packet.ack) {
                        gateway.emit('sendCallback', packet.sender, [packet.id + "_", packet.id]);
                    }

                    // console.log("recieve:",packet);
                    //create callback
                    if (packet.event == 'gateway-ping')
                        packet.param[0]['ping_recieve_time'] = time;

                    // else if (packet.event == 'gateway-callback')
                    //     if (packet.param[1])
                    //         packet.param[1]['pong_recieve_time'] = time;

                    var callback = undefined;
                    if (packet.callback) {
                        callback = function (...args) {
                            var param = [];
                            param.push(packet.id);
                            if (args && args.length > 0) {
                                for (var index in args) {
                                    param.push(args[index]);
                                }
                                // while (args.length > 0) {
                                //     param.push(args.pop());
                                // }
                            }

                            gateway.emit('sendCallback', packet.sender, param);
                        };
                    }
                    // console.log("recieve:",packet);

                    //execute emitter
                    try {
                        // console.log('consumer:', topic, packet);
                        var param = undefined;
                        param = (packet.param.length == 1) ? packet.param[0] : undefined
                        param = (packet.param.length > 1) ? packet.param : param
                        // param = packet.param;
                        if (packet.event == 'gateway-callback') {
                            param = packet.param;
                        }

                        var time2 = new Date().getTime();
                        // console.log('message:', time2 - packet.stamp, time2);
                        // console.log(param);
                        gateway.emit(packet.event, param, callback);
                        // this.consumer.commit();
                    }
                    catch (e) {
                        console.error(e);
                    }
                } catch (e) { console.error(e); }

            });

            gateway.emit('gateway-consumer-ready', topic);
        }
    }

    close(cb) {
        this.consumer.close(true, function () {
            cb();
        });
    }
}

exports = module.exports = KafkaGateway_Consumer;