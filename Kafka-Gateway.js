"use strict";

const EventEmitter = require('events');

class KafkaGateway extends EventEmitter {

    constructor(options) {
        super();

        const that = this;
        const kafka = require('kafka-node');
        const innerEmitter = new EventEmitter();


        this.requires = {
            kafka: kafka,
            Consumer: require('./kafka-gateway-consumer'),
            Producer: require('./kafka-gateway-producer'),
            UUID: require('uuid'),
            HighLevelConsumer: kafka.HighLevelConsumer,
            HighLevelProducer: kafka.HighLevelProducer,
            ConsumerGroup: kafka.ConsumerGroup
        };

        this.options = options;
        this.resolvers = options.resolvers;
        this.verboseMode = options.verboseMode;
        this.allRoutes = options.allRoutes;
        this.activeMode = options.activeMode;
        //declare id and name
        // this.instanceID = instanceName;

        this.DedicateName = options.zone + "." + options.server + "." + options.node + "." + options.nodeID;

        this.KafkaClient = new this.requires.kafka.KafkaClient({ kafkaHost: options.kafkaHosts });
        this.Client = new this.requires.kafka.Client(options.zookeeper);

        //internal Events
        this.callbacks = [];

        this.consumerReadyCount = 0;

        this.on('gateway-consumer-ready', function (topic) {
            if (that.verboseMode)
                console.log('Cluster:Gateway: consumer %s is READY', topic);
            that.consumerReadyCount++;
            if (that.consumerReadyCount == 2) {
                that.emit('gateway-ready');
            }
        });


        this.on('gateway-producer-ready', function (...param) {
            //Cost less for dedicate consumer when it init after createTopic
            that.Producer.createTopic([options.node, that.DedicateName], function (err, data) {
                that.DedicateConsumer = new that.requires.Consumer(that, undefined, that.DedicateName, true, that.verboseMode);
                that.Consumer = new that.requires.Consumer(that, undefined, options.node, that.activeMode, that.verboseMode);

            })
            //initial the clients

        });

        this.on('gateway-callback', function (param) {
            // console.log('gateway-callback', param);
            var id = param.splice(0, 1);
            var arg = undefined;
            // console.log('id:', id, id[0]);
            that.emit('innerCallback', id[0], (param.length > 0) ? param[0] : undefined);
        });

        this.on('sendCallback', (serviceName, param) => {
            that.Producer.send(serviceName, 'gateway-callback', param);
        });

        this.on('startCallback', (id, callback) => {
            that.callbacks[id] = callback;
            // console.log('callbacksert');
        });

        this.on('innerCallback', (id, param) => {
            // console.log([id],that.callbacks[id],param);
            if (param)
                that.callbacks[id](param);
            else
                that.callbacks[id]();

            delete that.callbacks[id];
        });


        this.on('gateway-ping', function (time, cb) {
            var now = new Date().getTime();
            time['mytimeis'] = now;
            cb(time);
        });

        //initial producer and consumers
        this.Producer = new this.requires.Producer(this, this.DedicateName, this.verboseMode);


    }

    resolveNode(resourceName) {
        return new CallChain(this, 'RES', resourceName);
    }

    node(node) {
        return new CallChain(this, 'NODE', node);
    }

    nearest(node) {

        if (!this.allRoutes) {
            throw new Error('Routes not declared.please set "allRoutes" function in options');
        }

        return new CallChain(this, 'NEAR', node);
    }

    close(cb) {
        if (this.verboseMode)
            console.log('Cluster:Gateway: waiting to close consumers..');

        this.Consumer.close(function () {
            action();
        });


        this.DedicateConsumer.close(function () {
            action();
        });

        var count = 0;
        const that = this;

        function action() {
            count++;
            if (count >= 2) {
                if (that.verboseMode)
                    console.log('Cluster:Gateway: killed service ..');
                cb();
            }

        }
    }

    ping(who, cb) {
        var time = new Date().getTime();

        this.call(who, 'gateway-ping', { pingStart: time }, function (a) {
            var now = new Date().getTime();

            var sendProccess = a.Ping_send_time - a.pingStart;
            var sent = a.ping_recieve_time - a.Ping_send_time;

            var recieveProccess = a.mytimeis - a.ping_recieve_time;
            var reciveSendProccess = a.pong_send_time - a.mytimeis;
            var recieveSent = a.pong_recieve_time - a.pong_send_time;
            var total = now - time;
            cb({
                destination: who,
                sendProccess: sendProccess,
                sent: sent,
                recieveProccess: recieveProccess,
                reciveSendProccess: reciveSendProccess,
                recieveSent: recieveSent,
                total: total
            });
        });

    }
}

class CallChain {

    constructor(gateway, type, node) {
        this.gateway = gateway;
        this.node = node;
        this.type = type;

    }

    ack(ack) {
        this.innerAck = ack;
        return this;
    }

    callback(callback) {
        this.innerCallback = callback;
        // console.log(this);
        return this;
    }

    call(event, ...param) {
        this.callWithAck(this.node, event, this.innerAck, this.innerCallback, ...param);
    }

    sendCallback(cb) {
        this.nodeMonitor = cb;
    }

    callWithAck(serviceName, event, ack, callback, ...param) {
        const that = this;
        function call(node) {
            if (that.nodeMonitor) {
                that.nodeMonitor({
                    result: true,
                    topic: node
                });
            }
            that.gateway.Producer.send(node, event, param, ack, callback);
        }

        function notFound() {
            if (that.nodeMonitor) {
                that.nodeMonitor({
                    result: false
                });
            }
        }

        if (this.type == 'RES') {
            var node = undefined;
            if (serviceName) {
                var matches = this.gateway.resolvers.filter((itm) => {
                    // console.log(resourceName, itm.prefix, (itm.prefix) ? itm.prefix.length : 0, itm.postfix, (itm.postfix) ? itm.postfix.length : 0);
                    var x1 = (itm.prefix && serviceName.length > itm.prefix.length && itm.prefix == serviceName.substring(0, itm.prefix.length)) ||
                        (itm.prefix == '*') ||
                        (itm.postfix && serviceName.length > itm.postfix.length && itm.postfix == serviceName.substring(serviceName.length - itm.postfix.length));
                    // console.error(x1, x2, x3);
                    return x1;
                });

                // console.log(matches);

                function search() {
                    if (!node && matches.length > 0) {
                        node = matches.pop().resolver(serviceName, function (targetNode) {
                            if (targetNode) {
                                call(targetNode);
                            }
                            else {
                                search();
                            }
                        });
                    }
                    else {
                        // throw new Error('Resourse "' + serviceName + '" not found.');
                        console.error('Resourse "' + serviceName + '" not found.');

                        notFound();
                        return;

                    }

                }
                search();
            }
            else {
                notFound();
            }
        }
        else if (this.type == 'NEAR') {

            var Routes = this.gateway.allRoutes();

            var sameServer = Routes.filter((itm) => itm.node == node && itm.zone == this.gateway.options.zone && itm.server == this.gateway.options.server);

            if (sameServer.length == 0) {
                var combineAll = (itm) => {
                    if (sameServer.indexOf(itm) < 0) {
                        sameServer.push(itm);
                    }
                };

                var sameZone = Routes.filter((itm) => itm.node == node && itm.zone == this.gateway.options.zone);
                sameZone.forEach(combineAll);

                if (sameServer.length == 0) {
                    var allOver = Routes.filter((itm) => itm.node == node);
                    allOver.forEach(combineAll);
                }
            }

            if (sameServer.length <= 0) {
                // throw new Error('Nearest node "' + node + '" not found.');
                console.error('Nearest node "' + node + '" not found.');
                notFound();
                return;
            }

            call(sameServer[0].zone + "." + sameServer[0].server + "." + sameServer[0].node + "." + sameServer[0].nodeID);
        }

        else if (this.type == 'NODE') {

            if (!serviceName) {
                notFound();
                return;
            }
            call(serviceName);
        }

        else {
            notFound();
        }
    }
}


exports = module.exports = KafkaGateway;