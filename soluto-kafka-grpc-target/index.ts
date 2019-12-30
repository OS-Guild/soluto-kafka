import {ProtobufMessage, Server, loadPackageDefinition, ServerCredentials} from 'grpc';
import {loadSync} from '@grpc/proto-loader';

const PROTO_PATH = __dirname + '/message.proto';

const packageDefinition = loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
});
const callTargetGrpc = loadPackageDefinition(packageDefinition).CallTarget as ProtobufMessage;

const _callTarget = run => async (call, callback) => {
    try {
        const receivedTimestamp = Date.now();
        const payload = JSON.parse(call.request.msgJson);
        await run(payload, call.request.recordOffset, call.request.recordTimestamp);
        callback(null, {statusCode: 200, receivedTimestamp, completedTimestamp: Date.now()});
    } catch (e) {
        if (e.statusCode) {
            callback(null, {statusCode: e.statusCode});
            return;
        }
        callback(null, {statusCode: 500});
    }
};

const getServer = execute => {
    const server = new Server();
    server.addService(callTargetGrpc.service, {
        callTarget: _callTarget(execute),
    });
    return server;
};

export default (port, execute) => {
    const routeServer = getServer(execute);
    routeServer.bind(`0.0.0.0:${port}`, ServerCredentials.createInsecure());
    routeServer.start();
    return routeServer;
};
