import {startServer} from '@npmsoluto/soluto-kafka-grpc-target';
import CallHistory, {Call} from './models/CallHistory';
import {PORTS} from './constants';

export const callHistory = new CallHistory();

const start = () =>
    startServer(PORTS.grpc.toString(), (metadata: Record<string, unknown>) => {
        callHistory.push(metadata as Call);
    });

export default {
    start,
};
