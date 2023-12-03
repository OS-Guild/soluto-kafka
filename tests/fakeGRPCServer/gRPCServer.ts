import {startServer} from '@npmsoluto/soluto-kafka-grpc-target';
import CallHistory, {Call} from './models/CallHistory';
import {PORTS} from './constants';
import delay from 'delay';

export const callHistory = new CallHistory();

const start = () =>
    startServer(PORTS.grpc.toString(), async (metadata: Record<string, unknown>) => {
        const call = metadata as Call;
        const delayMillis = call?.headers?.recordHeaders?.executionDelay;
        if (delayMillis) {
            console.log(`delaying grpc call with: ${delayMillis}ms`);
            await delay(delayMillis);
        }
        callHistory.push(metadata as Call);
    });

export default {
    start,
};
