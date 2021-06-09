import {PORTS} from './constants';
import * as express from 'express';
import {Request, Response} from 'express';
import gRPCServer, {callHistory} from './gRPCServer';

const app = express();

app.listen(PORTS.express, () => {
    gRPCServer.start();
});

app.get('/getCallHistory', (req: Request, res: Response) => {
    res.send(callHistory);
});

app.get('/clear', (req: Request, res: Response) => {
    callHistory.clear();
    res.send('Call history cleared');
});
