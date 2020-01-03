import delay from 'delay';
import fetch from 'node-fetch';
import Server from 'simple-fake-server-server-client';

import readinessCheck from '../readinessCheck';

jest.setTimeout(180000);

const fakeHttpServer = new Server({
    baseUrl: `http://localhost`,
    port: 3000,
});

describe('tests', () => {
    beforeAll(async () => {
        await expect(readinessCheck()).resolves.toBeTruthy();
        await fetch('http://localhost:4771/clear');
        await fakeHttpServer.clear();
    });

    it('services are alive', async () => {
        let attempts = 10;
        while (attempts > 0) {
            try {
                const responseConsumer = await fetch('http://localhost:4000/isAlive');
                const responseProducer = await fetch('http://localhost:6000/isAlive');
                const responseRetryProducer = await fetch('http://localhost:7000/isAlive');
                if (responseConsumer.ok && responseProducer.ok && responseRetryProducer.ok) {
                    return;
                }
                attempts--;
            } catch (e) {
                console.log('not alive');
                attempts--;
            }
            await delay(10000);
        }
        fail();
    });

    it('test topic - should produce and consume', async () => {
        await mockGrpcTarget();
        const callId = await mockHttpTarget();
        
        await produce('http://localhost:6000/produce');
        await delay(5000);

        const {hasBeenMade} = await fakeHttpServer.getCall(callId);
        expect(hasBeenMade).toBeTruthy();
    });

    it('test retry topic - should produce and consume', async () => {
        await mockGrpcTarget();
        const callId = await mockHttpTarget();

        await produce('http://localhost:7000/produce');
        await delay(20000);

        const {hasBeenMade} = await fakeHttpServer.getCall(callId);
        expect(hasBeenMade).toBeTruthy();
    });    
});

const produce = (url: string) => fetch(url, {
    method: 'post',
    body: JSON.stringify([{key: 'key', message: {data: 1}}]),
    headers: {'Content-Type': 'application/json'},
})

const mockHttpTarget = () => fakeHttpServer.mock({
    method: 'post',
    url: '/consume',
    statusCode: 200,
})

const mockGrpcTarget = () => 
     fetch('http://localhost:4771/add', {
        method: 'post',
        headers: {'content-type': 'application-json'},
        body: JSON.stringify({
            service: 'CallTarget',
            method: 'callTarget',
            input: {matches: {msgJson: '{"data":1}'}, recordTimestamp: '(.*)'},
            output: {
                data: {
                    message: 'assertion',
                },
            },
        }),
    });
