import delay from 'delay';
import fetch from 'node-fetch';
import Server from 'simple-fake-server-server-client';

import readinessCheck from '../readinessCheck'

jest.setTimeout(180000);

const fakeHttpServer = new Server({
    baseUrl: `http:/localhost`,
    port: 3000,
});

describe('tests', () => {
    beforeAll(async () => {
        expect(readinessCheck()).resolves.toBeTruthy();
        await fetch('http://localhost:4771/clear');
        await fakeHttpServer.clear()
    });

    it('services are alive', async () => {
        let attempts = 10;
        while (attempts > 0) {
            try {
                const responseConsumer = await fetch('http://localhost:4000/isAlive');
                const responseProducer = await fetch('http://localhost:6000/isAlive');
                if (responseConsumer.ok && responseProducer.ok) {
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

    it('should produce and consume', async () => {
        await fetch('http://localhost:4771/add', {
            method: 'post',
            headers: {'content-type': 'application-json'},
            body: JSON.stringify({
                service: 'CallTarget',
                method: 'callTarget',
                input: {matches: {msgJson: '{"data":1}',}, recordTimestamp: '(.*)'},
                output: {
                    data: {
                        message: 'assertion',
                    },
                },
            }),
        });
        
        const callId = await fakeHttpServer.mock({
            url: "http://localhost:3000/fake_server_admin/calls'",
            body: {
                method: 'post',
                url: '/',
                statusCode: 200,
            },
            respondAsJson: true,
        })
       
        await fetch('http://localhost:6000/produce', {
            method: 'post',
            body: JSON.stringify([{key: 'key', message: {data: 1}}]),
            headers: {'Content-Type': 'application/json'},
        });

        await delay(5000);

        const {hasBeenMade} = await fakeHttpServer.getCall(callId)
        expect(hasBeenMade).toBeTruthy();
    });
});
