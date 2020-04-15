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
    });

    beforeEach(async () => {
        await fetch('http://localhost:4771/clear');
        await fakeHttpServer.clear();
    });

    it('services are alive', async () => {
        let attempts = 10;
        while (attempts > 0) {
            try {
                const consumer1 = await fetch('http://localhost:4000/isAlive');
                const consumer2 = await fetch('http://localhost:5000/isAlive');
                const errorConsumer = await fetch('http://localhost:8000/isAlive');
                const producer = await fetch('http://localhost:6000/isAlive');
                if (consumer1.ok && consumer2.ok && errorConsumer.ok && producer.ok) {
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
        await mockGrpcTarget();
        const callId = await mockHttpTarget();

        await produce('http://localhost:6000/produce', 'test');
        await produce('http://localhost:6000/produce', 'another_test');
        await delay(5000);

        const {madeCalls} = await fakeHttpServer.getCall(callId);
        expect(madeCalls.length).toBe(2);
        expect(madeCalls[0].headers['x-record-topic']).toBe('test');
        expect(madeCalls[1].headers['x-record-topic']).toBe('another_test');

        const consumerWithUnresponsiveTarget = await fetch('http://localhost:8000/isAlive');
        expect(consumerWithUnresponsiveTarget.status).toBe(500);
    });

    it('producer request validation', async () => {
        const method = 'post';
        const producerUrl = 'http://localhost:6000/produce';
        const headers = {'Content-Type': 'application/json'};
        let response;

        response = await fetch(producerUrl, {
            method,
            body: JSON.stringify([{key: 'key', value: {data: 1}}]),
            headers,
        });
        expect(response.status).toBe(400);
        expect(await response.text()).toBe('topic is missing');

        response = await fetch(producerUrl, {
            method,
            body: JSON.stringify([{topic: 'test', value: {data: 1}}]),
            headers,
        });
        expect(response.status).toBe(400);
        expect(await response.text()).toBe('key is missing');

        response = await fetch(producerUrl, {
            method,
            body: JSON.stringify([{topic: 'test', key: 'key'}]),
            headers,
        });
        expect(response.status).toBe(400);
        expect(await response.text()).toBe('value is missing');
    });
});

const produce = (url: string, topic: string) =>
    fetch(url, {
        method: 'post',
        body: JSON.stringify([{topic, key: 'key', message: {data: 1}}]),
        headers: {'Content-Type': 'application/json'},
    });

const mockHttpTarget = () =>
    fakeHttpServer.mock({
        method: 'post',
        url: '/consume',
    });

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
