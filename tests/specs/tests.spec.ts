import delay from 'delay';
import fetch from 'node-fetch';
import Server from 'simple-fake-server-server-client';
import {range} from 'lodash';

import checkReadiness from '../checkReadiness';
import * as uuid from 'uuid';
import {PORTS} from '../fakeGRPCServer/constants';

jest.setTimeout(180000);

const fakeHttpServer = new Server({
    baseUrl: `http://localhost`,
    port: 3000,
});

describe('tests', () => {
    beforeAll(async () => {
        await expect(checkReadiness(['foo', 'bar', 'retry', 'dead-letter', 'unexpected'])).resolves.toBeTruthy();
    });

    beforeEach(async () => {
        await fakeHttpServer.clear();
    });

    it('liveliness', async () => {
        await delay(1000);
        const producer = await fetch('http://localhost:6000/isAlive');
        const consumer = await fetch('http://localhost:4001/isAlive');
        expect(producer.ok).toBeTruthy();
        expect(consumer.ok).toBeTruthy();
    });

    it('should produce and consume', async () => {
        const callId = await mockHttpTarget('/consume', 200);

        await produce('http://localhost:6000/produce', [
            {
                topic: 'foo',
                key: 'thekey',
                value: {data: 'foo'},
                headers: {eventType: 'test1', source: 'test-service1', nullHeader: null},
            },
        ]);
        await delay(1000);
        await produce('http://localhost:6000/produce', [
            {
                topic: 'bar',
                key: 'thekey',
                value: {data: 'bar'},
                headers: {eventType: 'test2', source: 'test-service2'},
            },
        ]);
        await delay(1000);

        const {hasBeenMade, madeCalls} = await fakeHttpServer.getCall(callId);
        expect(hasBeenMade).toBeTruthy();
        expect(madeCalls.length).toBe(2);
        const actualHeaders1 = JSON.parse(madeCalls[0].headers['x-record-headers']);
        const actualHeaders2 = JSON.parse(madeCalls[1].headers['x-record-headers']);
        expect(madeCalls[0].headers['x-record-topic']).toBe('foo');
        expect(actualHeaders1!.eventType).toEqual('test1');
        expect(actualHeaders1!.source).toEqual('test-service1');
        expect(actualHeaders1!.nullHeader).toEqual(null);
        expect(madeCalls[1].headers['x-record-topic']).toBe('bar');
        expect(actualHeaders2!.eventType).toEqual('test2');
        expect(actualHeaders2!.source).toEqual('test-service2');
        expect(parseInt(madeCalls[0].headers['x-record-timestamp'])).not.toBe(NaN);
        expect(parseInt(madeCalls[1].headers['x-record-timestamp'])).not.toBe(NaN);
    });

    it('should consume bursts of records', async () => {
        const callId = await mockHttpTarget('/consume', 200);

        const recordsCount = 1000;
        const records = range(recordsCount).map(() => ({topic: 'foo', key: uuid(), value: {data: 'foo'}}));

        await produce('http://localhost:6000/produce', records);
        await delay(recordsCount * 10);

        const {madeCalls} = await fakeHttpServer.getCall(callId);
        expect(madeCalls.length).toBe(recordsCount);
    });

    it('consumer should produce to dead letter topic when target response is 400', async () => {
        await mockHttpTarget('/consume', 400);
        const callId = await mockHttpTarget('/deadLetter', 200);

        await produce('http://localhost:6000/produce', [{topic: 'foo', key: uuid(), value: {data: 'foo'}}]);
        await delay(5000);

        const {hasBeenMade, madeCalls} = await fakeHttpServer.getCall(callId);
        expect(hasBeenMade).toBeTruthy();
        expect(madeCalls[0].headers['x-record-original-topic']).toEqual('foo');
    });

    it('consumer should produce to retry topic when target response is 500', async () => {
        await mockHttpTarget('/consume', 500);
        const callId = await mockHttpTarget('/retry', 200);

        await produce('http://localhost:6000/produce', [{topic: 'foo', key: uuid(), value: {data: 'foo'}}]);
        await delay(5000);

        const {hasBeenMade, madeCalls} = await fakeHttpServer.getCall(callId);
        expect(hasBeenMade).toBeTruthy();
        expect(madeCalls[0].headers['x-record-original-topic']).toEqual('foo');
    });

    it('consumer should terminate on an unexpected error', async () => {
        await delay(1000);
        let consumerLiveliness = await fetch('http://localhost:4002/isAlive');
        expect(consumerLiveliness.ok).toBeTruthy();

        await produce('http://localhost:6000/produce', [
            {topic: 'unexpected', key: uuid(), value: {data: 'unexpected'}},
        ]);
        await delay(10000);

        consumerLiveliness = await fetch('http://localhost:4002/isAlive');
        expect(consumerLiveliness.ok).toBeFalsy();
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
            body: JSON.stringify([{topic: 'bar', value: {data: 1}}]),
            headers,
        });
        expect(response.status).toBe(400);
        expect(await response.text()).toBe('key is missing');

        response = await fetch(producerUrl, {
            method,
            body: JSON.stringify([{topic: 'bar', key: 'key'}]),
            headers,
        });
        expect(response.status).toBe(400);
        expect(await response.text()).toBe('value is missing');
    });
});

describe('gRPC tests', () => {
    const FAKE_GRPC_SERVER_URL = `http://localhost:${PORTS.express}`;

    beforeEach(async () => {
        await fetch(`${FAKE_GRPC_SERVER_URL}/clear`);
    });

    it('should pass all data and headers', async () => {
        const topic = 'bar';
        const payload = {data: 'bar'};

        await produce('http://localhost:6000/produce', [
            {
                topic,
                key: 'thekey',
                value: payload,
            },
        ]);

        await delay(1000);

        const fakeGRPCResponse = await fetch(`${FAKE_GRPC_SERVER_URL}/getCallHistory`);
        const {
            calls: [call],
        } = await fakeGRPCResponse.json();

        expect(call?.headers?.recordOffset).toBeDefined();
        expect(typeof call?.headers?.recordOffset).toBe('number');
        expect(call?.headers?.recordTimestamp).toBeDefined();
        expect(typeof call?.headers?.recordTimestamp).toBe('number');
        expect(call?.headers?.topic).toBe(topic);
        expect(call?.payload).toMatchObject(payload);
    });
});

describe.only('message order tests', () => {
    const FAKE_GRPC_SERVER_URL = `http://localhost:${PORTS.express}`;

    beforeEach(async () => {
        await fetch(`${FAKE_GRPC_SERVER_URL}/clear`);
    });

    it('should receive calls in same order they were produced', async () => {
        const topic = 'foo';
        const payload = {data: 'bar'};
        const numberOfMessages = 10;
        const delayMillis = 3000;
        let index = 0;

        while (index < numberOfMessages) {
            await produce('http://localhost:6000/produce', [
                {
                    topic,
                    key: 'samePartitioningKey',
                    headers: {
                        executionDelay: index % 2 > 0 ? null : `${delayMillis}`,
                    },
                    value: {
                        index,
                        ...payload,
                    },
                },
            ]);
            index++;
        }

        await delay(numberOfMessages * delayMillis);

        const fakeGRPCResponse = await fetch(`${FAKE_GRPC_SERVER_URL}/getCallHistory`);
        const {calls} = await fakeGRPCResponse.json();
        expect(calls.length).toBe(numberOfMessages);
        index = 0;
        while (index < numberOfMessages) {
            expect(calls[index].payload.index).toBe(index);
            index++;
        }
    });
});

const produce = (url: string, batch: any[]) =>
    fetch(url, {
        method: 'post',
        body: JSON.stringify(batch),
        headers: {'Content-Type': 'application/json'},
    });

const mockHttpTarget = (route: string, statusCode: number) =>
    fakeHttpServer.mock({
        method: 'post',
        url: route,
        statusCode,
    });
