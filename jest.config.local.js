module.exports = {
    preset: 'ts-jest',
    testEnvironment: 'node',
    rootDir: '.',
    testMatch: ['<rootDir>/__tests__/specs/**'],
    globals: {
        'ts-jest': {
            tsConfig: {
                strictPropertyInitialization: false,
                noUnusedLocals: false,
            },
        },
    },
};
