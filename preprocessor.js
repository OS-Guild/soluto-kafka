const tsc = require('typescript');
const tsConfig = require('./tsconfig.json');

module.exports = {
    process: (src, path) =>
        path.endsWith('.ts') || path.endsWith('.tsx') ? tsc.transpile(src, tsConfig.compilerOptions, path, []) : src,
};
