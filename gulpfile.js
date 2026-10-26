const gulp     = require('gulp');
const fs       = require('fs');
const rollup   = require('rollup');
const babel    = require('@rollup/plugin-babel').babel;
const resolve  = require('@rollup/plugin-node-resolve').nodeResolve;
const inject   = require('@rollup/plugin-inject');
const path     = require('path');
const terser   = require('@rollup/plugin-terser');
const version  = require('./package.json').version;

const banner = `/**
 * xSocket v${version}
 * @licence MIT
 * @author Vadim Pride (pride.mk.ua@gmail.com)
 * @link https://github.com/pride-oO/xsocket
 */`;

const legacyTargetList = ['ie >= 10', 'safari >= 6', 'ios_saf >= 6', 'android >= 4.4', 'defaults'];

const legacyPluginList = [
    resolve(),
    inject({
        'include' : './src/**/*.js',
        'exclude' : './src/legacy/**',
        'Promise' : path.resolve('./src/legacy/Promise.js')
    }),
    babel({
        'babelHelpers' : 'bundled',
        'babelrc' : false,
        'configFile' : false,
        'presets' : [['@babel/preset-env', {'targets' : legacyTargetList, 'loose' : true, 'modules' : false}]]
    })
];

const build = async (input, pluginList, outputList, externalList) => {
    const bundle = await rollup.rollup({'input' : input, 'plugins' : pluginList, 'external' : externalList || []});
    for(const output of outputList){
        await bundle.write(Object.assign({'banner' : banner}, output));
    }
    await bundle.close();
};

gulp.task('version', (done) => {
    if(fs.readFileSync('./src/xSocket.js', 'utf8').indexOf("xSocket.VERSION   = '" + version + "'") === -1){
        throw new Error('xSocket.VERSION mismatch');
    }
    if(fs.readFileSync('./python/xsocket/helpers.py', 'utf8').indexOf("VERSION = '" + version + "'") === -1){
        throw new Error('Python VERSION mismatch');
    }
    done();
});

gulp.task('node', () => build('./src/node.js', [], [
    {'file' : './index.js', 'format' : 'cjs', 'exports' : 'default'}
], ['ws', 'events', 'http', 'https', 'fs', 'crypto']));

gulp.task('esm', () => build('./src/index.js', [], [
    {'file' : './xSocket.mjs', 'format' : 'es'},
    {'file' : './xSocket.min.mjs', 'format' : 'es', 'plugins' : [terser({'format' : {'comments' : /^!|@licence/}})]}
]));

gulp.task('legacy', () => build('./src/global.js', legacyPluginList, [
    {'file' : './xSocket.js', 'format' : 'umd', 'name' : 'xSocket', 'exports' : 'default'},
    {'file' : './xSocket.min.js', 'format' : 'umd', 'name' : 'xSocket', 'exports' : 'default', 'plugins' : [terser({'ecma' : 5, 'safari10' : true, 'format' : {'comments' : /^!|@licence/}})]}
]));

gulp.task('default', gulp.series('version', gulp.parallel('node', 'esm', 'legacy')));

gulp.task('watch', () => {
    gulp.watch('./src/**/*.js', gulp.series('default'));
});
