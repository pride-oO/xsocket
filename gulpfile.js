const gulp   = require('gulp');
const concat = require('gulp-concat');
const uglify = require('gulp-uglify');
const fs     = require('fs');
const version = require('./package.json').version;

const FileWatchArray = [
    './Helpers/web.js',
    './Helpers/index.js',
    './Data/index.js',
    './Object/index.js',
    './Client/index.js',
];

gulp.task('version', function(done) {
    if(fs.readFileSync('./Helpers/index.js', 'utf8').indexOf("xSocket.VERSION = '" + version + "'") === -1){
        throw new Error('xSocket.VERSION mismatch');
    }
    if(fs.readFileSync('./python/xsocket/helpers.py', 'utf8').indexOf("VERSION = '" + version + "'") === -1){
        throw new Error('Python VERSION mismatch');
    }
    done();
});

gulp.task('build', function() {
    return gulp.src(FileWatchArray).pipe(concat('xSocket.js')).pipe(gulp.dest('./')).pipe(concat('xSocket.min.js')).pipe(uglify()).pipe(gulp.dest('./'));
});

gulp.task('esm', function() {
    return gulp.src(FileWatchArray.concat('./Helpers/esm.js')).pipe(concat('xSocket.mjs')).pipe(gulp.dest('./'));
});

gulp.task('default', gulp.series('version', 'build', 'esm'));

gulp.task('watch', function() {
    gulp.watch(FileWatchArray, gulp.series('default'));
});