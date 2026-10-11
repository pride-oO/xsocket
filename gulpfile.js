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

gulp.task('default', function() {
    if(fs.readFileSync('./Helpers/index.js', 'utf8').indexOf("xSocket.VERSION = '" + version + "'") === -1){
        throw new Error('xSocket.VERSION mismatch');
    }
    return gulp.src(FileWatchArray).pipe(concat('xSocket.js')).pipe(gulp.dest('./')).pipe(concat('xSocket.min.js')).pipe(uglify()).pipe(gulp.dest('./'));
});

gulp.task('watch', function() {
    gulp.watch(FileWatchArray, gulp.series('default'));
});