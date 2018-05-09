#!/usr/bin/env node
var fs = require('fs');

var HOME_DIR =  __dirname + '/';
const version  = parseInt(Number(process.version.match(/^v(\d+\.\d+)/)[1]));

var deleteFolderRecursive = function(path) {
    if (fs.existsSync(path)) {
        fs.readdirSync(path).forEach(function(file, index){
            var curPath = path + "/" + file;
            if (fs.lstatSync(curPath).isDirectory()) { // recurse
                deleteFolderRecursive(curPath);
            } else { // delete file
                fs.unlinkSync(curPath);
            }
        });
        fs.rmdirSync(path);
    }
};


if (version === 6) {
    fs.rename(HOME_DIR+'../lib/addon_6', HOME_DIR+'../lib/addon', function (err) {
        if (err) throw err;
        console.log('renamed complete');
    });
    deleteFolderRecursive(HOME_DIR+'../lib/addon_8');
} else {
    fs.rename(HOME_DIR+'../lib/addon_8', HOME_DIR+'../lib/addon', function (err) {
        if (err) throw err;
        console.log('renamed complete');
    });
    deleteFolderRecursive(HOME_DIR+'../lib/addon_6');
}

