const fs = require('fs-extra');
const path = require('path')

const node_version  = parseInt(Number(process.version.match(/^v(\d+\.\d+)/)[1]));  
const addon_path = (node_version === 6) ? path.resolve('lib', 'addon_6') : path.resolve('lib', 'addon_8') // path.resolve starts with current folder

fs.copy(addon_path, path.resolve('lib', 'addon'), function (err) {
    if (err) throw err;
    console.log('renamed complete');
});
//deleteFolderRecursive(HOME_DIR+'../lib/addon_8');


// Not currently used, not cross platform since resolves paths with + "/" ..
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




