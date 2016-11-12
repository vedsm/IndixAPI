
var chokidar = require('chokidar');
var fs = require('fs');


// wathc for new csv files
module.exports = function(dataLocation){

var watcher = chokidar.watch('D:\\IndixAPI\\data', {
    persistent: true,
    ignoreInitial: true,
    followSymlinks: false,
    usePolling: true,
    depth: 0,
    interval: 100,
    ignorePermissionErrors: false
});


watcher
    .on('ready', function() { console.log('Initial scan complete. Ready for changes.'); })
    .on('error', function(err) {
        console.error('Chokidar file watcher failed. ERR: ' + err.message);
    })
    .on('add', function(path) {
        console.log('File', path, 'has been ADDED');
        if(!path.match(/\.*csv$/)){

	        fs.stat(path, function (err, stat) {

	            if (err){
	                console.error(component + 'Error watching file for copy completion. ERR: ' + err.message);
	                console.error(component + 'Error file not processed. PATH: ' + path);
	            } else {
	                console.log('File copy started...');
	                setTimeout(checkFileCopyComplete, 5*1000, path, stat);
	            }
	        });
	    }

    })
    .on('change',function(path){
    	console.log('File', path, 'has been chnaged');
    });



 function checkFileCopyComplete(path, prev) {
    fs.stat(path, function (err, stat) {

        if (err) {
            throw err;
        }
        if (stat.mtime.getTime() === prev.mtime.getTime()) {
            console.log('File copy complete => beginning processing');
            //------------------------------------- 
            // call parsing function to pars the file
            parseFileAndPopulateDb(path);
            //-------------------------------------
        }
        else {
            setTimeout(checkFileCopyComplete, 5*1000, path, stat);
        }
    });
};

};