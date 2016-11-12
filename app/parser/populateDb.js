var fs = require('fs');
var parsedListFileName="./files/parsedList.csv";
var Product = require('./../models/product');
var chokidar = require('chokidar');

module.exports = function (dataLocation) {
    console.log("dataLocation->",dataLocation);

    //checks if the file exists.
    //If it does, it just calls back.
    //If it doesn't, then the file is created.
    function checkForFile(fileName,callback) {
        fs.exists(fileName, function (exists) {
            if(exists)
            {
                callback(null);
            }else
            {
                fs.writeFile(fileName, "", function (err, data)
                {
                    if(err)callback(err);
                    else callback(null);
                })
            }
        });
    };
   
    

    function  addRowToDb(dataArray,index,fileArray,fileIndex) {
        //console.log("adding row to db->",dataArray);
        //var dataRowArray = dataArray.split(",");
        console.log("Index --> ",index)
        if(index == dataArray.length){
            console.log("Done Reading file");
            fileIsRead(fileArray[fileIndex]);
            if(fileIndex + 7 < fileArray.length){
                parseFileAndPopulateDb(fileArray,fileIndex + 3);
            }
            return ;
        }
        var dataRowArray = dataArray[index];
        var pid = dataRowArray[0];
        var title = dataRowArray[1];
        var upcs = dataRowArray[2];
        var categoryld = dataRowArray[3];
        var storeld = dataRowArray[4];
        var seller = dataRowArray[5] ;
        var timestamp = -1;
        if(!isNaN(dataRowArray[6]))
            timestamp=Number(dataRowArray[6]);
        var price = -1;
        if(!isNaN(dataRowArray[7]))
            price=Number(dataRowArray[7]);

        Product.findOne({$and:[{'pid': pid},{'seller':seller}]}, function (err, product) {
            if (err) console.error("ERROR :: ", err);
            else if (!product) {
                console.log("no product found in db, creating new product");
                var newProduct = new Product();
                newProduct.pid = pid;
                newProduct.title = title;
                newProduct.upcs = upcs;
                newProduct.categoryld = categoryld;
                newProduct.storeld = storeld;
                newProduct.seller = seller;
                newProduct.timestamp = timestamp;
                newProduct.price = price;
                newProduct.save(function (err) {
                    if (err)console.error(err);
                    addRowToDb(dataArray,index+1,fileArray,fileIndex);
                });
            }
            else {
                console.log("product found in db");
                if(product.timestamp < timestamp){
                    console.log("If timestamp is greater ");
                    if(title != "")product.title = title;
                    if(upcs!="")product.upcs = upcs;
                    if(categoryld!="")product.categoryld = categoryld;
                    if(storeld!="")product.storeld = storeld;
                    if(timestamp!=-1)product.timestamp = timestamp;
                    if(price!=-1)product.price = price;
                }
                else{
                    console.log("timestamp is less than the exisiting timestamp");
                    if(product.title==""      && title!="")product.title = title;
                    if(product.upcs==""       && upcs!="")product.upcs = upcs;
                    if(product.categoryld=="" && categoryld!="")product.categoryld = categoryld;
                    if(product.storeld==""    && storeld!="")product.storeld = storeld;

                }


                product.save(function (err) {
                    if (err)console.error(err);
                    console.log("calling again dude ",index + 1);
                    addRowToDb(dataArray,index+1,fileArray,fileIndex);
                });
            }
        });
    };

    function isFileRead(filename,callback){
        fs.readFile(parsedListFileName, 'utf8', function(err, data) {
            if (err) {
                callback(err);
            }
            //console.log('OK: ' + filename);
            //console.log("data split->",data.split("\n"));
            var response = false;
            for(var i=0;i<data.split("\n").length;i++){
                if(data.split("\n")[i]==filename){
                    response=true;
                    break;
                }
            }
            callback(null,response);
        });
    };
    function fileIsRead(fileName){
        fs.readFile(parsedListFileName, 'utf8', function(err, data) {
            if (err) console.error(err);
            //console.log(data);
            var content= data+"\n"+fileName;
            fs.writeFile(parsedListFileName, content, function(err) {
                if(err) { return console.error(err);}
                console.log("The file was saved!");
            });
        });

    };

    function startReading(dataLocation){
        var files = fs.readdirSync(dataLocation);
        console.log("start reading files for the first time");
        var fileArray = [] ;
        for(var i = 0 ; i< files.length; i ++ ){
            if(files[i].match(/\.*csv$/)){
                fileArray.push(dataLocation + '/' +files[i]);
                //parseFileAndPopulateDb(dataLocation + '/' +files[i]) ;
                console.log("adding file "+files[i]);
            }
        for (var i = 0 ; i < 7 && i < fileArray.length ; i ++ ){
            console.log("reading file "+fileArray[i]);
            parseFileAndPopulateDb(fileArray,i);
        }
        }
    };



    function parseFileAndPopulateDb(fileArray,fileIndex){
        //TODO:?check if filename exists in the "readFile"
        var fileName = fileArray[fileIndex] ;
        isFileRead(fileName,function(err,response){
            if(err)console.error("error in detecting if the file was read",err);
            else if(response==true){
                console.log("file "+fileName+" was already read");
                if(fileIndex + 7 < fileArray.length){
                parseFileAndPopulateDb(fileArray,fileIndex + 3);
                }
            }
            else if(response==false){
                console.log("reading file "+fileName+" and adding it to db");
                //read file and add to db
                var csv = require('fast-csv');
                var stream = fs.createReadStream(fileName);
                var fileData = [] ;
                console.log("Adding Data into array");
                var csvStream = csv()
                    .on("data", function(data){
                        //console.log(data);
                        //addRowToDb(data);
                        fileData.push(data);
                    })
                    .on("end", function(){
                        console.log("Now starting representing file into db");
                        //fileIsRead(fileName);
                        addRowToDb(fileData,0,fileArray,fileIndex);

                    });
                stream.pipe(csvStream);
            }
        })
    }
    //parseFileAndPopulateDb(dataLocation+"test.csv");

    checkForFile(parsedListFileName,function(err) {
        if(err)console.error("error in creating file");
        console.log("initiated file parsedList.csv (if it didn't exists earlier) generated for storing which files have been parsed");
        startReading(dataLocation);
    });



    //for monitoring new files added
    var watcher = chokidar.watch(dataLocation, {
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
            if(path.match(/\.*csv$/)){

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
                var pathArray = [] ;
                pathArray.push(path);
                parseFileAndPopulateDb(pathArray,0);
                //-------------------------------------
            }
            else {
                setTimeout(checkFileCopyComplete, 5*1000, path, stat);
            }
        });
    };
};