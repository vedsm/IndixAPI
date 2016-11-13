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

    //recursive function to add rows to mongod
    function addRowsToDb(dataArray,rowIndex,callback){
        if(rowIndex>=dataArray.length){
            callback(null);
        }
        else{
            //console.log("adding row to db->",dataArray);
            //var dataRowArray = dataArray.split(",");
            console.log("reading row number->",rowIndex);           //LOG
            //console.log("which has value",dataArray[rowIndex]);
            var dataRowArray = dataArray[rowIndex];
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
                    //console.log("no product found in db, creating new product");    //LOG
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
                        if (err){callback(err);console.error(err);}
                        else{
                            addRowsToDb(dataArray,rowIndex+1,function(err){
                                if(err)callback(err);
                                else callback(null);
                            })
                        }
                    });
                }
                else {
                    //console.log("product found in db");            //LOG
                    if(product.timestamp < timestamp){
                        //console.log("If timestamp is greater ");     //LOG
                        if(title != "")product.title = title;
                        if(upcs!="")product.upcs = upcs;
                        if(categoryld!="")product.categoryld = categoryld;
                        if(storeld!="")product.storeld = storeld;
                        if(timestamp!=-1)product.timestamp = timestamp;
                        if(price!=-1)product.price = price;
                    }
                    else{
                        //console.log("timestamp is less than the exisiting timestamp");     //LOG
                        if(product.title==""      && title!="")product.title = title;
                        if(product.upcs==""       && upcs!="")product.upcs = upcs;
                        if(product.categoryld=="" && categoryld!="")product.categoryld = categoryld;
                        if(product.storeld==""    && storeld!="")product.storeld = storeld;

                    }


                    product.save(function (err) {
                        if(err)callback(err);
                        else{
                            addRowsToDb(dataArray,rowIndex+1,function(err){
                                if(err)callback(err);
                                else callback(null);
                            })
                        }
                    });
                }
            });
        }
    }

    /*function  addRowToDb(dataArray) {
        //console.log("adding row to db->",dataArray);
        //var dataRowArray = dataArray.split(",");
        var dataRowArray = dataArray;
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
                //console.log("no product found in db, creating new product");
                var newProduct = new Product();
                newProduct.pid = pid;
                newProduct.title = title;
                newProduct.upcs = upcs;
                newProduct.categoryld = categoryld;
                newProduct.storeld = storeld;
                newProduct.seller = seller;
                newProduct.timestamp = timestamp;
                newProduct.price = price;
                newProduct.save(function (err) {if (err)console.error(err);});
            }
            else {
                //console.log("product found in db");
                if(product.timestamp < timestamp){
                    //console.log("If timestamp is greater ");
                    if(title != "")product.title = title;
                    if(upcs!="")product.upcs = upcs;
                    if(categoryld!="")product.categoryld = categoryld;
                    if(storeld!="")product.storeld = storeld;
                    if(timestamp!=-1)product.timestamp = timestamp;
                    if(price!=-1)product.price = price;
                }
                else{
                    //console.log("timestamp is less than the exisiting timestamp");
                    if(product.title==""      && title!="")product.title = title;
                    if(product.upcs==""       && upcs!="")product.upcs = upcs;
                    if(product.categoryld=="" && categoryld!="")product.categoryld = categoryld;
                    if(product.storeld==""    && storeld!="")product.storeld = storeld;

                }


                product.save(function (err) {if (err)console.error(err);});
            }
        });
    };*/

    //to check if the given file is already read by comparing it with list of files in parsedListFileName
    function isFileRead(filename,callback){
        fs.readFile(parsedListFileName, 'utf8', function(err, data) {
            if (err) {
                callback(err);
            }
            else {
                //console.log('OK: ' + filename);
                //console.log("data split->",data.split("\n"));
                var response = false;
                for (var i = 0; i < data.split("\n").length; i++) {
                    if (data.split("\n")[i] == filename) {
                        response = true;
                        break;
                    }
                }
                callback(null, response);
            }
        });
    };

    //adds the file to parsedListFileName when parsing of it is finished
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

    //reads all files in the folder
    function startReading(dir){
        var files = fs.readdirSync(dir);
        var filesArray=[];
        for(var i = 0 ; i< files.length ; i ++ ){
            if(files[i].match(/\.*csv$/)){
                //parseFileAndPopulateDb(dataLocation+files[i]) ;
                //console.log("reading file "+files[i]);
                filesArray.push(dataLocation+files[i]);
            }
        }
        console.log("reading files",filesArray);
        var batchSize=7;
        for(var i=0;i<filesArray.length/batchSize;i++){
            parseFilesAndPopulateDb(filesArray.slice(i*batchSize,i*batchSize+batchSize),0,function(err){
                if(err)console.error("error in reading files",err);
                else{
                    console.log("read all files successfully");
                }
            });
        }
    };

    //recursive function which takes an array of files and reads them in sync
    function parseFilesAndPopulateDb(filesArray,fileIndex,callback){
        if(fileIndex>=filesArray.length){
            callback(null);
        }
        else{
            var currFile=filesArray[fileIndex];
            isFileRead(currFile,function(err,response){
                if(err){callback(err);console.error("error in detecting if the file was read",err);}
                else if(response==true) {
                    console.log("file " + currFile + " was already read");
                    parseFilesAndPopulateDb(filesArray,fileIndex+1,function(err){
                        if(err){callback(err);console.error("error in reading files",err);}
                        else{
                            console.log("read file successfully");
                            callback(null);
                        }
                    });
                }
                else if(response==false){
                    console.log("reading file "+currFile+" and adding it to db");
                    //read file and add to db
                    var csv = require('fast-csv');
                    var stream = fs.createReadStream(filesArray[fileIndex]);
                    var dataArray=[];
                    var csvStream = csv()
                        .on("data", function(data){
                            //console.log(data);
                            dataArray.push(data);
                            //addRowToDb(data);
                        })
                        .on("end", function(){
                            console.log("done");
                            addRowsToDb(dataArray,0,function(err){
                                if(err){callback(err);console.error("error in adding all rows of a file to db",err);}
                                else{
                                    console.log("added file to db");
                                    fileIsRead(currFile);
                                    parseFilesAndPopulateDb(filesArray,fileIndex+1,function(err){
                                        if(err){callback(err);console.error("error in reading files",err);}
                                        else{
                                            console.log("read file successfully");
                                            callback(null);
                                        }
                                    });
                                }
                            });
                        });
                    stream.pipe(csvStream);
                }
            })
        }
    };

    /*//TODO:TESTING
    console.log("reading a file");
    parseFilesAndPopulateDb([dataLocation+"xai.csv"],0,function(err){
        if(err)console.error("error in reading file"+dataLocation+"xaa.csv",err);
        else{
            console.log("file:"+dataLocation+"xaa.csv read successfully");
        }
    });
    parseFilesAndPopulateDb([dataLocation+"xaw.csv"],0,function(err){
        if(err)console.error("error in reading file"+dataLocation+"xaa.csv",err);
        else{
            console.log("file:"+dataLocation+"xaa.csv read successfully");
        }
    });
    parseFilesAndPopulateDb([dataLocation+"xaq.csv"],0,function(err){
        if(err)console.error("error in reading file"+dataLocation+"xaa.csv",err);
        else{
            console.log("file:"+dataLocation+"xaa.csv read successfully");
        }
    });*/



    /*function parseFileAndPopulateDb(fileName){
        //check if filename exists in the "readFile"
        isFileRead(fileName,function(err,response){
            if(err)console.error("error in detecting if the file was read",err);
            else if(response==true)
                console.log("file "+fileName+" was already read");
            else if(response==false){
                console.log("reading file "+fileName+" and adding it to db");
                //read file and add to db
                var csv = require('fast-csv');
                var stream = fs.createReadStream(fileName);
                var dataArray=[];
                var csvStream = csv()
                    .on("data", function(data){
                        //console.log(data);
                        dataArray.push(data);
                        //addRowToDb(data);
                    })
                    .on("end", function(){
                        console.log("done");
                        addRowsToDb(dataArray,0,function(err){
                            if(err){console.error("error in adding all rows of a file to db",err);}
                            else{
                                console.log("added file to db");
                                fileIsRead(fileName);
                                console.log("read file successfully");
                            }
                        });
                    });
                stream.pipe(csvStream);
            }
        })
    }*/
    //parseFileAndPopulateDb(dataLocation+"xaa.csv");

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
                //parseFileAndPopulateDb(path);
                parseFilesAndPopulateDb([path],0,function(err){
                    if(err)console.error("error in reading file "+path,err);
                    else{
                        console.log("file:"+path+" read successfully");
                    }
                });
                //-------------------------------------
            }
            else {
                setTimeout(checkFileCopyComplete, 5*1000, path, stat);
            }
        });
    };




};