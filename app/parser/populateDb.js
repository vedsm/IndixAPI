var fs = require('fs');
var parsedListFileName="./files/parsedList.csv";
var Product = require('./../models/product');

module.exports = function (dataLocation) {
    console.log("dataLocation->",dataLocation);

    //checks if the file exists.
    //If it does, it just calls back.
    //If it doesn't, then the file is created.
    function checkForFile(fileName,callback)
    {
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

    function getFilesRead(filename,callback){
        fs.readFile(filename, 'utf8', function(err, data) {
            if (err) {
                console.error(err);
            }
            //console.log('OK: ' + filename);
            //console.log("data split->",data.split("\n"));
            callback(data.split("\n"));
        });
    };

    function getAllFilesInFolder(dataLocation,callback){
        //TODO:complete this
        callback(["a","b","abc"]);
    };

    function arr_diff (a1, a2) {
        var a = [], diff = [];
        for (var i = 0; i < a1.length; i++) {
            a[a1[i]] = true;
        }
        for (var i = 0; i < a2.length; i++) {
            if (a[a2[i]]) {
                delete a[a2[i]];
            } else {
                a[a2[i]] = true;
            }
        }
        for (var k in a) {
            diff.push(k);
        }
        return diff;
    };

    function  addRowToDb(dataArray) {
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
                newProduct.save(function (err) {if (err)console.error(err);});
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


                product.save(function (err) {if (err)console.error(err);});
            }
        });
    }

    function addRowsToDb(dataArray,currIndex){
        console.log("current row number->"+currIndex);
        if(currIndex==dataArray.length) return;
        var dataRowArray = dataArray[currIndex].split(",");
        var pid = dataRowArray[0];
        Product.findOne({'pid': pid}, function (err, product) {
            if (err) console.error("ERROR :: ", err);
            else if (!product) {
                console.log("no product found in db, creating new product");
                var newProduct = new Product();
                newProduct.pid = pid;
                newProduct.title = dataRowArray[1];
                newProduct.upcs = dataRowArray[2];
                newProduct.categoryld = dataRowArray[3];
                newProduct.storeld = dataRowArray[4];
                newProduct.seller = dataRowArray[5];
                newProduct.timestamp = dataRowArray[6];
                newProduct.price = dataRowArray[7];
                newProduct.save(function (err) {
                    if (err){console.error(err);}
                    else addRowsToDb(dataArray,currIndex+1)
                });
            }
            else {
                console.log("product found in db");
                if(product.title==""      && dataRowArray[1]!="")product.title = dataRowArray[1];
                if(product.upcs==""       && dataRowArray[2]!="")product.upcs = dataRowArray[2];
                if(product.categoryld=="" && dataRowArray[3]!="")product.categoryld = dataRowArray[3];
                if(product.storeld==""    && dataRowArray[4]!="")product.storeld = dataRowArray[4];
                if(product.seller==""     && dataRowArray[5]!="")product.seller = dataRowArray[5];
                if(product.timestamp==""  && dataRowArray[6]!="")product.timestamp = dataRowArray[6];
                if(product.price==""      && dataRowArray[7]!="")product.price = dataRowArray[7];

                if(Number(product.timestamp)<Number(dataRowArray[6])){
                    product.timestamp = dataRowArray[6];
                    product.price     = dataRowArray[7];
                }
                product.save(function (err) {
                    if (err){console.error(err);}
                    else addRowsToDb(dataArray,currIndex+1)
                });
            }
        });
    }


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
    }
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

    }


    function parseFileAndPopulateDb(fileName){
        //TODO:?check if filename exists in the "readFile"
        isFileRead(fileName,function(err,response){
            if(err)console.error("error in detecting if the file was read",err);
            else if(response==true)
                console.log("file "+fileName+" was already read");
            else if(response==false){
                console.log("reading file "+fileName+" and adding it to db");
                //read file and add to db

                var csv = require('fast-csv');

                var stream = fs.createReadStream(dataLocation + fileName);

                var csvStream = csv()
                    .on("data", function(data){
                        //console.log(data);
                        addRowToDb(data);
                    })
                    .on("end", function(){
                        console.log("done");
                        fileIsRead(fileName);
                    });

                stream.pipe(csvStream);







                //fs.readFile(dataLocation + fileName, 'utf8', function(err, data) {
                //    if (err) {
                //        console.error(err);
                //    }
                //    console.log('parsing file: ' + fileName);
                //    //console.log(data);
                //    var dataArray=data.split("\n");
                //    var dataArrayLength=dataArray.length;
                //    console.log("file length->",dataArrayLength);
                //    /*for(var i=1;i<dataArrayLength;i++){
                //        addRowToDb(dataArray[i]);
                //
                //    }*/
                //
                //    addRowsToDb(dataArray,0);
                //    //TODO:add file to read list
                //    setTimeout(function() {
                //        fileIsRead(fileName);
                //    },100);
                //
                //});
            }
        })
    }



    /*checkForFile(parsedListFileName,function(){
        console.log("initiated file parsedList.csv (if it didn't exists earlier) generated for storing which files have been parsed");

        //get list of all files which are present
        getAllFilesInFolder(dataLocation,function(allFilesInFolder){
            console.log("allFilesInFolder->",allFilesInFolder);
            //check which files have been read
            getFilesRead(parsedListFileName,function(filesRead){
                console.log("filesRead->",filesRead);
                //get an array of files which have not been read
                var filesUnread = arr_diff(filesRead,allFilesInFolder);
                console.log("filesUnread->",filesUnread);
                /!*for(var i=0;i<filesUnread.length;i++)
                    parseFileAndPopulateDb(filesUnread[i]);*!/
                //parseFileAndPopulateDb("xaa.csv");

            });
        });
        //for each file in the array
        //read that file and store it in the database
    });*/


    function startReading(dir){
        var files = fs.readdirSync(dir);
        for(var i = 0 ; i< files.length ; i ++ ){
            if(files[i].match(/\.*csv$/)){
                //parseFileAndPopulateDb(files[i]) ;
                //console.log("reading file "+files[i]);
            }
        }

    };


    //parseFileAndPopulateDb("test.csv");



    checkForFile(parsedListFileName,function(err) {
        if(err)console.error("error in creating file");
        console.log("initiated file parsedList.csv (if it didn't exists earlier) generated for storing which files have been parsed");
        startReading(dataLocation);
    });

};