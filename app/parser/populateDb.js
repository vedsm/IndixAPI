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
                callback();
            }else
            {
                fs.writeFile(fileName, "", function (err, data)
                {
                    callback();
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
        var dataRowArray = dataArray.split(",");
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
                newProduct.save(function (err) {if (err)console.error(err);});
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
                product.save(function (err) {if (err)console.error(err);});
            }
        });
    }


    function parseFileAndPopulateDb(fileName){
        //TODO:?check if filename exists in the "readFile"

        //read file and add to db
        fs.readFile(dataLocation + fileName, 'utf8', function(err, data) {
            if (err) {
                console.error(err);
            }
            console.log('parsing file: ' + fileName);
            //console.log(data);
            var dataArray=data.split("\n");
            var dataArrayLength=dataArray.length;
            console.log("file length->",dataArrayLength);
            for(var i=1;i<dataArrayLength;i++){
                addRowToDb(dataArray[i]);

            }
        });
    }



    checkForFile(parsedListFileName,function(){
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
                /*for(var i=0;i<filesUnread.length;i++)
                    parseFileAndPopulateDb(filesUnread[i]);*/
                parseFileAndPopulateDb("xaa.csv");

            });
        });

        //for each file in the array
        //read that file and store it in the database
    });

};