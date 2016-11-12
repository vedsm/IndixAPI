var Product = require('./../models/product');

module.exports = function (app) {
    app.get('/upc/:inputUpc', function (req, res) {
        var inputUpc = req.params.inputUpc;
        Product.find({upcs:inputUpc},function(err,products){
            if(err)return res.json({success:false,message:"error "+err});
            else{
                res.json({success:true,message:"getting data for upc->"+inputUpc,products:products});
            }
        })
    });

    app.get('/pid/:inputPid', function (req, res) {
        var inputPid = req.params.inputPid;
        Product.find({pid:inputPid},function(err,products){
            if(err)return res.json({success:false,message:"error "+err});
            else{
                res.json({success:true,message:"getting data for inputPid->"+inputPid,products:products})
            }
        })
    });

    app.get('/category/:inputCategory', function (req, res) {
        var inputCategory = req.params.inputCategory;
        Product.find({category:inputCategory},function(err,products){
            if(err)return res.json({success:false,message:"error "+err});
            else{
                res.json({success:true,message:"getting data for inputCategory->"+inputCategory,products:products})
            }
        })
    });

    app.get('/search', function (req, res) {
        var token    = req.query.q;
        var minPrice = Number(req.query.minPrice);
        var maxPrice = Number(req.query.maxPrice);
        console.log("Token requested ",token);
        Product.find(function(err,allProducts){
            //console.log("all products",JSON.stringify(allProducts));
        })

        //
        Product.find({$and:[{title: {$regex:'.*'+token+'.*'}},{price:{$lt:maxPrice}},{price:{$gt:minPrice}}]},function(err,products){
            if(err)return res.json({success:false,message:"error "+err});
            else{
                res.json({success:true,message:"getting data for token->"+token,products:products});
            }
        })
    });

}