var mongoose = require('mongoose');

// define the schema for our place model
var productSchema = mongoose.Schema({
    pid:String,
    title:String,
    upcs:String,
    categoryld:String,
    storeld:String,
    seller:String,
    timestamp:String,
    price:String
});
module.exports = mongoose.model('Product', productSchema);