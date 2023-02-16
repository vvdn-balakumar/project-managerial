var objMeterManagerialData = require('./controllers/metermanagerialdatacontroller.js');
var dbCon = require('./dao/mongodaoimpl');

module.exports = function (context, myTimer) {
    context.log('Managerials map initiated');
    objMeterManagerialData.updateMeterManagerialDataToRDBMS(context, function (err, obj) {
        context.log('Error updateMeterManagerialDataToRDBMS', err);
        //dbCon.closeConnection();
        context.done()
       
    });
};