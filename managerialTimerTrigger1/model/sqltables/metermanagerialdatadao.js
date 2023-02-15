var objManagerialDatadaoimpl = require('./managerialdatadaoimpl.js.js');
var objManagerialDataModel = require('./sqltables/managerialdatamodel.js.js');
var objmysqldaoimpl = require('../dao/mysqldaoimpl.js');
var dbConMysql = require('../dao/mysqlconnector.js');
var connection = dbConMysql.getDbconnection
const json2csv = require('json2csv').parse;
var forEach = require('async-foreach').forEach;
var fs = require('fs');
var logger = console;
const dateTime = new Date().toLocaleString().slice(-24).replace(/\D/g, '').slice(0, 14);

var filePath = process.env.csvPath + "/csv_ManagerialData-" + dateTime + ".csv";
//var filePath = path.join(__dirname, "../../../", "project-datamart", "managerial", "public", "csv-" + dateTime + ".csv");
const fields = [
    'CircuitID','HypersproutID','HypersproutSerialNumber','MeterID','MeterSerialNumber',
    'MeterStatus','TransformerID','TransformerSerialNumber','TFMRName','MeterConsumerAddress', 'createdAt', 'updatedAt'
];
/**
 * @description - Code to update meter managerial data
 * @param context - console
 * @return callback
 */
function updateMeterManagerialDataToRDBMS(context, callback) {
    logger = context;
    objManagerialDatadaoimpl.getManagerialData(function (err, data) {
        processDataByMeter(context,data, callback);
    });
}
/**
 * @description - Code to process data by meter id
 * @param objManagerialData - managerial data
 * @return callback
 */
function processDataByMeter(context,objManagerialData, callback) {
    var transformerLen = Object.keys(objManagerialData.transformerobj);
    var flagMeter = false;
    if (transformerLen.length == 0) {
      callback("No transformer found",null);
    }
    else {
        objmysqldaoimpl.truncateEntries("managerialdata", objManagerialDataModel.objTableColumns,
            objManagerialDataModel.objTableProps, {}, function (err) {
                if (err) {
                    logger.log(err);
                }
                if (objManagerialData.meterobj) {
                    var arrMeterKeys = Object.keys(objManagerialData.meterobj);
                    if (arrMeterKeys.length == 0) {
                        callback("No registered meter found", null);
                    } else {
                        var intSyncIndex = 0;
                        var objDataCollection = [];
                        var now = new Date(Date.now()).toISOString();
                        forEach(arrMeterKeys, function (strMeterId, index, arrMeterObj) {
                            try {
                                var objMeterData = objManagerialData.meterobj[strMeterId];
                                var objTransformerData = objManagerialData.transformerobj[objMeterData.TransformerID];
                                objTransformerData = objTransformerData ? objTransformerData : {};
                                
                                objDataCollection.push(getDefinedObject(strMeterId, objMeterData.TransformerID, objMeterData, objTransformerData,now));
                              
                                if(index===(arrMeterObj.length-1))
                                {
                                    context.log("array length--->"+objDataCollection.length)
                                    if(objDataCollection.length>0){
                                        try {
                                            csv = json2csv(objDataCollection,{fields});
                                        } catch (err) {
                                            context.log("error in csv",err);
                                        }
                                        fs.appendFile(filePath,csv, function(err){
                                            if (err) {
                                                context.log("this is the error-->",err);
                                            } else {
                                                insertCsvtomysql(context,filePath,function(err,res){
                                                    if(err){
                                                        callback(err,null);
                                                    }else
                                                    callback(null,true);
                                                });
                                            }
                                        });
                                        delete objDataCollection;
                            
                                    }else{
                                        context.log("objDataCollection length -- array-is empty")
                                        callback(null, true);
                                    }
                                }
                            } catch (err) {
                                logger.log(err);
                            }
                         
                        });
                        //processDataByTransformer(context,objDataCollection,objManagerialData, now,callback);                        
                        // insertDataToManagerialData(objDataCollection[index-1], function (err, bUpdateStatus) {
                        //     if (err) {
                        //         logger.log(err, bUpdateStatus);
                        //     }
                        //     delete objDataCollection[intSyncIndex];
                        //     if (intSyncIndex === (arrMeterObj.length - 1)) {
                        //         return processDataByTransformer(objManagerialData, callback);
                        //     }
                        //     intSyncIndex++;
                        // });
                    }
                } else {
                    callback(null, null);
                }
            });
    }
}
/**
 * @description - Code to process data by transformer id
 * @param objManagerialData - managerial data
 * @param objDataCollection - combined data collection
 * @param now - Datetime for created at and updated at
 * @return callback
 */
function processDataByTransformer(context,objDataCollection,objManagerialData,now,callback) {
    var arrTransformerKeys = objManagerialData.arrtransformeridstoprocess;

    if (arrTransformerKeys && arrTransformerKeys.length > 0) {
        // var intSyncIndex = 0;
        forEach(arrTransformerKeys, function (strTransId, index, arrTransObj) {
            try {
                var objMeterData = {};
                var objTransformerData = objManagerialData.transformerobj[strTransId];
                objDataCollection.push(getDefinedObject(null, strTransId, objMeterData, objTransformerData, now));
                // insertDataToManagerialData(objProcessedManagerialData, function (err, bUpdateStatus) {
                //     if (err) {
                //         logger.log(err, bUpdateStatus);
                //     }
                // if (intSyncIndex === (arrTransObj.length - 1)) {
                //     callback(null, objDataCollection);
                // }
                // intSyncIndex++;
                // });
            } catch (err) {
                logger.log(err);
            }
        });
        context.log("array length--->"+objDataCollection.length)
        if(objDataCollection.length>0){
            try {
                csv = json2csv(objDataCollection,{fields});
            } catch (err) {
                context.log("error in csv",err);
            }
            fs.appendFile(filePath,csv, function(err){
                if (err) {
                    context.log("this is the error-->",err);
                } else {
                    insertCsvtomysql(context,filePath,function(err,res){
                        if(err){
                            callback(err,null);
                        }else
                        callback(null,true);
                    });
                }
            });
            delete objDataCollection;

        }else{
            context.log("objDataCollection length -- array-is empty")
            callback(null, true);
        }
        //callback(null, objDataCollection);
    } else {
        callback(null, true);
       // callback(null, objDataCollection);
    }
}
/**
 * @description - Code to insert data to managerial table
 * @param objProcessedManagerialData - managerial data
 * @return callback
 */
function insertDataToManagerialData(objProcessedManagerialData, callback) {
    objmysqldaoimpl.insertData("managerialdata", objManagerialDataModel.objTableColumns,
        objManagerialDataModel.objTableProps,
        objProcessedManagerialData, function (err, objTransformerTransData) {
            callback(err, objTransformerTransData);
        });
}
/**
 * @description - Code to get object with proper data defined
 * @param strMeterId - meter id
 * @param strTransformerId - transformer id
 * @param objMeterData - meter data
 * @param objTransformerData - transformer data
 * @return callback
 */
function getDefinedObject(strMeterId, strTransformerId, objMeterData, objTransformerData,now) {
    var objToReturn = {};
    objToReturn.CircuitID = objTransformerData.CircuitID;
    objToReturn.TransformerID = strTransformerId;
    objToReturn.HypersproutID = objTransformerData.HypersproutID;
    objToReturn.TransformerSerialNumber = objTransformerData.TransformerSerialNumber;
    objToReturn.TFMRName = objTransformerData.TFMRName;
    objToReturn.HypersproutSerialNumber = objTransformerData.HypersproutSerialNumber;
    objToReturn.MeterID = strMeterId;
    objToReturn.MeterSerialNumber = objMeterData.MeterSerialNumber;
    objToReturn.MeterStatus = objMeterData.Status;
    objToReturn.MeterConsumerAddress = objMeterData.ConsumerAddress;
    objToReturn["createdAt"] = now;
    objToReturn["updatedAt"] = now;
    return objToReturn;
}

function insertCsvtomysql(context,filepath,callback) {
    // Removing connection as it is not required. 
    // https://stackoverflow.com/questions/14087924/cannot-enqueue-handshake-after-invoking-quit/56490766
    // connection.connect(function (err) {
    //     if (err) {
    //         throw err;
    //     }
    context.log('inside the insertion managerail');
    var sql = "LOAD DATA LOCAL INFILE '" + filepath + "' REPLACE INTO TABLE managerialdata FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\r\n' IGNORE 1 LINES (`CircuitID`,`HypersproutID`,`HypersproutSerialNumber`,`MeterID`,`MeterSerialNumber`,`MeterStatus`,`TransformerID`,`TransformerSerialNumber`,`TFMRName`,`MeterConsumerAddress`,`createdAt`,`updatedAt`) set id = NULL;"
    connection.query(sql, function (err, result) {
        if (err) {
            context.log(err);
            callback("err", true);
        }
        if (result) {
            context.log(result);
            fs.unlinkSync(filepath);
            context.log('unlinking temp file -->' + filepath)
           callback(null, true);
            //connection.end;
        }
    });
    //});
}

module.exports = {
    updateMeterManagerialDataToRDBMS: updateMeterManagerialDataToRDBMS
};