var objdaoimpl = require('../dao/mysqldaoimpl.js');
var objSummaryMapModel = require('../model/sqltables/summarymapmodel.js');
var objManagerialDatadaoimpl = require('./managerialdatadaoimpl.js');
var objLatestTransactionModel = require('../model/sqltables/latesttransactionmodel.js');
var objSummaryMapModelDAO = require('../model/summarymapdaoimpl.js');
var objConfig = require('../config.js');
var objjsFunctions = require('../util/jsfunctions.js');
var moment = require("moment");
var forEach = require('async-foreach').forEach;
var loopIndex = 0;
var loopCount = 0;

var logger = console;
var arrDateToCompare = [];
var arrAvailableMeters = [];
var objMeterHSMap = {};

/**
 * @description - Code to post all summary map related data
 * @param objData -  data
 * @return callback
 */
function postAllSummaryMapRelatedDetails(objData, callback) {
    try {
        arrDateToCompare = [];
        objMeterHSMap = {};
        objManagerialDatadaoimpl.getManagerialData(function (err, objManagerialdata) {
            if (err) {
                logger.log(err);
            }
            arrAvailableMeters = objManagerialdata ? Object.keys(objManagerialdata.meterobj) : [];
            populateMeterManagerialInfo(objManagerialdata);
            var objCurrentDate = new Date();
            var objStartDate = new Date();
            populateRequiredDates(objStartDate, objCurrentDate);
            loopIndex = 0;
            loopCount = 0;
            objdaoimpl.synctable("summarymap", objSummaryMapModel.objSummaryMap,
                objSummaryMapModel.objTableProps, function (err) {
                    if (err) {
                        logger.log(err);
                    }
                    objdaoimpl.synctable("latesttransactions", objLatestTransactionModel.objLatestTrans,
                        objLatestTransactionModel.objTableProps, function (err) {
                            if (err) {
                                logger.log(err);
                            }
                            processAllAvailData(objData, callback);
                        });
                });
        });
    } catch (err) {
        console.error(err);
    }
}

/**
 * @description - Code to populate all meter managerial data
 * @param objManagerialdata -  data
 * @return Nil
 */
function populateMeterManagerialInfo(objManagerialdata) {
    var intArrAvailableMetersLen = arrAvailableMeters.length;
    while (intArrAvailableMetersLen--) {
        var strMeterId = arrAvailableMeters[intArrAvailableMetersLen];
        var strTransformerId = objManagerialdata.meterobj[strMeterId].TransformerID;
        var strCircuitId = "";
        objMeterHSMap[strMeterId] = {};
        objjsFunctions.assignValuesFrmObject(objManagerialdata.meterobj[strMeterId], objMeterHSMap[strMeterId], "", [], true, false);
        objMeterHSMap[strMeterId].Meter_DeviceID = strMeterId;
        objMeterHSMap[strMeterId].TransformerID = strTransformerId;

        if (objManagerialdata.transformerobj[strTransformerId]) {
            objjsFunctions.assignValuesFrmObject(objManagerialdata.transformerobj[strTransformerId], objMeterHSMap[strMeterId], "", [], true, false);
            objMeterHSMap[strMeterId].Transformer_CellID = objManagerialdata.transformerobj[strTransformerId].HypersproutID;
            strCircuitId = objMeterHSMap[strMeterId].CircuitID = objManagerialdata.transformerobj[strTransformerId].CircuitID;
        }

        if (objManagerialdata.circuitobj[strCircuitId]) {
            objjsFunctions.assignValuesFrmObject(objManagerialdata.circuitobj[strCircuitId], objMeterHSMap[strMeterId], "", [], true, false);
        }

    }
}
/**
 * @description - Code to populate non reporting meter transaction data
 * @param objData -  data
 * @return Nil
 */
function populateNonReportingMeterTransData(objData, callback) {
    var intArrAvailableMetersLen = arrAvailableMeters.length;
    var oldLoopCount = loopCount;
    while (intArrAvailableMetersLen--) {
        var strMeterId = arrAvailableMeters[intArrAvailableMetersLen];
        arrAvailableMeters.splice(intArrAvailableMetersLen, 1);
        var strHypersproutId = objMeterHSMap[strMeterId].Transformer_CellID;
        objData.meterdata[strHypersproutId] = objData.meterdata[strHypersproutId] ? objData.meterdata[strHypersproutId] : {};
        objData.meterdata[strHypersproutId][strMeterId] = objData.meterdata[strHypersproutId][strMeterId] ? objData.meterdata[strHypersproutId][strMeterId] : {};
        objData.meterdata[strHypersproutId][strMeterId].TranData = objData.meterdata[strHypersproutId][strMeterId].TranData ? objData.meterdata[strHypersproutId][strMeterId].TranData : {};
        objData.meterdata[strHypersproutId][strMeterId].managerialdata = objMeterHSMap[strMeterId];
        processTransactionRecords(arrDateToCompare, objData, strHypersproutId, strMeterId, callback);
    }
    if (oldLoopCount === loopCount) {
        callback(null, true);
    }
}
/**
 * @description - Code to populate required dates
 * @param objStartDate -  start data
 * @param objCurrentDate - today date
 * @return Nil
 */
function populateRequiredDates(objStartDate, objCurrentDate) {
    objStartDate.setUTCDate(objStartDate.getUTCDate() - objConfig.numberofDaysSM);
    objStartDate.setUTCMinutes(0);
    objStartDate.setUTCSeconds(0);
    while (objStartDate < objCurrentDate) {
        var objRequiredDateFormat = moment.utc(objStartDate, 'DD-MM-YYYY HH:mm:ss');
        var strDateFormat = objRequiredDateFormat.format('YYYY-MM-DD_HH');
        arrDateToCompare.push(strDateFormat);
        objStartDate.setUTCMinutes(objStartDate.getUTCMinutes() + 60);
    }
}
/**
 * @description - Code to process all available data
 * @param objData -   data
 * @return callback
 */
function processAllAvailData(objData, callback) {
    var arrHSKeys = Object.keys(objData.meterdata);
    if (arrHSKeys.length === 0) {
        populateNonReportingMeterTransData(objData, callback);
    }
    forEach(arrHSKeys, function (strHSId) {
        if (objData.meterdata.hasOwnProperty(strHSId)) {
            var done = this.async();
            loopMeterData(objData, strHSId, callback);
            setTimeout(function () {
                done();
            }, 100);
        }
    });
}
/**
 * @description - Code to loop over meter data
 * @param objData -   data
 * @param objHSKey - HS key
 * @return callback
 */
function loopMeterData(objData, objHSKey, callback) {
    var objDeviceMergedData = objData.meterdata;
    var arrMeterIds = Object.keys(objDeviceMergedData[objHSKey]);
    forEach(arrMeterIds, function (objDeviceID) {
        var done = this.async();
        if (objDeviceMergedData[objHSKey].hasOwnProperty(objDeviceID) && objDeviceID !== "TransformerData") {
            var indexOfMeter = arrAvailableMeters.indexOf(objDeviceID);
            if (indexOfMeter !== -1) {
                arrAvailableMeters.splice(indexOfMeter, 1);
            }
            processTransactionRecords(arrDateToCompare, objData, objHSKey, objDeviceID, callback);
        }
        setTimeout(function () {
            done();
        }, 50);
    });
}
/**
 * @description - Code to process transaction data
 * @param arrDateToCompare -   date to compare
 * @param objData - data
 * @param objHSKey - HS key
 * @param objDeviceID - device id
 * @return callback
 */
function processTransactionRecords(arrDateToCompare, objData, objHSKey, objDeviceID, callback) {
    var objDeviceMergedData = objData.meterdata;
    var arrDateToCompareLen = arrDateToCompare.length;
    while (arrDateToCompareLen--) {
        var objPropertyKey = arrDateToCompare[arrDateToCompareLen];
        if (objDeviceMergedData[objHSKey][objDeviceID].TranData.hasOwnProperty(objPropertyKey)) {
            loopCount += objSummaryMapModelDAO.updateSummaryMapModel(objDeviceMergedData, objHSKey, objDeviceID, objPropertyKey,
                responseCallback);
        } else {
            objDeviceMergedData[objHSKey][objDeviceID].TranData = objDeviceMergedData[objHSKey][objDeviceID].TranData ? objDeviceMergedData[objHSKey][objDeviceID].TranData : {};
            objDeviceMergedData[objHSKey][objDeviceID].TranData[objPropertyKey] = {};
            var objMomentDateToUpdate = moment.utc(objPropertyKey, 'YYYY-MM-DD_HH');
            var objDateToUpdate = new Date();
            objDateToUpdate.setTime(objMomentDateToUpdate.valueOf());
            objDeviceMergedData[objHSKey][objDeviceID].TranData[objPropertyKey].Meter_ReadTimestamp = objDateToUpdate;
            objDeviceMergedData[objHSKey][objDeviceID].TranData[objPropertyKey].NetworkResponceRate = 0;
            loopCount += objSummaryMapModelDAO.updateSummaryMapModel(objDeviceMergedData, objHSKey, objDeviceID, objPropertyKey,
                responseCallback);
        }
    }
    loopCount += objSummaryMapModelDAO.updateSummaryMapModel(objDeviceMergedData, objHSKey, objDeviceID, 'MeterLastData',
        responseCallback);

    function responseCallback(err) {
        if (err) {
            console.error("Error", err);
        }
        loopIndex++;

        if (loopIndex >= loopCount) {
            if (arrAvailableMeters.length > 0) {
                populateNonReportingMeterTransData(objData, callback);
            } else {
                callback(err, true);
            }
        }
    }
}

module.exports = {
    postAllSummaryMapRelatedDetails: postAllSummaryMapRelatedDetails
};