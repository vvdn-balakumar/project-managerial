var objdaoimpl = require('../dao/mongodaoimpl.js');
var async = require('async');
var moment = require("moment");
var objConfig = require('../config.js');
var objCalculations = require('../util/calculations.js');
var objjsFunctions = require('../util/jsfunctions.js');
var objProcessedManagerialData = require('./summarymapmanagerialdataprocessor');
var objProcessData = require('./summarymapdataprocessor');
var logger = console;

var intNumOfDays = objConfig.numberofDaysSM;
var objOuptMeterTransactionData;
var objOuptTransTransactionData;
var COMMONFAULT_TEXT = "CommonFault";
var objStartDate;

var arrFilter = ["CellID", "Line1Voltage", "Line2Voltage", "Line3Voltage",
    "Line1Current", "Line2Current", "Line3Current", "AmbientTemperarture", "TopTemperature",
    "BottomTemperature", "TransformerOilLevel", "ActiveReceivedCumulativeRate_Total", "ActiveDeliveredCumulativeRate_Total",
    "Apparent_m_Total", "ReadTimestamp", "Line1PhaseAngle", "Line2PhaseAngle", "Line3PhaseAngle",
    "BatteryVoltage", "BatteryStatus"];

var arrMeterFilter = ["CellID", "DeviceID", "ActiveReceivedCumulativeRate_Total", "ActiveDeliveredCumulativeRate_Total",
    "Apparent_m_Total", "Line1InstVoltage", "Line2InstVoltage", "Line3InstVoltage", "Line1InstCurrent",
    "Line2InstCurrent", "Line3InstCurrent", "Line1Frequency", "Line2Frequency", "Line3Frequency",
    "ReadTimestamp", "Status", "Line1PowerFactor", "Line2PowerFactor", "Line3PowerFactor"];
/**
 * @description - Code to get all summary map related data
 * @param context - console
 * @return - callback
 */
function getAllSummaryMapRelatedDetails(context, callback) {
    logger = context;
    intNumOfDays = objConfig.numberofDaysSM;
    objStartDate = new Date();
    objStartDate.setUTCDate(objStartDate.getUTCDate() - objConfig.numberofDaysSM);
    objStartDate.setUTCMinutes(0);
    objStartDate.setUTCSeconds(0);
    getAllSummaryMapTransactionRelatedDetails(function (err, data) {
        try {
            if (data) {
                mergeAndPopulateManagerialData(data, callback);
            } else {
                logger.log(err);
                callback(err, null);
            }
            objdaoimpl.closeConnection();
        } catch (exc) {
            callback(exc, null);
            objdaoimpl.closeConnection();
        }
    });
}

/**
 * @description - Code to merge and populate managerial data
 * @param data - data
 * @return - callback
 */
function mergeAndPopulateManagerialData(data, callback) {
    mergeKeys(data);
    objProcessedManagerialData.getManagerialData(data, function (err) {
        if (err) {
            logger.log(err);
        }
        mergeData(data);
        if (data && data.meterdata) {
            var objCellData = data.meterdata;
            for (var strCellId in objCellData) {
                if (objCellData.hasOwnProperty(strCellId) && objCellData[strCellId]) {
                    var objMeterData = objCellData[strCellId];
                    var arrCellObjDet = Object.keys(objMeterData);
                    var totalNoOfMeters = arrCellObjDet.length - 1;
                    processMeterDataForAccumlation(objMeterData, totalNoOfMeters);
                }
            }
        }
        callback(null, data);
        objdaoimpl.closeConnection();
    });
}
/**
 * @description - Code to process meter data for accumulative value
 * @param objMeterData - meter data
 * @param totalNoOfMeters - no of meters
 * @return - callback
 */
function processMeterDataForAccumlation(objMeterData, totalNoOfMeters) {
    for (var strDateVal in objMeterData.TransformerData) {
        if (objMeterData.TransformerData.hasOwnProperty(strDateVal)) {
            calculateAccumlativeValues(objMeterData, totalNoOfMeters, strDateVal);
        }
    }
}
/**
 * @description - Code to calculate accumulative value
 * @param objMeterData - meter data
 * @param totalNoOfMeters - no of meters
 * @param strDateVal - starting date
 * @return - callback
 */
function calculateAccumlativeValues(objMeterData, totalNoOfMeters, strDateVal) {
    objMeterData.TransformerData[strDateVal].AllMeter_ActiveReceivedCumulativeRate_Total = 0;
    for (var strKey in objMeterData) {
        if (objMeterData.hasOwnProperty(strKey) && (strKey !== 'TransformerData' && objMeterData[strKey])) {
            var objMeterDataDet = objMeterData[strKey].TranData[strDateVal];
            if (objMeterDataDet) {
                objMeterData.TransformerData[strDateVal].AllMeter_CumulativeRate_Total += objMeterDataDet.Meter_;
            }
        }
    }
    objMeterData.TransformerData[strDateVal].Non_Technical_Loss =
        ((objMeterData.TransformerData[strDateVal].Transformer_ActiveReceivedCumulativeRate_Total -
            objMeterData.TransformerData[strDateVal].AllMeter_ActiveReceivedCumulativeRate_Total) +
            (objMeterData.TransformerData[strDateVal].AllMeter_ActiveReceivedCumulativeRate_Total * 0.10)) / totalNoOfMeters;
}
/**
 * @description - Code to get all summary map transaction related data
 * @param   Nil
 * @return - callback
 */
function getAllSummaryMapTransactionRelatedDetails(callback) {
    async.parallel({
        meterdata: function (callback) {
            getAllSummaryMapRelatedDetailsMeter(callback);
        },
        transformerdata: function (callback) {
            getAllSummaryMapRelatedDetailsTransformer(callback);
        }
    }, function (err, results) {
        if (err) {
            callback(err, null);
            return;
        }
        try {
            if (results) {
                if (results.meterdata) {
                    objProcessData.processDataFor(results.meterdata, false);
                }
                if (results.transformerdata) {
                    objProcessData.processDataFor(results.transformerdata, true);
                }
                callback(null, results);
            }
        } catch (exc) {
            callback(exc, null);
        }
    });
}
/**
 * @description - Code to get summary map transformer related data
 * @return - callback
 */
function getAllSummaryMapRelatedDetailsTransformer(callback) {
    objdaoimpl.getCursorFromCollectionSorted("DELTA_Transaction_Data", ['DBTimestamp', 'result.Transformer.StatusTransformer'], [{ $gt: objStartDate }, "Connected"], null, { 'result.meters': 0 }, function (err, arrTransformerTransData) {
        objOuptTransTransactionData = {};
        if (arrTransformerTransData) {

            arrTransformerTransData.stream()
                .on('data', function (meterTranItem) {
                    processTransformerTransactionItem(meterTranItem);
                })
                .on('error', function (err) {
                    callback(err, null);
                    arrTransformerTransData.close();
                })
                .on('end', function () {
                    callback(null, objOuptTransTransactionData);
                    arrTransformerTransData.close();
                });

        } else {
            callback(err, null);
        }

    });
}
/**
 * @description - Code to process transformer transaction 
 * @param meterTranItem - meter transaction data
 * @return Nil
 */
function processTransformerTransactionItem(meterTranItem) {
    try {
        var objMeterData = meterTranItem;
        var arrTransformerResultData = [];
        var strCellIdVal = null;
        if (objMeterData.result) {
            arrTransformerResultData = objMeterData.result.Transformer;
            strCellIdVal = objMeterData.result.CellID;
        }
        var objDBTimestampVal = objMeterData.DBTimestamp;
        var meterResultTranItem = arrTransformerResultData;
        var objRowData = {};

        processTransformerTransactionSkipItems(strCellIdVal, meterResultTranItem, objRowData);

        objRowData.Transformer_CellID = strCellIdVal;
        objRowData.MeterDBTimestampVal = new Date(objDBTimestampVal);
        objRowData.ActualTransformer_ReadTimestamp = objRowData.Transformer_ReadTimestamp ? objRowData.Transformer_ReadTimestamp : objRowData.MeterDBTimestampVal;
        objRowData.Transformer_ReadTimestamp = objDBTimestampVal;

        var strCellID = objRowData.Transformer_CellID;
        if (strCellID) {
            if (!objOuptTransTransactionData[strCellID]) {
                objOuptTransTransactionData[strCellID] = {};
            }
            if (!objOuptTransTransactionData[strCellID].TransLastData) {
                objOuptTransTransactionData[strCellID].TransLastData = [];
            }
            if (objRowData.Transformer_ReadTimestamp) {
                objRowData.Transformer_ReadTimestamp = objDBTimestampVal;
                var objMeterDateMoment = moment.utc(objRowData.Transformer_ReadTimestamp, 'DD-MM-YYYY HH:mm:ss');
                var strTransformerDate = objMeterDateMoment.format('YYYY-MM-DD_HH');
                var strTransactionKey = strTransformerDate;
                updateDataToTransaction(objOuptTransTransactionData, strCellID, strTransactionKey, objRowData);
            }
        }

    } catch (err) {
        logger.log(err);
    }
}
/**
 * @description - Code to skip items for process transformer transaction  
 * @param strCellIdVal - cell id to be skipped
 * @param meterResultTranItem - meter transaction item
 * @param objRowData - row data
 * @return Nil
 */
function processTransformerTransactionSkipItems(strCellIdVal, meterResultTranItem, objRowData) {
    if (objConfig.cellIdToSkip.indexOf(strCellIdVal) === -1) {
        objjsFunctions.assignValuesFrmObject(meterResultTranItem, objRowData, "Transformer_", arrFilter, false, false);
    } else {
        objjsFunctions.assignValuesFrmObject(meterResultTranItem, objRowData, "Transformer_", arrFilter, false, true);
    }
}
/**
 * @description - Code to skip items for process transformer transaction  
 * @param strCellIdVal - cell id to be skipped
 * @param meterResultTranItem - meter transaction item
 * @param objRowData - row data
 * @return Nil
 */
function updateDataToTransaction(objOuptTransTransactionData, strCellID, strTransactionKey, objRowData) {
    if (!objOuptTransTransactionData[strCellID].TranData) {
        objOuptTransTransactionData[strCellID].TranData = {};
    }
    if (!objOuptTransTransactionData[strCellID].TranData[strTransactionKey]) {
        objOuptTransTransactionData[strCellID].TranData[strTransactionKey] = [];
    }
    objOuptTransTransactionData[strCellID].TranData[strTransactionKey].push(objRowData);
    objOuptTransTransactionData[strCellID].TransLastData = objRowData;
}
/**
 * @description - Code to get all summary map related meter data
 * @return callback
 */
function getAllSummaryMapRelatedDetailsMeter(callback) {
    objdaoimpl.getCursorFromCollectionSorted("DELTA_Transaction_Data", ['DBTimestamp', 'result.meters.Status'], [{ $gt: objStartDate }, ["Connected", COMMONFAULT_TEXT]], null, { 'result.Transformer': 0 }, function (err, objMeterTransDataCursor) {
        objOuptMeterTransactionData = {};
        if (objMeterTransDataCursor) {
            objMeterTransDataCursor.stream()
                .on('data', function (meterTranItem) {
                    processMeterTransactionItem(meterTranItem);
                })
                .on('error', function (err) {
                    logger.log('errr:', err);
                    callback(err, null);
                    objMeterTransDataCursor.close();
                })
                .on('end', function () {
                    callback(null, objOuptMeterTransactionData);
                    objMeterTransDataCursor.close();
                });
        } else {
            callback(err, null);
        }
    });
}
/**
 * @description - Code to process meter transaction item  
 * @param meterTranItem - meter transaction item
 * @return Nil
 */
function processMeterTransactionItem(meterTranItem) {
    try {
        var objMeterData = meterTranItem;
        var arrMeterResultData = [];
        var strCellIdVal = null;
        if (objMeterData.result) {
            arrMeterResultData = objMeterData.result.meters;
            strCellIdVal = objMeterData.result.CellID;
        }
        var objDBTimestampVal = objMeterData.DBTimestamp;

        if (!arrMeterResultData) {
            return;
        }

        for (var i = 0; i < arrMeterResultData.length; i++) {
            processMeterTransactionRecord(i, arrMeterResultData, objOuptMeterTransactionData, strCellIdVal, objDBTimestampVal);
        }
    } catch (err) {
        logger.log(err);
    }
}
/**
 * @description - Code to process meter transaction record
 * @param i - index
 * @param arrMeterResultData - meter result data
 * @param objOuptMeterTransactionData - meter transaction data
 * @param strCellIdVal - cell id
 * @param objDBTimestampVal- timestamp
 * @return Nil
 */
function processMeterTransactionRecord(i, arrMeterResultData, objOuptMeterTransactionData, strCellIdVal, objDBTimestampVal) {
    var meterResultTranItem = arrMeterResultData[i];
    var objRowData = {};
    assignValuesFrmObject(meterResultTranItem, objRowData, "Meter_", arrMeterFilter);
    objRowData.Meter_CellID = strCellIdVal;
    objRowData.Meter_NetworkResponseRate = 1;
    if (objRowData.Meter_Status === COMMONFAULT_TEXT) {
        objRowData.Meter_NetworkResponseRate = 0;
    }
    objRowData.MeterDBTimestampVal = new Date(objDBTimestampVal);
    var strCellID = objRowData.Meter_CellID;
    objRowData.ActualMeter_ReadTimestamp = objRowData.Meter_ReadTimestamp;

    if (!strCellID) {
        return;
    }

    if (!objOuptMeterTransactionData[strCellID]) {
        objOuptMeterTransactionData[strCellID] = {};
    }
    if (objRowData.Meter_Status === "Connected" || objRowData.Meter_Status === COMMONFAULT_TEXT) {
        updateMeterTransactionData(objOuptMeterTransactionData, strCellID, objRowData, objDBTimestampVal);
    }
}
/**
 * @description - Code to update meter transaction record
 * @param objOuptMeterTransactionData - meter transaction data
 * @param strCellID - cell id
 * @param objRowData - row data
 * @param objDBTimestampVal- timestamp
 * @return Nil
 */
function updateMeterTransactionData(objOuptMeterTransactionData, strCellID, objRowData, objDBTimestampVal) {
    objRowData.Meter_ReadTimestamp = objDBTimestampVal;
    var objMeterDateMoment = moment.utc(objRowData.Meter_ReadTimestamp, 'DD-MM-YYYY HH:mm:ss');
    var strMeterDate = objMeterDateMoment.format('YYYY-MM-DD_HH');
    var strTransactionKey = strMeterDate;
    if (!objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID]) {
        objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID] = {};
    }
    if (objRowData.Meter_Status === "Connected") {
        updateMeterConnectedTransactionData(objOuptMeterTransactionData, strCellID, strTransactionKey, objRowData);
    }
    if (objRowData.Meter_Status === COMMONFAULT_TEXT) {
        updateMeterFaultData(objOuptMeterTransactionData, strCellID, strTransactionKey, objRowData);
    }
}
/**
 * @description - Code to update connected meter transaction record
 * @param objOuptMeterTransactionData - meter transaction data
 * @param strTransactionKey - transaction key
 * @param objRowData - row data
 * @return Nil
 */
function updateMeterConnectedTransactionData(objOuptMeterTransactionData, strCellID, strTransactionKey, objRowData) {
    if (!objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID].TranData) {
        objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID].TranData = {};
    }

    if (!objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID].MeterLastData) {
        objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID].MeterLastData = {};
    }
    if (!objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID].TranData[strTransactionKey]) {
        objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID].TranData[strTransactionKey] = [];
    }
    objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID].TranData[strTransactionKey].push(objRowData);
    objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID].MeterLastData = objRowData;
}
/**
 * @description - Code to update meter fault data
 * @param objOuptMeterTransactionData - meter transaction data
 * @param strCellID - cell id
 * @param strTransactionKey - transaction key
 * @param objRowData - row data
 * @return Nil
 */
function updateMeterFaultData(objOuptMeterTransactionData, strCellID, strTransactionKey, objRowData) {
    if (!objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID].FaultTranData) {
        objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID].FaultTranData = {};
    }
    if (!objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID].FaultTranData[strTransactionKey]) {
        objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID].FaultTranData[strTransactionKey] = [];
    }
    objOuptMeterTransactionData[strCellID][objRowData.Meter_DeviceID].FaultTranData[strTransactionKey].push(objRowData);
}
/**
 * @description - Code to assign value
 * @param objInput - input data
 * @param objOutput - output data 
 * @param keyPrefix - key prefix
 * @param arrFilter - array of filter
 * @param isSkipValues - skip values
 * @return Nil
 */
function assignValuesFrmObject(objInput, objOutput, keyPrefix, arrFilter, isSkipValues) {
    objjsFunctions.assignValuesFrmObject(objInput, objOutput, keyPrefix, arrFilter, false, isSkipValues);
}
/**
 * @description - Code to merge keys
 * @param objInputData - input data
 * @return Nil
 */
function mergeKeys(objInputData) {
    try {
        for (var strPropertyKey in objInputData.transformerdata) {
            if (objInputData.transformerdata.hasOwnProperty(strPropertyKey) && !objInputData.meterdata[strPropertyKey]) {
                objProcessedManagerialData.createDummyMeterObj(objInputData, strPropertyKey);
            }
        }
    } catch (err) {
        logger.log(err);
    }
}
/**
 * @description - Code to merge data
 * @param objInputData - input data
 * @return Nil
 */
function mergeData(objInputData) {
    try {
        for (var strPropertyKey in objInputData.transformerdata) {
            if (!objInputData.transformerdata.hasOwnProperty(strPropertyKey)) {
                continue;
            }
            var objTransformerData = objInputData.transformerdata[strPropertyKey];
            if (!objInputData.meterdata[strPropertyKey]) {
                objProcessedManagerialData.createDummyMeterObj(objInputData, strPropertyKey);
            }
            loopAndMergeMeterData(objInputData, objTransformerData, strPropertyKey);
        }
    } catch (err) {
        logger.log("Error while merge data", err);
    }
}
/**
 * @description - Code to loop over meter data and merge
 * @param objInputData - input data
 * @param objTransformerData - transformer data
 * @param strPropertyKey - property key
 * @return Nil
 */
function loopAndMergeMeterData(objInputData, objTransformerData, strPropertyKey) {
    for (var strMeterIdVal in objInputData.meterdata[strPropertyKey]) {
        if (objInputData.meterdata[strPropertyKey].hasOwnProperty(strMeterIdVal)) {
            var objMeterData = objInputData.meterdata[strPropertyKey][strMeterIdVal];
            if (!objMeterData) {
                continue;
            }
            loopTransformerAndMergeDataToMeter(objInputData, objTransformerData, objMeterData, strPropertyKey);
        }
    }
}
/**
 * @description - Code to loop over transformer data and merge
 * @param objInputData - input data
 * @param objTransformerData - transformer data
 * @param objMeterData - meter data
 * @param strPropertyKey - property key
 * @return Nil
 */
function loopTransformerAndMergeDataToMeter(objInputData, objTransformerData, objMeterData, strPropertyKey) {
    for (var strDatePropertyKey in objTransformerData) {
        if (objMeterData[strDatePropertyKey]) {
            if (strDatePropertyKey === 'TranData') {
                objInputData.meterdata[strPropertyKey].TransformerData = objTransformerData[strDatePropertyKey];
            } else {
                assignValuesFrmObject(objTransformerData[strDatePropertyKey], objMeterData[strDatePropertyKey]);
            }

        } else {
            objMeterData[strDatePropertyKey] = objTransformerData[strDatePropertyKey];
        }
    }
    assignValuesFrmObject(objTransformerData.TransLastData, objMeterData.MeterLastData);
}

module.exports = {
    getAllSummaryMapRelatedDetails: getAllSummaryMapRelatedDetails
};