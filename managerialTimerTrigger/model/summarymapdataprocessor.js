var objdaoimpl = require('../dao/mongodaoimpl.js');
var async = require('async');
var moment = require("moment");
var objConfig = require('../config.js');
var objCalculations = require('../util/calculations.js');
var objjsFunctions = require('../util/jsfunctions.js');
var logger = console;

function assignValuesFrmObject(objInput, objOutput, keyPrefix, arrFilter, isSkipValues) {
    objjsFunctions.assignValuesFrmObject(objInput, objOutput, keyPrefix, arrFilter, false, isSkipValues);
}

/**
    * @description - Code to process data 
    * @param objInput - input data
    * @param isTransformer - is transformer or not
    * @return Nil
    */
function processDataFor(objInput, isTransformer) {
    objdaoimpl.getDataFromCollection("DELTA_SystemSettings", ["Settings", "Type.Status"], ["Communications", "Updated"], function (err, transactionQueries) {
        if (err) {
            logger.log(err);
        } else {
            try {
                if (transactionQueries[0].Type.Values.HypersproutTransactionPoolingInterval === 5) {
                    objConfig.transScheduler = 12;
                } else if (transactionQueries[0].Type.Values.HypersproutTransactionPoolingInterval === 10) {
                    objConfig.transScheduler = 6;
                } else {
                    objConfig.transScheduler = 4;
                }
                for (var devicePropertyKey in objInput) {
                    if (!objInput.hasOwnProperty(devicePropertyKey)) {
                        continue;
                    }
                    var objDeviceData = objInput[devicePropertyKey];
                    if (isTransformer) {
                        processDataForTransformer(objInput, objDeviceData, devicePropertyKey);
                    } else {
                        processDataForMeter(objInput, objDeviceData, devicePropertyKey);
                    }
                }
            } catch (err) {
                logger.log("Error while processing data ", err);
            }
        }
    });
}
/**
    * @description - Code to process data for transformer
    * @param objInput - input data
    * @param objDeviceData - device data
    * @param devicePropertyKey - device property key
    * @return Nil
    */
function processDataForTransformer(objInput, objDeviceData, devicePropertyKey) {
    var objDeviceTransactionData, propertyKey, objInnerData, objFormattedData, i, j, arrKeys, objPreviousInnerData;
    objDeviceTransactionData = objDeviceData.TranData;
    if (!objInput[devicePropertyKey].managerialdata) {
        objInput[devicePropertyKey].managerialdata = {};
    }
    arrKeys = Object.keys(objDeviceTransactionData).sort();
    objPreviousInnerData = null;
    for (j = 0; j < arrKeys.length; j++) {
        propertyKey = arrKeys[j];
        if (objDeviceTransactionData.hasOwnProperty(propertyKey)) {
            objInnerData = objDeviceTransactionData[propertyKey];
            objFormattedData = {};
            for (i = 0; i < objInnerData.length; i++) {
                objCalculations.processValues(objInnerData, i, objInput[devicePropertyKey].managerialdata, objConfig.transfomer_sm_arrNoProccessKey, propertyKey);
                objCalculations.processArrTimeStampValues(objInnerData, i, objFormattedData, objConfig.transfomer_sm_arrTimestpampKeys, propertyKey);
                objCalculations.processAverage(objInnerData, i, objFormattedData, objConfig.transfomer_sm_arrAverageKeys);
                objCalculations.processLastValue(objInnerData, i, objFormattedData, objConfig.transfomer_sm_arrLastValKeys, propertyKey);
                objCalculations.processSum(objInnerData, i, objFormattedData, objConfig.transfomer_sm_arrSumKeys);
                objCalculations.processDifference(objInnerData, i, objFormattedData, objConfig.transfomer_sm_arrDifffernceSumKeys, objPreviousInnerData);
            }
            objPreviousInnerData = {};
            objjsFunctions.assignValuesFrmObject(objInput[devicePropertyKey].TranData, objPreviousInnerData, "", [propertyKey], false, false);
            objPreviousInnerData = objPreviousInnerData[propertyKey];
            objInput[devicePropertyKey].TranData[propertyKey] = objFormattedData;
        }
    }
}
/**
    * @description - Code to process data for meter
    * @param objInput - input data
    * @param objDeviceData - device data
    * @param devicePropertyKey - device property key
    * @return Nil
    */
function processDataForMeter(objInput, objDeviceData, devicePropertyKey) {
    for (var strMeterid in objDeviceData) {
        if (!objDeviceData[strMeterid].TranData && objDeviceData[strMeterid].FaultTranData) {
            objDeviceData[strMeterid].TranData = objDeviceData[strMeterid].FaultTranData;
            delete objDeviceData[strMeterid].FaultTranData;
        }
        if (objDeviceData.hasOwnProperty(strMeterid) && objDeviceData[strMeterid].TranData) {
            var objFaultTranData = objDeviceData[strMeterid].FaultTranData ? objDeviceData[strMeterid].FaultTranData : null;
            if (!objInput[devicePropertyKey][strMeterid].managerialdata) {
                objInput[devicePropertyKey][strMeterid].managerialdata = {};
            }
            loopThroughMeterAndCalculate(objInput, objDeviceData, devicePropertyKey, objFaultTranData, strMeterid);
        }
    }
}
/**
    * @description - Code to loop through meter and calculate
    * @param objInput - input data
    * @param objDeviceData - device data
    * @param devicePropertyKey - device property key
    * @param objFaultTranData - Fault Transaction data
    * @param strMeterid - meter id
    * @return Nil
    */
function loopThroughMeterAndCalculate(objInput, objDeviceData, devicePropertyKey, objFaultTranData, strMeterid) {
    var propertyKey, objInnerData, objFormattedData, i, j;
    var objDeviceTransactionData = objDeviceData[strMeterid].TranData;
    var arrKeys = Object.keys(objDeviceTransactionData).sort();
    var objPreviousInnerData = null;

    for (j = 0; j < arrKeys.length; j++) {
        propertyKey = arrKeys[j];
        if (!objDeviceTransactionData.hasOwnProperty(propertyKey)) {
            continue;
        }
        objInnerData = objDeviceTransactionData[propertyKey];
        objFormattedData = {};
        for (i = 0; i < objInnerData.length; i++) {
            objCalculations.processValues(objInnerData, i, objInput[devicePropertyKey][strMeterid].managerialdata, objConfig.meter_sm_arrNoProccessKey, propertyKey);
            objCalculations.processArrTimeStampValues(objInnerData, i, objFormattedData, objConfig.meter_sm_arrTimestpampKeys, propertyKey);
            objCalculations.processAverage(objInnerData, i, objFormattedData, objConfig.meter_sm_arrAverageKeys);
            objCalculations.processLastValue(objInnerData, i, objFormattedData, objConfig.meter_sm_arrLastValKeys, propertyKey);
            objCalculations.processSum(objInnerData, i, objFormattedData, objConfig.meter_sm_arrSumKeys);
            objCalculations.processDifference(objInnerData, i, objFormattedData, objConfig.meter_sm_arrDifffernceSumKeys, objPreviousInnerData);
            calculateNetworkResponseRate(objInnerData, i, objFormattedData, ['Meter_NetworkResponseRate']);
        }
        objPreviousInnerData = {};
        objjsFunctions.assignValuesFrmObject(objInput[devicePropertyKey][strMeterid].TranData, objPreviousInnerData, "", [propertyKey], false, false);
        objPreviousInnerData = objPreviousInnerData[propertyKey];
        if (objFaultTranData && objFaultTranData[propertyKey]) {
            var objDataToCalNetworkResRate;
            objDataToCalNetworkResRate = objFaultTranData[propertyKey].concat(objDeviceTransactionData[propertyKey]);
            objFormattedData.Meter_NetworkResponseRate = 0;
            for (i = 0; i < objDataToCalNetworkResRate.length; i++) {
                calculateNetworkResponseRate(objDataToCalNetworkResRate, i, objFormattedData, ['Meter_NetworkResponseRate']);
            }
        }
        objInput[devicePropertyKey][strMeterid].TranData[propertyKey] = objFormattedData;
    }
}

function calculateNetworkResponseRate(objInputData, index, objFormattedData, arrAverageKeys) {
    objCalculations.processAverage(objInputData, index, objFormattedData, arrAverageKeys, objConfig.transScheduler);
}

module.exports = {
    processDataFor: processDataFor
};