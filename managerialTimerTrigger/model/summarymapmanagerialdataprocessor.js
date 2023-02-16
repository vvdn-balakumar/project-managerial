var objdaoimpl = require('../dao/mongodaoimpl.js');
var async = require('async');
var objjsFunctions = require('../util/jsfunctions.js');
var logger = console;

/**
    * @description - Code to get Transformer data by Cell ID
    * @param objRowData - row data
    * @return callback
    */
function getTransformerDataByCellID(objRowData, callback) {
    objRowData = objRowData ? objRowData : {};
    objdaoimpl.getDataFromCollection("DELTA_Transformer", ['TransformerID'], [objRowData.TransformerID], function (err, objSelTransformerData) {
        if (err) {
            callback(err, null);
        } else {
            try {
                if (objSelTransformerData && objSelTransformerData.length > 0) {
                    objRowData.TransformerID = objSelTransformerData[0].TransformerID;
                    objRowData.TransformerSerialNumber = objSelTransformerData[0].TransformerSerialNumber;
                    getCircuitByCircuitID(objSelTransformerData[0].CircuitID, function (err, objCircuitData) {
                        assignValuesFrmObject(objCircuitData, objRowData);
                        callback(null, objRowData);
                    });
                } else {
                    callback(null, objRowData);
                }
            } catch (exc) {
                callback(exc, null);
            }
        }
    });
}

/**
    * @description - Code to get circuit data by circuit ID
    * @param circuitIdVal - circuit id
    * @return callback
    */
function getCircuitByCircuitID(circuitIdVal, callback) {
    var objRowData = {};
    objdaoimpl.getDataFromCollection("DELTA_Circuit", ['CircuitID'], [circuitIdVal], function (err, objSelCircuitData) {
        if (err) {
            callback(err, null);
        } else {
            try {
                objRowData.CircuitID = -1;
                if (objSelCircuitData && objSelCircuitData.length > 0) {
                    objRowData.CircuitID = objSelCircuitData[0].CircuitID;
                    objRowData.CircuitLatitude = objSelCircuitData[0].Latitude;
                    objRowData.CircuitLongitude = objSelCircuitData[0].Longitude;
                }

                callback(null, objRowData);
            } catch (exc) {
                logger.log(exc);
                callback(exc, null);
            }
        }
    });
}
/**
    * @description - Code to get hypersprout data by cell ID
    * @param cellIdVal - cell id
    * @return callback
    */
function getHypersproutDataByCellID(cellIdVal, callback) {
    try {
        var objRowData = {};
        objRowData.cellIdVal = cellIdVal;
        objdaoimpl.getDataFromCollection("DELTA_Hypersprouts", ['HypersproutID'], [cellIdVal], function (err, objSelTransformerData) {
            if (err) {
                callback(err, null);
                return;
            }
            try {
                processHypersproutDataByCellID(objRowData, objSelTransformerData, callback);
            } catch (exc) {
                callback(exc, null);
            }
        });
    } catch (err) {
        callback(err, null);
    }
}
/**
    * @description - Code to get process hypersprout data 
    * @param objRowData - row data
    * @param objSelTransformerData - Transformer data
    * @return callback
    */

function processHypersproutDataByCellID(objRowData, objSelTransformerData, callback) {
    objRowData.TransformerID = -1;
    if (!objSelTransformerData && objSelTransformerData.length < 1) {
        return;
    }

    objRowData.HypersproutSerialNumber = objSelTransformerData[0].HypersproutSerialNumber;
    var objHypersproutComm = objSelTransformerData[0].Hypersprout_Communications;
    if (objHypersproutComm) {
        objRowData.TransformerLatitude = objHypersproutComm.Latitude;
        objRowData.TransformerLongitude = objHypersproutComm.Longitude;
    }
    var objHypersproutDevDetails = objSelTransformerData[0].Hypersprout_DeviceDetails;
    if (objHypersproutDevDetails) {
        objRowData.Transformer_Phase = objHypersproutDevDetails.Phase;
        objRowData.TransformerRating = objHypersproutDevDetails.TransformerRating;
    }
    objRowData.IsHyperHub = objSelTransformerData[0].IsHyperHub;
    objRowData.TransformerID = objSelTransformerData[0].TransformerID;

    getTransformerDataByCellID(objRowData, callback);
}
/**
    * @description - Code to get meter data by meter ID
    * @param cellIdVal - cell id
    * @param meterIdVal - meter id
    * @return callback
    */
function getMeterByMeterID(cellIdVal, meterIdVal, callback) {
    var objRowData = {};
    objdaoimpl.getDataFromCollection("DELTA_Meters", ['MeterID'], [meterIdVal], function (err, objSelMeterData) {
        if (err) {
            callback(err, null);
        } else {
            try {
                if (objSelMeterData && objSelMeterData.length > 0) {
                    for (var i = 0; i < objSelMeterData.length; i++) {
                        objRowData[objSelMeterData[i].MeterID] = {};
                        objRowData[objSelMeterData[i].MeterID].cellId = cellIdVal;
                        objRowData[objSelMeterData[i].MeterID].MeterSerialNumber = objSelMeterData[i].MeterSerialNumber;
                        var objMeterComm = objSelMeterData[i].Meters_Communications;
                        if (objMeterComm) {
                            objRowData[objSelMeterData[i].MeterID].MeterLatitude = objMeterComm.Latitude;
                            objRowData[objSelMeterData[i].MeterID].MeterLongitude = objMeterComm.Longitude;
                        }
                        var objMeterDeviceDetails = objSelMeterData[i].Meters_DeviceDetails;
                        if (objMeterDeviceDetails) {
                            objRowData[objSelMeterData[i].MeterID].Meter_Phase = objMeterDeviceDetails.Phase;
                        }
                        objSelMeterData[i].SolarPanel = objSelMeterData[i].SolarPanel;
                        objRowData[objSelMeterData[i].MeterID].SolarPanel = objSelMeterData[i].SolarPanel ? true : false;
                        objSelMeterData[i].EVMeter = objSelMeterData[i].EVMeter;
                        objRowData[objSelMeterData[i].MeterID].EVMeter = objSelMeterData[i].EVMeter ? true : false;
                    }
                }
                callback(null, objRowData);
            } catch (exc) {
                logger.log(exc);
                callback(exc, null);
            }
        }
    });
}
/**
    * @description - Code to assign value
    * @param objInput - input data
    * @param objOutput - output data
    * @param keyPrefix - key prefix
    * @param arrFilter - array of filter
    * @param isSkipValues - skip values
    * @return callback
    */
function assignValuesFrmObject(objInput, objOutput, keyPrefix, arrFilter, isSkipValues) {
    objjsFunctions.assignValuesFrmObject(objInput, objOutput, keyPrefix, arrFilter, false, isSkipValues);
}
/**
    * @description - Code to create meter dummy object
    * @param objInputData - input data
    * @param strPropertyKey - property key
    * @return callback
    */
function createDummyMeterObj(objInputData, strPropertyKey) {
    objInputData.meterdata[strPropertyKey] = {};
    objInputData.meterdata[strPropertyKey][0] = {};
    objInputData.meterdata[strPropertyKey][0].managerialdata = {};
    objInputData.meterdata[strPropertyKey][0].managerialdata.Meter_CellID = strPropertyKey;
    objInputData.meterdata[strPropertyKey][0].managerialdata.MeterSerialNumber = 0;
    objInputData.meterdata[strPropertyKey][0].MeterLastData = {};
}
/**
    * @description - Code to get managerial data
    * @param objParentData - parent data
    * @return callback
    */
function getManagerialData(objParentData, callback) {
    try {
        var objInput = objParentData.meterdata;
        if (!objInput) {
            return callback('No data', false);
        }
        var objKeyLen = Object.keys(objInput).length;
        var loopIndex = 0;
        if (objKeyLen === 0) {
            return callback('No data', false);
        }
        for (var strLoopPropertyKey in objInput) {
            if (objInput.hasOwnProperty(strLoopPropertyKey)) {
                var arrMeterData = [];
                var strPropertyKey = parseInt(strLoopPropertyKey);
                for (var strMeterIdVal in objInput[strPropertyKey]) {
                    if (objInput[strPropertyKey].hasOwnProperty(strMeterIdVal)) {
                        arrMeterData.push(parseInt(strMeterIdVal));
                    }
                }

                if (objInput.hasOwnProperty(strPropertyKey)) {
                    invokeAsyncTask(strPropertyKey, arrMeterData);
                } else {
                    loopIndex++;
                }
            }
        }
    } catch (err) {
        logger.log(err);
        callback(err, false);
    }
    /**
        * @description - Code to invoke async tasks
        * @param strPropertyKey - property key
        * @param arrMeterData - array of meter data
        * @return callback
        */
    function invokeAsyncTask(strPropertyKey, arrMeterData) {
        async.parallel({
            hypersproutdata: function (innercallback) {
                getHypersproutDataByCellID(strPropertyKey, innercallback);
            },
            meterdata: function (innercallback) {
                getMeterByMeterID(strPropertyKey, arrMeterData, innercallback);
            }
        }, asyncResponse);
    }
    /**
            * @description - Code of async responses
            * @param results - results
            * @return Nil
            */
    function asyncResponse(err, results) {
        try {
            loopIndex++;
            if (results) {

                if (results.meterdata) {
                    var intNumOfMeters = Object.keys(results.meterdata);
                    if (intNumOfMeters < 1 && results.transformerdata) {
                        if (results.hypersproutdata) {
                            assignValuesFrmObject(results.hypersproutdata, objInput[results.transformerdata.cellIdVal][0].managerialdata);
                        }
                        assignValuesFrmObject(results.transformerdata, objInput[results.transformerdata.cellIdVal][0].managerialdata);
                    } else {
                        for (var strMeterId in results.meterdata) {
                            if (results.meterdata.hasOwnProperty(strMeterId)) {
                                var meterCellIdVal = results.meterdata[strMeterId].cellId;
                                if (results.hypersproutdata) {
                                    assignValuesFrmObject(results.hypersproutdata, objInput[meterCellIdVal][strMeterId].managerialdata);
                                }
                                if (results.transformerdata) {
                                    assignValuesFrmObject(results.transformerdata, objInput[meterCellIdVal][strMeterId].managerialdata);
                                }
                                if (results.meterdata) {
                                    assignValuesFrmObject(results.meterdata[strMeterId], objInput[meterCellIdVal][strMeterId].managerialdata);
                                }
                            }
                        }
                    }
                }
            }
            if (loopIndex === objKeyLen) {
                callback(null, true);
            }
        } catch (exc) {
            callback(exc, false);
        }
    }
}


module.exports = {
    getManagerialData: getManagerialData,
    createDummyMeterObj: createDummyMeterObj
};