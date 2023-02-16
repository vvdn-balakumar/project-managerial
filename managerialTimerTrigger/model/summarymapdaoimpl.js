var objdaoimpl = require('../dao/mysqldaoimpl.js');
var moment = require("moment");
var objSummaryMapModel = require('../model/sqltables/summarymapmodel.js');
var objLatestTransactionModel = require('../model/sqltables/latesttransactionmodel.js');

/**
    * @description - Code to update summary map data
    * @param objDeviceMergedData - merged data
    * @param objHSKey - HS key
    * @param objDeviceID - device id
    * @param objPropertyKey - property key
    * @return Nil
    */
function updateSummaryMapModel(objDeviceMergedData, objHSKey, objDeviceID, objPropertyKey, callback) {
    var objLooppedData = objDeviceMergedData[objHSKey][objDeviceID].TranData;
    var objLooppedTransformerData = objDeviceMergedData[objHSKey].TransformerData;
    objLooppedData = objPropertyKey === 'MeterLastData' ? objDeviceMergedData[objHSKey][objDeviceID] : objLooppedData;

    if (objPropertyKey === 'MeterLastData' && !objLooppedData[objPropertyKey]) {
        return 0;
    }

    if (objPropertyKey !== 'managerialdata') {
        var objManagerialData = objDeviceMergedData[objHSKey][objDeviceID].managerialdata;
        var objTransactionData = objLooppedData[objPropertyKey];
        objLooppedTransformerData = objLooppedTransformerData ? objLooppedTransformerData : {};
        var objTransfomerTransactionData = objLooppedTransformerData[objPropertyKey];
        objManagerialData = !objManagerialData ? {} : objManagerialData;
        objTransactionData = !objTransactionData ? {} : objTransactionData;
        objTransfomerTransactionData = !objTransfomerTransactionData ? {} : objTransfomerTransactionData;

        var objectToInsert = {};

        if (objManagerialData.MeterSerialNumber || objManagerialData.Transformer_CellID) {
            objectToInsert.MeterID = objManagerialData.MeterSerialNumber;
            objectToInsert.HypersproutID = objManagerialData.HypersproutSerialNumber;
            objectToInsert.Meter_DeviceID = objManagerialData.Meter_DeviceID;
            if (objManagerialData.MeterSerialNumber === 0 && !objTransactionData.Meter_ReadTimestamp) {
                objTransactionData.Meter_ReadTimestamp = objTransactionData.Transformer_ReadTimestamp;
            }
            objTransactionData.Meter_ReadTimestamp.setUTCMinutes(0);
            objTransactionData.Meter_ReadTimestamp.setUTCSeconds(0);
            objectToInsert.Meter_ReadTimestamp = objTransactionData.Meter_ReadTimestamp ? moment(objTransactionData.Meter_ReadTimestamp, 'DD-MM-YYYY HH:mm:ss').utc() : null;
            objectToInsert.ActualMeter_ReadTimestamp = objTransactionData.ActualMeter_ReadTimestamp ? moment(objTransactionData.ActualMeter_ReadTimestamp, 'DD-MM-YYYY HH:mm:ss').utc() : null;
            objectToInsert.ActualTransformer_ReadTimestamp = objTransfomerTransactionData.ActualTransformer_ReadTimestamp ? moment(objTransfomerTransactionData.ActualTransformer_ReadTimestamp, 'DD-MM-YYYY HH:mm:ss').utc() : null;
            objectToInsert.TransformerID = objManagerialData.TransformerSerialNumber;
            objectToInsert.CircuitID = objManagerialData.CircuitID;
            objectToInsert.CircuitLatitude = objManagerialData.CircuitLatitude ? parseFloat(objManagerialData.CircuitLatitude).toFixed(7) : 0;
            objectToInsert.CircuitLongitude = objManagerialData.CircuitLongitude ? parseFloat(objManagerialData.CircuitLongitude).toFixed(7) : 0;
            objectToInsert.TransformerLatitude = objManagerialData.TransformerLatitude ? parseFloat(objManagerialData.TransformerLatitude).toFixed(7) : 0;
            objectToInsert.TransformerLongitude = objManagerialData.TransformerLongitude ? parseFloat(objManagerialData.TransformerLongitude).toFixed(7) : 0;
            objectToInsert.MeterLatitude = objManagerialData.MeterLatitude ? parseFloat(objManagerialData.MeterLatitude).toFixed(7) : 500.0000000;
            objectToInsert.MeterLongitude = objManagerialData.MeterLongitude ? parseFloat(objManagerialData.MeterLongitude).toFixed(7) : 500.0000000;
            objectToInsert.Transformer_CellID = objManagerialData.Transformer_CellID ? objManagerialData.Transformer_CellID : objHSKey;
            if (objTransfomerTransactionData.Transformer_ReadTimestamp) {
                objTransfomerTransactionData.Transformer_ReadTimestamp.setUTCMinutes(0);
                objTransfomerTransactionData.Transformer_ReadTimestamp.setUTCSeconds(0);
            }
            objectToInsert.Transformer_ReadTimestamp = objTransfomerTransactionData.Transformer_ReadTimestamp ? moment(objTransfomerTransactionData.Transformer_ReadTimestamp, 'DD-MM-YYYY HH:mm:ss').utc() : objectToInsert.Meter_ReadTimestamp;

            objectToInsert.Meter_Line1InstVoltage = objTransactionData.Meter_Line1InstVoltage ? objTransactionData.Meter_Line1InstVoltage.toFixed(7) : null;
            objectToInsert.Meter_Line1InstCurrent = objTransactionData.Meter_Line1InstCurrent ? objTransactionData.Meter_Line1InstCurrent.toFixed(7) : null;
            objectToInsert.Meter_Line1Frequency = objTransactionData.Meter_Line1Frequency ? objTransactionData.Meter_Line1Frequency.toFixed(7) : null;
            objectToInsert.Meter_Apparent_m_Total = objTransactionData.Meter_Apparent_m_Total ? objTransactionData.Meter_Apparent_m_Total.toFixed(3) : null;
            objectToInsert.Meter_ActiveReceivedCumulativeRate_Total = objTransactionData.Meter_ActiveReceivedCumulativeRate_Total ? objTransactionData.Meter_ActiveReceivedCumulativeRate_Total.toFixed(6) : null;
            objectToInsert.Meter_ActiveDeliveredCumulativeRate_Total = objTransactionData.Meter_ActiveDeliveredCumulativeRate_Total ? objTransactionData.Meter_ActiveDeliveredCumulativeRate_Total.toFixed(6) : null;

            objectToInsert.Transformer_Line1Voltage = objTransfomerTransactionData.Transformer_Line1Voltage ? objTransfomerTransactionData.Transformer_Line1Voltage : null;
            objectToInsert.Transformer_Line2Voltage = objTransfomerTransactionData.Transformer_Line2Voltage ? objTransfomerTransactionData.Transformer_Line2Voltage : null;
            objectToInsert.Transformer_Line3Voltage = objTransfomerTransactionData.Transformer_Line3Voltage ? objTransfomerTransactionData.Transformer_Line3Voltage : null;
            objectToInsert.Transformer_Line1Current = objTransfomerTransactionData.Transformer_Line1Current ? objTransfomerTransactionData.Transformer_Line1Current.toFixed(2) : null;
            objectToInsert.Transformer_Line2Current = objTransfomerTransactionData.Transformer_Line2Current ? objTransfomerTransactionData.Transformer_Line2Current.toFixed(2) : null;
            objectToInsert.Transformer_Line3Current = objTransfomerTransactionData.Transformer_Line3Current ? objTransfomerTransactionData.Transformer_Line3Current.toFixed(2) : null;
            objectToInsert.Transformer_AmbientTemperarture = objTransfomerTransactionData.Transformer_AmbientTemperarture ? objTransfomerTransactionData.Transformer_AmbientTemperarture.toFixed(7) : null;
            objectToInsert.Transformer_TopTemperature = objTransfomerTransactionData.Transformer_TopTemperature ? objTransfomerTransactionData.Transformer_TopTemperature.toFixed(7) : null;
            objectToInsert.Transformer_BottomTemperature = objTransfomerTransactionData.Transformer_BottomTemperature ? objTransfomerTransactionData.Transformer_BottomTemperature.toFixed(7) : null;
            objectToInsert.Transformer_TransformerOilLevel = objTransfomerTransactionData.Transformer_TransformerOilLevel ? objTransfomerTransactionData.Transformer_TransformerOilLevel : null;
            objectToInsert.Transformer_Apparent_m_Total = objTransfomerTransactionData.Transformer_Apparent_m_Total ? objTransfomerTransactionData.Transformer_Apparent_m_Total.toFixed(3) : null;
            objectToInsert.Transformer_ActiveReceivedCumulativeRate_Total = objTransfomerTransactionData.Transformer_ActiveReceivedCumulativeRate_Total ? objTransfomerTransactionData.Transformer_ActiveReceivedCumulativeRate_Total : null;
            objectToInsert.Transformer_ActiveDeliveredCumulativeRate_Total = objTransfomerTransactionData.Transformer_ActiveDeliveredCumulativeRate_Total ? objTransfomerTransactionData.Transformer_ActiveDeliveredCumulativeRate_Total : null;


            objectToInsert.DateTime = objTransactionData.Meter_ReadTimestamp ? moment(objTransactionData.Meter_ReadTimestamp, 'DD-MM-YYYY HH:mm:ss').utc() : null;
            objectToInsert.Hours = objTransactionData.Meter_ReadTimestamp ? moment(objTransactionData.Meter_ReadTimestamp, 'DD-MM-YYYY HH:mm:ss').utc().hours() : null;
            objectToInsert["Circuit ID"] = objManagerialData.CircuitID;
            objectToInsert.Circuit_Latitude = objManagerialData.CircuitLatitude ? parseFloat(objManagerialData.CircuitLatitude).toFixed(7) : 0;
            objectToInsert.Circuit_Longitude = objManagerialData.CircuitLongitude ? parseFloat(objManagerialData.CircuitLongitude).toFixed(7) : 0;
            objectToInsert["Transformer ID"] = objManagerialData.TransformerSerialNumber;
            objectToInsert["Hypersprout ID"] = objManagerialData.HypersproutSerialNumber;
            objectToInsert.Transformer_Latitude = objManagerialData.TransformerLatitude ? parseFloat(objManagerialData.TransformerLatitude).toFixed(7) : 0;
            objectToInsert.Transformer_Longitude = objManagerialData.TransformerLongitude ? parseFloat(objManagerialData.TransformerLongitude).toFixed(7) : 0;
            objectToInsert["Meter ID"] = objManagerialData.MeterSerialNumber;
            objectToInsert.Meter_Latitude = objManagerialData.MeterLatitude ? parseFloat(objManagerialData.MeterLatitude).toFixed(7) : 0;
            objectToInsert.Meter_Longitude = objManagerialData.MeterLongitude ? parseFloat(objManagerialData.MeterLongitude).toFixed(7) : 0;
            objectToInsert.SolarPanel = objManagerialData.SolarPanel;
            objectToInsert.EVMeter = objManagerialData.EVMeter;
            objectToInsert.Transformer_active_energy_received = objTransfomerTransactionData.Transformer_ActiveReceivedCumulativeRate_Total ? objTransfomerTransactionData.Transformer_ActiveReceivedCumulativeRate_Total : null;
            objectToInsert.Transformer_active_energy_delivered = objTransfomerTransactionData.Transformer_ActiveDeliveredCumulativeRate_Total ? objTransfomerTransactionData.Transformer_ActiveDeliveredCumulativeRate_Total : null;
            objectToInsert.Meter_active_energy_received = objTransactionData.Meter_ActiveReceivedCumulativeRate_Total ? objTransactionData.Meter_ActiveReceivedCumulativeRate_Total.toFixed(6) : null;
            objectToInsert.Meter_active_energy_delivered = objTransactionData.Meter_ActiveDeliveredCumulativeRate_Total ? objTransactionData.Meter_ActiveDeliveredCumulativeRate_Total.toFixed(6) : null;
            objectToInsert.Top_Temperature = objTransfomerTransactionData.Transformer_TopTemperature ? objTransfomerTransactionData.Transformer_TopTemperature.toFixed(7) : null;
            objectToInsert.Bottom_Temperature = objTransfomerTransactionData.Transformer_BottomTemperature ? objTransfomerTransactionData.Transformer_BottomTemperature.toFixed(7) : null;
            objectToInsert.ambient_temparature = objTransfomerTransactionData.Transformer_AmbientTemperarture ? objTransfomerTransactionData.Transformer_AmbientTemperarture.toFixed(7) : null;
            objectToInsert.Energy_Apparent_Absolute = objTransfomerTransactionData.Transformer_Apparent_m_Total ? objTransfomerTransactionData.Transformer_Apparent_m_Total.toFixed(3) : null;
            objectToInsert.Date = objTransactionData.Meter_ReadTimestamp ? moment(objTransactionData.Meter_ReadTimestamp, 'DD-MM-YYYY HH:mm:ss').utc() : null;
            objectToInsert.TransformerActiveReceivedCumulativeRate_Total = objTransfomerTransactionData.Transformer_ActiveReceivedCumulativeRate_Total ? objTransfomerTransactionData.Transformer_ActiveReceivedCumulativeRate_Total : null;
            objectToInsert.TransformerActiveDeliveredCumulativeRate_Total = objTransfomerTransactionData.Transformer_ActiveDeliveredCumulativeRate_Total ? objTransfomerTransactionData.Transformer_ActiveDeliveredCumulativeRate_Total : null;
            objectToInsert.MeterActiveReceivedCumulativeRate_Total = objTransactionData.Meter_ActiveReceivedCumulativeRate_Total ? objTransactionData.Meter_ActiveReceivedCumulativeRate_Total.toFixed(6) : null;
            objectToInsert.MeterActiveDeliveredCumulativeRate_Total = objTransactionData.Meter_ActiveDeliveredCumulativeRate_Total ? objTransactionData.Meter_ActiveDeliveredCumulativeRate_Total.toFixed(6) : null;

            objectToInsert.NetworkResponceRate = objTransactionData.Meter_NetworkResponseRate ? parseFloat(objTransactionData.Meter_NetworkResponseRate).toFixed(2) : 0;
            objectToInsert.TopTemperature = objTransfomerTransactionData.Transformer_TopTemperature ? objTransfomerTransactionData.Transformer_TopTemperature.toFixed(7) : null;
            objectToInsert.BottomTemperature = objTransfomerTransactionData.Transformer_BottomTemperature ? objTransfomerTransactionData.Transformer_BottomTemperature.toFixed(7) : null;
            objectToInsert.AmbientTemperarture = objTransfomerTransactionData.Transformer_AmbientTemperarture ? objTransfomerTransactionData.Transformer_AmbientTemperarture.toFixed(7) : null;
            objectToInsert.Apparent_m_Total = objTransfomerTransactionData.Transformer_Apparent_m_Total ? objTransfomerTransactionData.Transformer_Apparent_m_Total.toFixed(3) : null;
            objectToInsert.Circuit_Id = objManagerialData.CircuitID;
            objectToInsert.Tranformer_Id = objManagerialData.TransformerSerialNumber;
            objectToInsert.Hypersprout_ID = objManagerialData.HypersproutSerialNumber;
            objectToInsert.Meter_Id = objManagerialData.MeterSerialNumber;
            // Commented based on shobin/mumbai team request
            objectToInsert.Non_technichal_Loss = objTransfomerTransactionData.Non_Technical_Loss;
            // objectToInsert.Non_technichal_Loss = 0;

            objectToInsert.Meter_Line2InstVoltage = objTransactionData.Meter_Line2InstVoltage ? parseFloat(objTransactionData.Meter_Line2InstVoltage).toFixed(7) : null;
            objectToInsert.Meter_Line3InstVoltage = objTransactionData.Meter_Line3InstVoltage ? parseFloat(objTransactionData.Meter_Line3InstVoltage).toFixed(7) : null;
            objectToInsert.Meter_Line2InstCurrent = objTransactionData.Meter_Line2InstCurrent ? parseFloat(objTransactionData.Meter_Line2InstCurrent).toFixed(7) : null;
            objectToInsert.Meter_Line3InstCurrent = objTransactionData.Meter_Line3InstCurrent ? parseFloat(objTransactionData.Meter_Line3InstCurrent).toFixed(7) : null;
            objectToInsert.Meter_Line2Frequency = objTransactionData.Meter_Line2Frequency ? parseFloat(objTransactionData.Meter_Line2Frequency).toFixed(7) : null;
            objectToInsert.Meter_Line3Frequency = objTransactionData.Meter_Line3Frequency ? parseFloat(objTransactionData.Meter_Line3Frequency).toFixed(7) : null;
            objectToInsert.Meter_Phase = objManagerialData.Meter_Phase ? objManagerialData.Meter_Phase : null;
            objectToInsert.Meter_Line1PowerFactor = objTransactionData.Meter_Line1PowerFactor ? parseFloat(objTransactionData.Meter_Line1PowerFactor).toFixed(7) : null;
            objectToInsert.Meter_Line2PowerFactor = objTransactionData.Meter_Line2PowerFactor ? parseFloat(objTransactionData.Meter_Line2PowerFactor).toFixed(7) : null;
            objectToInsert.Meter_Line3PowerFactor = objTransactionData.Meter_Line3PowerFactor ? parseFloat(objTransactionData.Meter_Line3PowerFactor).toFixed(7) : null;

            objectToInsert.Transformer_Phase = objManagerialData.Transformer_Phase ? objManagerialData.Transformer_Phase : null;
            objectToInsert.Transformer_Line1PhaseAngle = objTransfomerTransactionData.Transformer_Line1PhaseAngle ? parseFloat(objTransfomerTransactionData.Transformer_Line1PhaseAngle).toFixed(7) : null;
            objectToInsert.Transformer_Line2PhaseAngle = objTransfomerTransactionData.Transformer_Line2PhaseAngle ? parseFloat(objTransfomerTransactionData.Transformer_Line2PhaseAngle).toFixed(7) : null;
            objectToInsert.Transformer_Line3PhaseAngle = objTransfomerTransactionData.Transformer_Line3PhaseAngle ? parseFloat(objTransfomerTransactionData.Transformer_Line3PhaseAngle).toFixed(7) : null;
            objectToInsert.Transformer_BatteryVoltage = objTransfomerTransactionData.Transformer_BatteryVoltage ? objTransfomerTransactionData.Transformer_BatteryVoltage : null;
            objectToInsert.Transformer_BatteryStatus = objTransfomerTransactionData.Transformer_BatteryStatus ? objTransfomerTransactionData.Transformer_BatteryStatus : null;

            objectToInsert.TransformerRating = objManagerialData.TransformerRating ? objManagerialData.TransformerRating : null;
            objectToInsert.IsHyperHub = objManagerialData.IsHyperHub ? true : false;

            if (!objectToInsert["Circuit ID"] && !objectToInsert["Transformer ID"]) {
                return 0;
            }

            if (objPropertyKey === 'MeterLastData') {
                objdaoimpl.insertData("latesttransactions", objLatestTransactionModel.objLatestTrans,
                    objLatestTransactionModel.objTableProps,
                    objectToInsert, function (err, objMeterTransData) {
                        if (err) {
                            console.error("Error", err);
                        }
                        callback(err, objMeterTransData);
                    });
            } else {
                objdaoimpl.insertData("summarymap", objSummaryMapModel.objSummaryMap,
                    objSummaryMapModel.objTableProps,
                    objectToInsert, function (err, objTransformerTransData) {
                        callback(err, objTransformerTransData);
                    });
            }
        } else {
            return 0;
        }
    } else {
        return 0;
    }
    return 1;
}

module.exports = {
    updateSummaryMapModel: updateSummaryMapModel
};