'use strict';
const customerTypeValue = {
    S: "Shipper",
    C: "Consignee",
    default: "Billto"
}
const weightDimensionValue = {
    K: "Kg",
    default: "lb"
}
const tableValues = [
    { tableName: process.env.SHIPPER_TABLE, pKey: "FK_ShipOrderNo", getValues: "FK_ShipOrderNo, ShipName, ShipAddress1, ShipCity, FK_ShipState, ShipZip, FK_ShipCountry", sortName: "shipper"},
    { tableName: process.env.CONSIGNEE_TABLE, pKey: "FK_ConOrderNo", getValues: "FK_ConOrderNo, ConName, ConAddress1, ConCity, FK_ConState, ConZip, FK_ConCountry", sortName: "consignee"},
    { tableName: process.env.SHIPMENT_DESC_TABLE, pKey: "FK_OrderNo", getValues: "FK_OrderNo, Pieces, Weight, ChargableWeight, WeightDimension", sortName: "shipmentDesc" },
    { tableName: process.env.REFERENCE_TABLE, pKey: "PK_ReferenceNo", getValues: "PK_ReferenceNo, FK_RefTypeId, CustomerType, ReferenceNo", sortName: "references" },
    { tableName: process.env.SHIPMENT_MILESTONE_TABLE, pKey: "FK_OrderNo", getValues: "FK_OrderNo, EventDateTime, EventTimeZone, FK_OrderStatusId, FK_ServiceLevelId, Description", sortName: "shipmentMilestone" },
    { tableName: process.env.SHIPMENT_HEADER_TABLE, pKey: "PK_OrderNo", getValues: "PK_OrderNo, Housebill, MasterAirWaybill, ShipmentDateTime, HandlingStation, OrgAirport, DestAirport, ETADateTime, ScheduledDateTime, PODDateTime, PODName, FK_ServiceLevelId, OrderDate", sortName: "shipmentHeader"},
  ];
const INDEX_VALUES = {
    TRACKING_NOTES: {
        INDEX: process.env.TRACKING_NOTES_TABLE_INDEXVALUE
    }
}
module.exports = {
    customerTypeValue,
    weightDimensionValue,
    tableValues,
    INDEX_VALUES
  };