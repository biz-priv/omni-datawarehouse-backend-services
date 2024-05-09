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
    { tableName: process.env.SHIPPER_TABLE, pKey: "FK_ShipOrderNo", getValues: "FK_ShipOrderNo, ShipName, ShipAddress1, ShipCity, FK_ShipState, ShipZip, FK_ShipCountry", sortName: "shipper" },
    { tableName: process.env.CONSIGNEE_TABLE, pKey: "FK_ConOrderNo", getValues: "FK_ConOrderNo, ConName, ConAddress1, ConCity, FK_ConState, ConZip, FK_ConCountry", sortName: "consignee" },
    { tableName: process.env.SHIPMENT_DESC_TABLE, pKey: "FK_OrderNo", getValues: "FK_OrderNo, Pieces, Weight, ChargableWeight, WeightDimension", sortName: "shipmentDesc" },
    { tableName: process.env.SHIPMENT_MILESTONE_TABLE, pKey: "FK_OrderNo", getValues: "FK_OrderNo, EventDateTime, EventTimeZone, FK_OrderStatusId, FK_ServiceLevelId, Description", sortName: "shipmentMilestone" },
    { tableName: process.env.SHIPMENT_HEADER_TABLE, pKey: "PK_OrderNo", getValues: "PK_OrderNo, Housebill, MasterAirWaybill, ShipmentDateTime, HandlingStation, OrgAirport, DestAirport, ETADateTime, ScheduledDateTime, PODDateTime, PODName, FK_ServiceLevelId, OrderDate, BillNo", sortName: "shipmentHeader" },
    { tableName: process.env.CUSTOMER_ENTITLEMENT_TABLE, pKey: "FileNumber", getValues: "FileNumber, CustomerID, HouseBillNumber", sortName: "customerEntitlement" }
];
const INDEX_VALUES = {
    TRACKING_NOTES: {
        INDEX: process.env.TRACKING_NOTES_TABLE_INDEXVALUE
    }
}

const statusCodes = {
    NEW: "New shipment",
    DEL: "Delivered",
    COB: "COB / intransit",
    AAD: "Arrived at destination",
    PUP: "Picked up",
    TTC: "Shipment tendered to carrier",
    DIS: "Pick up dispatched",
    POD: "Hard copy POD rcv'd",
    OFD: "Out for delivery",
    DCD: "Departed cross dock",
    APD: "Delivery appointment scheduled",
    AAO: "Arrived at Omni destination",
    WEB: "New web shipment",
    BOR: "Booked",
    CCL: "Customs cleared",
    DAR: "Delivery appointment requested",
    DSB: "Docs submitted to broker",
    IS: "ISF filed",
    APU: "Pick up attempt",
    DLA: "Arrived at delivery location",
    REF: "Shipment refused",
    ST: "Tendered to carrier",
    SRS: "Shipment returned to shipper",
    SOS: "Emergency weather delay",
    SDE: "Shipment delayed",
    CAN: "Shipment canceled",
    FLO: "Freight loaded",
    ECC: "Export customs cleared",
    PRE: "Cargo prepared for loading",
    HBC: "Held by customs",
    GIN: "Gate in",
    RLD: "Ready for local delivery",
}
module.exports = {
    customerTypeValue,
    weightDimensionValue,
    tableValues,
    INDEX_VALUES,
    statusCodes
};