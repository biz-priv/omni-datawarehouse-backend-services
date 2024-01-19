'use strict';
const _ = require('lodash');
const AWS = require('aws-sdk');
const moment = require('moment-timezone');
const { getLocationId, createLocation } = require('./apis');

const dynamoDB = new AWS.DynamoDB.DocumentClient({
  region: 'us-east-1',
});

const refTypeMap = {
  'ACT': '11',
  'AWB': 'AW',
  'ASP': 'ASP',
  'AUT': 'BB',
  'BOL': 'BM',
  'BOO': 'BN',
  'CHA': '80',
  'CTN': 'OC',
  'CUS': 'IT',
  'APD': 'AO',
  'DBL#': 'KK',
  'DL#': 'KK',
  'DO': 'DO',
  'DEP': 'DP',
  'GLC': 'GZ',
  'INV': 'IK',
  'IN1': 'IK',
  'JOB': '9R',
  'LOA': 'LO',
  'LOT': 'LT',
  'MAN': 'MA',
  'ORD': 'OQ',
  'PU': 'P8',
  'APP': 'AO',
  'PRO': '2I',
  'PRD': 'OZ',
  'PRJ': 'JB',
  'PO': 'PO',
  'QUO': 'Q1',
  'REF': 'CR',
  'REG': 'ACB',
  'RO': 'Q9',
  'RID': 'ACE',
  'RMA': 'QJ',
  'ROU': 'RU',
  'SO': 'SW',
  'SAL': 'SA',
  'SEA': 'SN',
  'SN': 'SE',
  'SID': '2I',
  'SKU': 'S6',
  'ST': 'ST',
  'STR': 'ST',
  'TN#': 'TF',
  'TRL': 'EQ',
  'TO': 'TF',
  'WOR': 'WO',
  'ZON': '6K',
};

// Function to get powerbroker code based on reftypeId
function getPowerBrokerCode(reftypeId) {
  return _.get(refTypeMap, reftypeId, 'Code Not Found');
}

function generateReferenceNumbers(references) {
  return _.map(references, (ref) => ({
    __type: 'reference_number',
    __name: 'referenceNumbers',
    company_id: 'TMS',
    element_id: '128',
    partner_id: 'TMS',
    reference_number: _.get(ref, 'ReferenceNo', ''),
    reference_qual: getPowerBrokerCode(_.get(ref, 'FK_RefTypeId', '')),
    send_to_driver: true,
    version: '004010',
  }));
}

async function formatTimestamp(eventdatetime) {
  const date = moment(eventdatetime);
  const week = date.week();
  const offset = week >= 11 && week <= 44 ? '-0500' : '-0600';
  return date.format('YYYYMMDDHHmmss') + offset;
}

async function getFormattedDateTime(dateTime, state, tblTimeZoneMaster) {
  if (!dateTime) {
    return '';
  }

  let offset;
  if (state === 'AZ') {
    offset = '-0600';
  } else if (moment(dateTime).isoWeek() >= 11 && moment(dateTime).isoWeek() <= 44) {
    offset = '-0500';
  } else {
    offset = '-0600';
  }
  //   state = 'AZ'
  //     ? '-0600'
  //     : moment(dateTime).isoWeek() >= 11 && moment(dateTime).isoWeek() <= 44
  //       ? '-0500'
  //       : '-0600';
  console.info('🙂 -> file: helper.js:112 -> getFormattedDateTime -> offset:', offset);
  const hoursAway = _.get(tblTimeZoneMaster, 'Items[0].HoursAway', 0) || 0;
  console.info('🙂 -> file: helper.js:106 -> getFormattedDateTime -> hoursAway:', hoursAway);
  const formattedDateTime = moment(dateTime).add({ hours: hoursAway }).format('YYYYMMDDHHmmss');
  console.info(
    '🙂 -> file: helper.js:109 -> getFormattedDateTime -> formattedDateTime:',
    formattedDateTime
  );

  return formattedDateTime + offset;
}

async function generateStop(
  shipmentHeader,
  references,
  orderSequence,
  stopType,
  locationId,
  stateData
) {
  console.info(
    `🙂 -> file: helper.js:122 ->   shipmentHeader,
  references,
  orderSequence,
  stopType,
  locationId,
  stateData:`,
    shipmentHeader,
    references,
    orderSequence,
    stopType,
    locationId,
    stateData
  );

  const pickupTimeZone = _.get(shipmentHeader, 'ReadyDateTimeZone', '');
  const deliveryTimeZone = _.get(shipmentHeader, 'ScheduledDateTimeZone', '');
  let schedArriveEarly = '';
  let schedArriveLate = '';
  let timeZone = '';
  let state = '';
  if (stopType === 'PU') {
    // For Pickup (PU)
    schedArriveEarly = _.get(shipmentHeader, 'ReadyDateTime', '');
    schedArriveLate = _.get(shipmentHeader, 'ReadyDateTimeRange', '');
    timeZone = pickupTimeZone;
    state = _.get(stateData, 'FK_ShipState', '');
  } else if (stopType === 'SO') {
    // For Delivery (SO)
    schedArriveEarly = _.get(shipmentHeader, 'ScheduledDateTime', '');
    console.info('🙂 -> file: helper.js:150 -> schedArriveEarly:', schedArriveEarly);
    schedArriveLate = _.get(shipmentHeader, 'ScheduledDateTimeRange', '');
    console.info('🙂 -> file: helper.js:152 -> schedArriveLate:', schedArriveLate);
    timeZone = deliveryTimeZone;
    state = _.get(stateData, 'FK_ConState', '');
  }
  const timezoneParams = await getParamsByTableName(
    '5344583',
    'omni-wt-rt-timezone-master-dev',
    timeZone
  );
  console.info('🙂 -> file: helper.js:161 -> timezoneParams:', timezoneParams);
  const timeZoneMaster = await queryDynamoDB(timezoneParams);

  return {
    __type: 'stop',
    __name: 'stops',
    company_id: 'TMS',
    appt_required: false,
    confirmed: false,
    driver_load_unload: 'N',
    location_id: locationId,
    sched_arrive_early: await getFormattedDateTime(schedArriveEarly, state, timeZoneMaster),
    sched_arrive_late: await getFormattedDateTime(schedArriveLate, state, timeZoneMaster),
    late_eta_colorcode: false,
    status: 'A',
    order_sequence: orderSequence,
    stop_type: stopType,
    requested_service: false,
    prior_uncleared_stops: false,
    referenceNumbers: generateReferenceNumbers(references),
    stopNotes: [
      {
        __type: 'stop_note',
        __name: 'stopNotes',
        company_id: 'TMS',
        comment_type: 'DC',
        comments: 'test',
      },
    ],
  };
}

function getParamsByTableName(orderNo, tableName, timezone, billno) {
  switch (tableName) {
    case 'omni-wt-rt-confirmation-cost-dev':
      return {
        TableName: 'omni-wt-rt-confirmation-cost-dev',
        IndexName: 'omni-wt-confirmation-cost-orderNo-index-dev',
        KeyConditionExpression: 'FK_OrderNo = :orderNo',
        ExpressionAttributeValues: {
          ':orderNo': orderNo,
        },
      };
    case 'omni-wt-rt-shipment-header-dev':
      return {
        TableName: 'omni-wt-rt-shipment-header-dev',
        KeyConditionExpression: 'PK_OrderNo = :orderNo',
        FilterExpression: 'ShipQuote = :shipquote',
        ExpressionAttributeValues: {
          ':orderNo': orderNo,
          ':shipquote': 'S',
        },
      };
    case 'omni-wt-rt-references-dev':
      return {
        TableName: 'omni-wt-rt-references-dev',
        IndexName: 'omni-wt-rt-ref-orderNo-index-dev',
        KeyConditionExpression: 'FK_OrderNo = :FK_OrderNo',
        ExpressionAttributeValues: {
          ':FK_OrderNo': orderNo,
        },
      };
    case 'omni-wt-rt-shipper-dev':
      return {
        TableName: 'omni-wt-rt-shipper-dev',
        KeyConditionExpression: 'FK_ShipOrderNo = :orderNo',
        ExpressionAttributeValues: {
          ':orderNo': orderNo,
        },
      };
    case 'omni-wt-rt-consignee-dev':
      return {
        TableName: 'omni-wt-rt-consignee-dev',
        KeyConditionExpression: 'FK_ConOrderNo = :orderNo',
        ExpressionAttributeValues: {
          ':orderNo': orderNo,
        },
      };
    case 'omni-wt-rt-shipment-apar-dev':
      return {
        TableName: 'omni-wt-rt-shipment-apar-dev',
        KeyConditionExpression: 'FK_OrderNo = :orderNo',
        ExpressionAttributeValues: {
          ':orderNo': orderNo,
        },
      };
    case 'omni-wt-rt-shipment-desc-dev':
      return {
        TableName: 'omni-wt-rt-shipment-desc-dev',
        KeyConditionExpression: 'FK_OrderNo = :orderNo',
        ExpressionAttributeValues: {
          ':orderNo': orderNo,
        },
      };
    case 'omni-wt-rt-timezone-master-dev':
      return {
        TableName: 'omni-wt-rt-timezone-master-dev',
        KeyConditionExpression: 'PK_TimeZoneCode = :code',
        ExpressionAttributeValues: {
          ':code': timezone,
        },
      };
    case 'omni-wt-rt-customers-dev':
      return {
        TableName: 'omni-wt-rt-customers-dev',
        KeyConditionExpression: 'PK_CustNo = :PK_CustNo',
        ExpressionAttributeValues: {
          ':PK_CustNo': billno,
        },
      };
    // case INSTRUCTIONS_TABLE:
    //     return {
    //         TableName: INSTRUCTIONS_TABLE,
    //         IndexName: INSTRUCTIONS_INDEX_KEY_NAME,
    //         KeyConditionExpression: "FK_OrderNo = :FK_OrderNo",
    //         ExpressionAttributeValues: {
    //             ":FK_OrderNo": orderNo,
    //         },
    //     };
    // case EQUIPMENT_TABLE:
    //     return {
    //         TableName: EQUIPMENT_TABLE,
    //         KeyConditionExpression: "PK_EquipmentCode = :PK_EquipmentCode",
    //         ExpressionAttributeValues: {
    //             ":PK_EquipmentCode": orderNo,
    //         },
    //     };
    default:
      return false;
  }
}

async function queryDynamoDB(params) {
  try {
    const result = await dynamoDB.query(params).promise();
    return result;
  } catch (error) {
    console.error('Error querying DynamoDB:', error);
    throw error;
  }
}

async function fetchLocationId({ confirmationCostData, shipperData, consigneeData }) {
  // Use confirmationCostData if available, otherwise use data from other tables
  let finalShipperData;
  if (_.isEmpty(confirmationCostData)) {
    finalShipperData = shipperData;
  } else {
    finalShipperData = confirmationCostData;
  }

  let finalConsigneeData;
  if (_.isEmpty(confirmationCostData)) {
    finalConsigneeData = consigneeData;
  } else {
    finalConsigneeData = confirmationCostData;
  }

  // const shipperLocationId = 'SAFEHOT1';
  // const consigneeLocationId = 'SAFEATG1';
  
  // Get location ID for shipper
  let shipperLocationId = await getLocationId(
    finalShipperData.ShipName,
    finalShipperData.ShipAddress1,
    finalShipperData.ShipAddress2,
    finalShipperData.ShipCity,
    finalShipperData.FK_ShipState,
    finalShipperData.ShipZip
  );

  // Get location ID for consignee
  let consigneeLocationId = await getLocationId(
    finalConsigneeData.ConName,
    finalConsigneeData.ConAddress1,
    finalConsigneeData.ConAddress2,
    finalConsigneeData.ConCity,
    finalConsigneeData.FK_ConState,
    finalConsigneeData.ConZip
  );

  // Use the obtained location IDs to create locations if needed
  if (!shipperLocationId) {
    shipperLocationId = await createLocation({
      __type: 'location',
      company_id: 'TMS',
      address1: finalShipperData.ShipAddress1,
      address2: finalShipperData.ShipAddress2,
      city_name: finalShipperData.ShipCity,
      is_active: true,
      name: finalShipperData.ShipName,
      state: finalShipperData.FK_ShipState,
      zip_code: finalShipperData.ShipZip,
    });
  }

  if (!consigneeLocationId) {
    consigneeLocationId = await createLocation({
      __type: 'location',
      company_id: 'TMS',
      address1: finalConsigneeData.ConAddress1,
      address2: finalConsigneeData.ConAddress2,
      city_name: finalConsigneeData.ConCity,
      is_active: true,
      name: finalConsigneeData.ConName,
      state: finalConsigneeData.FK_ConState,
      zip_code: finalConsigneeData.ConZip,
    });
  }

  return { shipperLocationId, consigneeLocationId };
}

module.exports = {
  getPowerBrokerCode,
  generateReferenceNumbers,
  formatTimestamp,
  getFormattedDateTime,
  generateStop,
  getParamsByTableName,
  queryDynamoDB,
  fetchLocationId,
};
