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

function generateReferenceNumbers({ references }) {
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

function formatTimestamp(eventdatetime) {
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
  stateData,
  confirmationCostData,
  type,
  userData
) {
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
  const timezoneParams = getParamsByTableName(
    '5344583',
    'omni-wt-rt-timezone-master-dev',
    timeZone
  );
  console.info('🙂 -> file: helper.js:161 -> timezoneParams:', timezoneParams);
  const timeZoneMaster = await queryDynamoDB(timezoneParams);

  const stopData = {
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
    stopNotes: [
      {
        __type: 'stop_note',
        __name: 'stopNotes',
        company_id: 'TMS',
        comment_type: 'DC',
        comments: _.get(confirmationCostData, type === 'shipper' ? 'PickupNote' : 'DeliveryNote'),
      },
    ],
  };

  if (type === 'shipper') {
    _.set(stopData, 'stopNotes[1]', {
      __type: 'stop_note',
      __name: 'stopNotes',
      company_id: 'TMS',
      comment_type: 'OC',
      comments: _.get(userData, 'UserEmail'),
    });
    _.set(stopData, 'referenceNumbers', generateReferenceNumbers({ references }));
  }
  return stopData;
}

function getParamsByTableName(orderNo, tableName, timezone, billno, userId, consoleNo) {
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
    case 'omni-wt-rt-shipment-header-non-console':
      return {
        TableName: 'omni-wt-rt-shipment-header-dev',
        KeyConditionExpression: 'PK_OrderNo = :orderNo',
        FilterExpression: 'ShipQuote = :shipquote',
        ExpressionAttributeValues: {
          ':orderNo': orderNo,
          ':shipquote': 'S',
        },
      };
    case 'omni-wt-rt-shipment-header-console':
      return {
        TableName: 'omni-wt-rt-shipment-header-dev',
        KeyConditionExpression: 'PK_OrderNo = :orderNo',
        ExpressionAttributeValues: {
          ':orderNo': orderNo,
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
    case 'omni-wt-rt-shipment-apar-console':
      return {
        TableName: 'omni-wt-rt-shipment-apar-dev',
        KeyConditionExpression: 'FK_OrderNo = :orderNo',
        FilterExpression: 'Consolidation = :consolidation',
        ExpressionAttributeValues: {
          ':orderNo': orderNo,
          ':consolidation': 'N',
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
    case 'omni-wt-rt-users':
      return {
        TableName: 'omni-wt-rt-users-dev',
        KeyConditionExpression: 'PK_UserId = :PK_UserId',
        ExpressionAttributeValues: {
          ':PK_UserId': userId,
        },
      };
    case 'omni-wt-rt-tracking-notes':
      return {
        TableName: 'omni-wt-rt-tracking-notes-dev',
        IndexName: 'omni-tracking-notes-console-index-dev',
        KeyConditionExpression: 'ConsolNo = :ConsolNo',
        ExpressionAttributeValues: {
          ':ConsolNo': consoleNo,
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

async function fetchLocationId({ finalShipperData, finalConsigneeData }) {
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

function getFinalShipperAndConsigneeData({ confirmationCostData, shipperData, consigneeData }) {
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
  return { finalShipperData, finalConsigneeData };
}

async function fetchAparTableForConsole({ orderNo }) {
  try {
    const aparParamsForConsole = getParamsByTableName(orderNo, 'omni-wt-rt-shipment-apar-console');
    return _.get(await queryDynamoDB(aparParamsForConsole), 'Items', false);
  } catch (e) {
    console.error('🙂 -> file: helper.js:415 -> e:', e);
    return [];
  }
}

async function fetchCommonTableData({ shipmentAparData }) {
  const tables = [
    'omni-wt-rt-confirmation-cost-dev',
    'omni-wt-rt-shipper-dev',
    'omni-wt-rt-consignee-dev',
  ];

  try {
    const [confirmationCostData, shipperData, consigneeData] = await Promise.all(
      tables.map(async (table) => {
        const param = getParamsByTableName(_.get(shipmentAparData, 'FK_OrderNo'), table);
        console.info('🙂 -> file: index.js:35 -> tables.map -> param:', table, param);
        const response = await queryDynamoDB(param);
        return _.get(response, 'Items', false);
      })
    );
    return { confirmationCostData, shipperData, consigneeData };
  } catch (err) {
    console.error('🙂 -> file: helper.js:438 -> err:', err);
    return { confirmationCostData: [], shipperData: [], consigneeData: [] };
  }
}

async function fetchNonConsoleTableData({ shipmentAparData }) {
  try {
    const tables = [
      'omni-wt-rt-shipment-header-non-console',
      'omni-wt-rt-references-dev',
      'omni-wt-rt-shipment-desc-dev',
    ];

    const [shipmentHeaderData, referencesData, shipmentDescData] = await Promise.all(
      tables.map(async (table) => {
        const param = getParamsByTableName(_.get(shipmentAparData, 'FK_OrderNo'), table);
        console.info('🙂 -> file: index.js:35 -> tables.map -> param:', table, param);
        const response = await queryDynamoDB(param);
        return _.get(response, 'Items', false);
      })
    );
    if (shipmentHeaderData.length === 0) {
      throw new Error('Missing data in shipment header');
    }
    const customersParams = getParamsByTableName(
      '',
      'omni-wt-rt-customers-dev',
      '',
      _.get(shipmentHeaderData, 'BillNo', '')
    );
    console.info('🙂 -> file: index.js:61 -> customersParams:', customersParams);
    const customersData = _.get(await queryDynamoDB(customersParams), 'Items', false);
    return { shipmentHeaderData, referencesData, shipmentDescData, customersData };
  } catch (err) {
    console.error('🙂 -> file: helper.js:469 -> err:', err);
    return { shipmentHeaderData: [], referencesData: [], shipmentDescData: [], customersData: [] };
  }
}

async function fetchConsoleTableData({ shipmentAparData }) {
  try {
    const tables = [
      'omni-wt-rt-shipment-header-console',
      'omni-wt-rt-references-dev',
      'omni-wt-rt-shipment-desc-dev',
      'omni-wt-rt-tracking-notes',
    ];

    const [shipmentHeaderData, referencesData, shipmentDescData, trackingNotesData] =
      await Promise.all(
        tables.map(async (table) => {
          const param = getParamsByTableName(
            _.get(shipmentAparData, 'FK_OrderNo'),
            table,
            '',
            '',
            '',
            _.get(shipmentAparData, 'ConsolNo')
          );
          console.info('🙂 -> file: index.js:35 -> tables.map -> param:', table, param);
          const response = await queryDynamoDB(param);
          return _.get(response, 'Items', false);
        })
      );
    if (shipmentHeaderData === 0 || trackingNotesData === 0) {
      throw new Error('Missing data in customers or users');
    }
    const tables2 = ['omni-wt-rt-customers-dev', 'omni-wt-rt-users'];
    const [customersData, userData] = await Promise.all(
      tables2.map(async (table) => {
        const param = getParamsByTableName(
          _.get(shipmentAparData, 'FK_OrderNo'),
          table,
          '',
          _.get(shipmentHeaderData, '[0].BillNo', ''),
          _.get(trackingNotesData, '[0].FK_UserId')
        );
        console.info('🙂 -> file: index.js:35 -> tables.map -> param:', table, param);
        const response = await queryDynamoDB(param);
        return _.get(response, 'Items', false);
      })
    );
    return {
      shipmentHeaderData,
      referencesData,
      shipmentDescData,
      trackingNotesData,
      customersData,
      userData,
    };
  } catch (err) {
    console.info('🙂 -> file: helper.js:526 -> err:', err);
    return {
      shipmentHeaderData: [],
      referencesData: [],
      shipmentDescData: [],
      trackingNotesData: [],
      customersData: [],
      userData: [],
    };
  }
}

async function getAparDataByConsole({ shipmentAparData }) {
  const shipmentAparParams = {
    TableName: 'omni-wt-rt-shipment-apar-dev',
    KeyConditionExpression: 'FK_OrderNo = :orderNo',
    FilterExpression: 'Consolidation = :consolidation and ConsolNo = :ConsolNo',
    ExpressionAttributeValues: {
      ':orderNo': _.get(shipmentAparData, 'FK_OrderNo'),
      ':ConsolNo': _.get(shipmentAparData, 'ConsolNo'),
      ':consolidation': 'N',
    },
  };
  console.info('🙂 -> file: helper.js:551 -> shipmentAparParams:', shipmentAparParams);
  return _.get(await queryDynamoDB(shipmentAparParams), 'Items', []);
}

async function getVesselForConsole({ shipmentAparConsoleData }) {
  const orderNo = _.get(shipmentAparConsoleData, '[0].FK_OrderNo');
  const shipmentHeaderParam = getParamsByTableName(orderNo, 'omni-wt-rt-shipment-header-console');
  console.info('🙂 -> file: helper.js:555 -> shipmentHeaderParam:', shipmentHeaderParam);
  const billno = _.get(await queryDynamoDB(shipmentHeaderParam), 'Items.[0].BillNo');
  const customersParams = getParamsByTableName('', 'omni-wt-rt-customers-dev', '', billno);
  console.info('🙂 -> file: helper.js:558 -> customersParams:', customersParams);
  return _.get(await queryDynamoDB(customersParams), 'Items.[0].CustName', '');
}

async function getWeightForConsole({ shipmentAparConsoleData: aparData }) {
  const descData = await Promise.all(
    aparData.map(async (data) => {
      const shipmentDescParams = {
        TableName: 'omni-wt-rt-shipment-desc-dev',
        KeyConditionExpression: 'FK_OrderNo = :orderNo',
        ExpressionAttributeValues: {
          ':orderNo': _.get(data, 'FK_OrderNo'),
        },
      };
      console.info('🙂 -> file: helper.js:551 -> shipmentAparParams:', shipmentDescParams);

      return _.get(await queryDynamoDB(shipmentDescParams), 'Items', []);
    })
  );
  console.info('🙂 -> file: test.js:36 -> descData:', descData);
  const descDataFlatten = _.flatten(descData);
  const totalWeight = _.sumBy(descDataFlatten, (item) => parseFloat(_.get(item, 'Weight', 0)));
  console.info('🙂 -> file: test.js:38 -> totalWeight:', totalWeight);
}

async function getHazmat({ shipmentAparConsoleData: aparData }) {
  const descData = await Promise.all(
    aparData.map(async (data) => {
      const shipmentDescParams = {
        TableName: 'omni-wt-rt-shipment-desc-dev',
        KeyConditionExpression: 'FK_OrderNo = :orderNo',
        ExpressionAttributeValues: {
          ':orderNo': _.get(data, 'FK_OrderNo'),
        },
      };
      console.info('🙂 -> file: helper.js:551 -> shipmentAparParams:', shipmentDescParams);

      return _.get(await queryDynamoDB(shipmentDescParams), 'Items');
    })
  );
  console.info('🙂 -> file: test.js:36 -> descData:', descData);
  const descDataFlatten = _.flatten(descData);
  console.info('🙂 -> file: test.js:38 -> descDataFlatten:', descDataFlatten);
  const filteredDescData = descDataFlatten
    .filter((data) => _.get(data, 'Hazmat'))
    .map((data) => _.get(data, 'Hazmat'))
    .includes('Y');
  console.info('🙂 -> file: test.js:40 -> filteredDescData:', filteredDescData);
  return filteredDescData;
}

async function getHighValue({ shipmentAparConsoleData: aparData }) {
  const descData = await Promise.all(
    aparData.map(async (data) => {
      const shipmentHeaderParams = {
        TableName: 'omni-wt-rt-shipment-header-dev',
        KeyConditionExpression: 'PK_OrderNo = :orderNo',
        ExpressionAttributeValues: {
          ':orderNo': _.get(data, 'FK_OrderNo'),
        },
      };
      console.info('🙂 -> file: helper.js:551 -> shipmentAparParams:', shipmentHeaderParams);

      return _.get(await queryDynamoDB(shipmentHeaderParams), 'Items');
    })
  );
  console.info('🙂 -> file: test.js:36 -> descData:', descData);
  const descDataFlatten = _.flatten(descData);
  console.info('🙂 -> file: test.js:38 -> descDataFlatten:', descDataFlatten);
  const sumByInsurance = _.sumBy(descDataFlatten, (data) =>
    parseFloat(_.get(data, 'Insurance', 0))
  );
  console.info('🙂 -> file: test.js:40 -> sumByInsurance:', sumByInsurance > 100000);
  return sumByInsurance > 100000;
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
  getFinalShipperAndConsigneeData,
  fetchAparTableForConsole,
  fetchCommonTableData,
  fetchNonConsoleTableData,
  fetchConsoleTableData,
  getVesselForConsole,
  getWeightForConsole,
  getHazmat,
  getHighValue,
  getAparDataByConsole,
};
