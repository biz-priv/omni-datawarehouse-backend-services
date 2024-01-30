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
  let schedArriveEarly = '';
  let schedArriveLate = '';
  let timeZone = '';
  let state = '';
  if (stopType === 'PU') {
    // For Pickup (PU)
    const pickupZip = _.get(stateData, 'ShipZip', '');
    console.info('🚀 ~ file: helper.js:131 ~ pickupZip:', pickupZip);
    schedArriveEarly = _.get(shipmentHeader, 'ReadyDateTime', '');
    schedArriveLate = _.get(shipmentHeader, 'ReadyDateTimeRange', '');
    timeZone = await getTimeZoneCode(pickupZip);
    state = _.get(stateData, 'FK_ShipState', '');
  } else if (stopType === 'SO') {
    // For Delivery (SO)
    const deliveryZip = _.get(stateData, 'ConZip', '');
    console.info('🚀 ~ file: helper.js:139 ~ deliveryZip:', deliveryZip);
    schedArriveEarly = _.get(shipmentHeader, 'ScheduledDateTime', '');
    console.info('🙂 -> file: helper.js:150 -> schedArriveEarly:', schedArriveEarly);
    schedArriveLate = _.get(shipmentHeader, 'ScheduledDateTimeRange', '');
    console.info('🙂 -> file: helper.js:152 -> schedArriveLate:', schedArriveLate);
    timeZone = await getTimeZoneCode(deliveryZip);
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
        comments: _.get(
          confirmationCostData,
          type === 'shipper' ? 'PickupNote' : 'DeliveryNote',
          ''
        ),
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
  // const shipperLocationId = 'BSDJAVB';
  // Get location ID for shipper
  let shipperLocationId = await getLocationId(
    finalShipperData.ShipName,
    finalShipperData.ShipAddress1,
    finalShipperData.ShipAddress2,
    finalShipperData.ShipCity,
    finalShipperData.FK_ShipState,
    finalShipperData.ShipZip
  );

  // const consigneeLocationId = 'NFAKJND';
  // Get location ID for consignee
  let consigneeLocationId = await getLocationId(
    finalConsigneeData.ConName,
    finalConsigneeData.ConAddress1,
    finalConsigneeData.ConAddress2,
    finalConsigneeData.ConCity,
    finalConsigneeData.FK_ConState,
    finalConsigneeData.ConZip
  );

  // return { shipperLocationId, consigneeLocationId };

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
  // Check if required fields are present and non-empty in confirmationCostData
  let finalShipperData;
  if (_.get(confirmationCostData, 'ShipName', '') === '') {
    finalShipperData = shipperData;
  } else {
    finalShipperData = confirmationCostData;
  }

  let finalConsigneeData;
  if (_.get(confirmationCostData, 'ConName', '') === '') {
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
      _.get(shipmentHeaderData, '[0].BillNo', '')
    );
    console.info('🚀 ~ file: helper.js:471 ~ BillNo:', _.get(shipmentHeaderData, '[0].BillNo', ''));
    console.info('🙂 -> file: index.js:61 -> customersParams:', customersParams);
    const tables2 = ['omni-wt-rt-customers-dev', 'omni-wt-rt-users'];
    const [customersData, userData] = await Promise.all(
      tables2.map(async (table) => {
        const param = getParamsByTableName(
          _.get(shipmentAparData, 'FK_OrderNo'),
          table,
          '',
          _.get(shipmentHeaderData, '[0].BillNo', ''),
          _.get(shipmentHeaderData, '[0].AcctManager')
        );
        console.info('🙂 -> file: index.js:35 -> tables.map -> param:', table, param);
        const response = await queryDynamoDB(param);
        return _.get(response, 'Items', false);
      })
    );
    console.info('🚀 ~ file: helper.js:467 ~ userData:', userData);
    return { shipmentHeaderData, referencesData, shipmentDescData, customersData, userData };
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
          console.info('🚀 ~ file: helper.js:510 ~ response:', response);
          return _.get(response, 'Items', false);
        })
      );
    if (shipmentHeaderData === 0 || trackingNotesData === 0) {
      throw new Error('Missing data in customers or users');
    }
    const tables2 = ['omni-wt-rt-customers-dev'];
    const [customersData] = await Promise.all(
      tables2.map(async (table) => {
        const param = getParamsByTableName(
          _.get(shipmentAparData, 'FK_OrderNo'),
          table,
          '',
          _.get(shipmentHeaderData, '[0].BillNo', '')
        );
        console.info('🙂 -> file: index.js:35 -> tables.map -> param:', table, param);
        const response = await queryDynamoDB(param);
        console.info('🚀 ~ file: helper.js:529 ~ response:', response);
        return _.get(response, 'Items', false);
      })
    );
    let userData = await Promise.all(
      trackingNotesData.map(async (trackingNote) => {
        const param = getParamsByTableName(
          '',
          'omni-wt-rt-users',
          '',
          '',
          _.get(trackingNote, 'FK_UserId')
        );
        console.info(
          '🙂 -> file: index.js:35 -> tables.map -> param:',
          trackingNote,
          'omni-wt-rt-users',
          param
        );
        const response = await queryDynamoDB(param);
        console.info('🚀 ~ file: helper.js:529 ~ response:', response);
        return _.get(response, 'Items[0]', false);
      })
    );
    userData = userData.filter((item) => item && _.has(item, 'UserEmail'))[0];
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
  try {
    const shipmentAparParams = {
      TableName: 'omni-wt-rt-shipment-apar-dev',
      IndexName: 'omni-ivia-ConsolNo-index-dev',
      KeyConditionExpression: 'ConsolNo = :ConsolNo',
      FilterExpression: 'Consolidation = :consolidation',
      ExpressionAttributeValues: {
        ':ConsolNo': _.get(shipmentAparData, 'ConsolNo'),
        ':consolidation': 'N',
      },
    };
    console.info('🙂 -> file: helper.js:551 -> shipmentAparParams:', shipmentAparParams);
    const result = _.get(await queryDynamoDB(shipmentAparParams), 'Items', []);
    console.info('🙂 -> file: helper.js:573 -> result:', result);
    if (result.length <= 0) {
      throw new Error(
        `Shipment apar data with console number ${_.get(shipmentAparData, 'ConsolNo')} not found where consolidation = 'N'.`
      );
    }
    return result;
  } catch (err) {
    console.error('🙂 -> file: helper.js:546 -> err:', err);
    throw err;
  }
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
  return totalWeight;
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

async function getHighValue({ shipmentAparConsoleData: aparData, type = 'high_value' }) {
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
  if (type === 'high_value') return sumByInsurance > 100000;
  return sumByInsurance;
}

function getPieces({ shipmentDesc }) {
  return _.sumBy(shipmentDesc, (data) => parseInt(_.get(data, 'Piece', 0), 10));
}

// Reusable function to convert string values to numbers and sum them
function sumNumericValues(items, propertyName) {
  return _.sum(
    items.map((item) => {
      const numericValue = parseFloat(_.get(item, propertyName));
      return !isNaN(numericValue) ? numericValue : 0;
    })
  );
}

async function fetchDataFromTablesList(CONSOL_NO) {
  try {
    if (!CONSOL_NO) {
      throw new Error('ConsolNo number is required.');
    }

    const sapparams = {
      TableName: 'omni-wt-rt-shipment-apar-dev',
      IndexName: 'omni-ivia-ConsolNo-index-dev',
      KeyConditionExpression: 'ConsolNo = :ConsolNo',
      FilterExpression:
        'FK_VendorId = :FK_VendorId and Consolidation = :Consolidation and FK_ServiceId = :FK_ServiceId and SeqNo <> :SeqNo and FK_OrderNo <> :FK_OrderNo',
      ExpressionAttributeValues: {
        ':ConsolNo': CONSOL_NO.toString(),
        ':FK_VendorId': 'LIVELOGI',
        ':Consolidation': 'N',
        ':FK_ServiceId': 'MT',
        ':SeqNo': '9999',
        ':FK_OrderNo': CONSOL_NO.toString(),
      },
    };
    console.info('🚀 ~ file: helper.js:678 ~ sapparams:', sapparams);

    let shipmentApar = await dynamoDB.query(sapparams).promise();
    shipmentApar = shipmentApar.Items;
    if (shipmentApar.length === 0) {
      throw new Error(`No shipment Apar data found for ConsolNo:${CONSOL_NO}`);
    }

    const uniqueShipmentApar = getUniqueObjects(shipmentApar);

    const shipmentHeader = [];
    const shipmentDesc = [];
    const consolStopHeaders = [];
    const consolStopItems = [];
    let element;

    for (const uniqueElement of uniqueShipmentApar) {
      element = uniqueElement;

      const shparams = {
        TableName: 'omni-wt-rt-shipment-header-dev',
        KeyConditionExpression: 'PK_OrderNo = :PK_OrderNo',
        ExpressionAttributeValues: {
          ':PK_OrderNo': element.FK_OrderNo.toString(),
        },
      };
      const sh = await dynamoDB.query(shparams).promise();
      shipmentHeader.push(...sh.Items);
      if (shipmentHeader.length === 0) {
        throw new Error(`No shipment header data found:${element.FK_OrderNo}`);
      }
      const sdparams = {
        TableName: 'omni-wt-rt-shipment-desc-dev',
        KeyConditionExpression: 'FK_OrderNo = :FK_OrderNo',
        ExpressionAttributeValues: {
          ':FK_OrderNo': element.FK_OrderNo.toString(),
        },
      };
      const sd = await dynamoDB.query(sdparams).promise();
      shipmentDesc.push(...sd.Items);

      if (shipmentDesc.length === 0) {
        throw new Error(`No shipment desc data found:${element.FK_OrderNo}`);
      }

      const cstparams = {
        TableName: 'omni-wt-rt-consol-stop-items-dev',
        KeyConditionExpression: 'FK_OrderNo = :FK_OrderNo',
        ExpressionAttributeValues: {
          ':FK_OrderNo': element.FK_OrderNo.toString(),
        },
      };
      const cst = await dynamoDB.query(cstparams).promise();
      consolStopItems.push(...cst.Items);
      if (consolStopItems.length === 0) {
        throw new Error(`No consol stop items data found for FK_OrderNo:${element.FK_OrderNo}`);
      }
      const uniqueConsolStopItems = getUniqueObjects(consolStopItems);

      for (const csitem of uniqueConsolStopItems) {
        const cshparams = {
          TableName: 'omni-wt-rt-consol-stop-headers-dev',
          KeyConditionExpression: 'PK_ConsolStopId = :PK_ConsolStopId',
          FilterExpression: 'FK_ConsolNo = :ConsolNo',
          ExpressionAttributeValues: {
            ':PK_ConsolStopId': csitem.FK_ConsolStopId.toString(),
            ':ConsolNo': CONSOL_NO.toString(),
          },
        };

        const csh = await dynamoDB.query(cshparams).promise();
        consolStopHeaders.push(...csh.Items);
      }
    }
    if (consolStopHeaders.length === 0) {
      throw new Error(`No consol stop headers data found for CONSOL_NO:${CONSOL_NO}`);
    }
    const uniqueConsolStopHeaders = getUniqueObjects(consolStopHeaders);

    let customer = [];
    if (shipmentHeader.length > 0 && shipmentHeader[0].BillNo !== '') {
      const customerParam = {
        TableName: 'omni-wt-rt-customers-dev',
        KeyConditionExpression: 'PK_CustNo = :PK_CustNo',
        ExpressionAttributeValues: {
          ':PK_CustNo': shipmentHeader[0].BillNo,
        },
      };
      const customerResult = await dynamoDB.query(customerParam).promise();
      customer = customerResult.Items;
    }
    if (customer.length === 0) {
      throw new Error(`No customer data found for BillNo:${shipmentHeader[0].BillNo}`);
    }

    const references = [];
    const refparams = {
      TableName: 'omni-wt-rt-references-dev',
      IndexName: 'omni-wt-rt-ref-orderNo-index-dev',
      KeyConditionExpression: 'FK_OrderNo = :FK_OrderNo',
      ExpressionAttributeValues: {
        ':FK_OrderNo': element.FK_OrderNo.toString(),
      },
    };
    const refResult = await dynamoDB.query(refparams).promise();
    references.push(...refResult.Items);
    if (references.length === 0) {
      throw new Error(`No references data found for FK_OrderNo:${element.FK_OrderNo}`);
    }
    const tnparams = {
      TableName: 'omni-wt-rt-tracking-notes-dev',
      IndexName: 'omni-tracking-notes-console-index-dev',
      KeyConditionExpression: 'ConsolNo = :ConsolNo',
      ExpressionAttributeValues: {
        ':ConsolNo': CONSOL_NO.toString(),
      },
    };

    const trackingNotesResult = await dynamoDB.query(tnparams).promise();
    console.info('🚀 ~ file: helper.js:777 ~ trackingNotesResult:', trackingNotesResult);

    if (trackingNotesResult.Items.length === 0) {
      throw new Error(`No data found in tracking notes table for ConsolNo:${CONSOL_NO}`);
    }
    let users = [];
    const userparams = {
      TableName: 'omni-wt-rt-users-dev',
      KeyConditionExpression: 'PK_UserId = :PK_UserId',
      ExpressionAttributeValues: {
        ':PK_UserId': _.get(trackingNotesResult, 'Items[0].FK_UserId', ''),
      },
    };
    console.info('🚀 ~ file: helper.js:787 ~ userparams:', userparams);
    const usersResult = await dynamoDB.query(userparams).promise();
    users = usersResult.Items;
    if (users.length === 0) {
      throw new Error(
        `No users data found for PK_UserId: ${_.get(trackingNotesResult, 'Items[0].FK_UserId', '')}`
      );
    }

    return {
      shipmentApar: uniqueShipmentApar,
      shipmentHeader,
      shipmentDesc,
      consolStopHeaders: uniqueConsolStopHeaders,
      consolStopItems,
      customer,
      references,
      users,
    };
  } catch (error) {
    console.error('error', error);
    return {};
  }
}

async function populateStops(consolStopHeaders, references, users) {
  const stops = [];

  // Fetch location IDs for stops
  const locationIds = await fetchLocationIds(consolStopHeaders);
  // const locationIds = ['JLDFNGA', 'NSFPAI', 'JFGDKNSD'];

  for (let i = 0; i < consolStopHeaders.length; i++) {
    const stopHeader = consolStopHeaders[i];
    const locationId = locationIds[i];

    console.info('ConsolStopPickupOrDelivery', stopHeader.ConsolStopPickupOrDelivery);

    let stoptype;

    const isPickup = stopHeader.ConsolStopPickupOrDelivery;

    if (isPickup === 'false') {
      stoptype = 'PU';
    } else {
      stoptype = 'SO';
    }
    const timeZoneCode = await getTimeZoneCode(stopHeader.ConsolStopZip);
    console.info('🚀 ~ file: helper.js:786 ~ stoptype:', stoptype);

    const stop = {
      __type: 'stop',
      __name: 'stops',
      company_id: 'TMS',
      appt_required: false,
      confirmed: false,
      driver_load_unload: 'N',
      late_eta_colorcode: false,
      location_id: locationId,
      sched_arrive_early: await calculateSchedArriveEarly(stopHeader, timeZoneCode),
      sched_arrive_late: await calculateSchedArriveLate(stopHeader, timeZoneCode),
      status: 'A',
      order_sequence: parseInt(_.get(stopHeader, 'ConsolStopNumber', 0), 10) + 1,
      stop_type: stoptype,
      requested_service: false,
      prior_uncleared_stops: false,
      stopNotes: [
        {
          __type: 'stop_note',
          __name: 'stopNotes',
          company_id: 'TMS',
          comment_type: 'DC',
          comments: _.get(stopHeader, 'ConsolStopNotes', ''),
        },
        {
          __type: 'stop_note',
          __name: 'stopNotes',
          company_id: 'TMS',
          comment_type: 'DC',
          comments: _.get(users, '[0].UserEmail'),
        },
      ],
    };
    if (stoptype === 'PU') {
      _.set(stop, 'referenceNumbers', generateReferenceNumbers({ references }));
    }
    stops.push(stop);
  }

  return stops;
}

async function fetchLocationIds(stopsData) {
  const locationPromises = stopsData.map(async (stopData) => {
    const locationId = await getLocationId(
      stopData.ConsolStopName,
      stopData.ConsolStopAddress1,
      stopData.ConsolStopAddress2,
      stopData.ConsolStopCity,
      stopData.FK_ConsolStopState,
      stopData.ConsolStopZip
    );

    if (!locationId) {
      return createLocation({
        __type: 'location',
        company_id: 'TMS',
        address1: stopData.ConsolStopAddress1,
        address2: stopData.ConsolStopAddress2,
        city_name: stopData.ConsolStopCity,
        is_active: true,
        name: stopData.ConsolStopName,
        state: stopData.FK_ConsolStopState,
        zip_code: stopData.ConsolStopZip,
      });
    }

    return locationId;
  });

  const locationIds = await Promise.all(locationPromises);

  return locationIds;
}

async function getTimeZoneCode(zipcode) {
  try {
    const params = {
      TableName: 'omni-wt-rt-timezone-zip-cr-dev',
      KeyConditionExpression: 'ZipCode = :code',
      ExpressionAttributeValues: {
        ':code': zipcode,
      },
    };

    const result = await dynamoDB.query(params).promise();
    const timezoncode = _.get(result, 'Items[0].FK_TimeZoneCode');
    console.info('🚀 ~ file: test.js:603 ~ timezoncode:', timezoncode);
    if (timezoncode) {
      return timezoncode;
    }
    console.error(`Timezone code not found for zipcode: ${zipcode}`);
    return null;
  } catch (error) {
    console.error('Error querying timezone code:', error);
    throw error;
  }
}

async function getHoursAwayFromTimeZone(timeZoneCode) {
  try {
    const params = {
      TableName: 'omni-wt-rt-timezone-master-dev',
      KeyConditionExpression: 'PK_TimeZoneCode = :code',
      ExpressionAttributeValues: {
        ':code': timeZoneCode,
      },
    };

    const result = await dynamoDB.query(params).promise();

    // Use lodash _.get to safely access nested properties
    const hoursAway = _.get(result, 'Items[0].HoursAway.S');

    // Check if hoursAway is defined and convert to integer
    return hoursAway ? null : parseInt(hoursAway, 10);
  } catch (error) {
    console.error('Error querying hours away:', error);
    throw error;
  }
}

async function calculateSchedArriveEarly(stopHeader, timeZoneCode) {
  const consolStopDate = stopHeader.ConsolStopDate;
  const consolStopTimeBegin = stopHeader.ConsolStopTimeBegin;
  const { timeZoneOffset, hoursAway } = await getConsolTimeZoneOffset(
    stopHeader.FK_ConsolStopState,
    consolStopDate,
    timeZoneCode
  );

  if (consolStopTimeBegin.startsWith('1900-01-01')) {
    // Invalid date, return consolStopDate with added offset
    return moment(consolStopDate).add(hoursAway, 'hours').format('YYYYMMDDHHmmss') + timeZoneOffset;
  }

  const formattedDateTime = moment(
    `${consolStopDate} ${consolStopTimeBegin}`,
    'YYYY-MM-DD HH:mm:ss.SSS'
  )
    .add(hoursAway, 'hours')
    .format('YYYYMMDDHHmmss');
  console.info('🚀 ~ file: test.js:652 ~ formattedDateTime:', formattedDateTime);

  return formattedDateTime + timeZoneOffset;
}

async function calculateSchedArriveLate(stopHeader, timeZoneCode) {
  const consolStopDate = stopHeader.ConsolStopDate;
  const consolStopTimeEnd = stopHeader.ConsolStopTimeEnd;
  const { timeZoneOffset, hoursAway } = await getConsolTimeZoneOffset(
    stopHeader.FK_ConsolStopState,
    consolStopDate,
    timeZoneCode
  );

  if (consolStopTimeEnd.startsWith('1900-01-01')) {
    // Invalid date, return consolStopDate with added offset
    return moment(consolStopDate).add(hoursAway, 'hours').format('YYYYMMDDHHmmss') + timeZoneOffset;
  }

  const formattedDateTime = moment(
    `${consolStopDate} ${consolStopTimeEnd}`,
    'YYYY-MM-DD HH:mm:ss.SSS'
  )
    .add(hoursAway, 'hours')
    .format('YYYYMMDDHHmmss');
  console.info('🚀 ~ file: helper.js:941 ~ formattedDateTime:', formattedDateTime);

  return formattedDateTime + timeZoneOffset;
}

async function getConsolTimeZoneOffset(fKConsolStopState, consolStopDate, timeZoneCode) {
  let timeZoneOffset = 0;
  let hoursAway = 0;
  if (fKConsolStopState === 'AZ') {
    timeZoneOffset = '-0600';
  } else {
    const weekOfYear = moment(consolStopDate).isoWeek();
    timeZoneOffset = weekOfYear >= 11 && weekOfYear <= 44 ? '-0500' : '-0600';
    hoursAway = await getHoursAwayFromTimeZone(timeZoneCode);
  }

  return { timeZoneOffset, hoursAway };
}

function getUniqueObjects(array) {
  const uniqueSet = new Set(array.map(JSON.stringify));
  return Array.from(uniqueSet, JSON.parse);
}

function mapEquipmentCodeToFkPowerbrokerCode(fkEquipmentCode) {
  const equipmentCodeMapping = {
    '22 BOX': 'SBT',
    '24 BOX': 'SBT',
    '26 BOX': 'SBT',
    '48': 'TT',
    '48 FT AIR': 'TT',
    '53': 'TT',
    '53 FT AIR': 'TT',
    'C': 'C',
    'CARGO VAN': 'V',
    'CN': 'CN',
    'DD': 'DD',
    'F': 'F',
    'FA': 'FA',
    'FC': 'FC',
    'FD': 'FD',
    'FH': 'FH',
    'FM': 'FM',
    'FN': 'FN',
    'FO': 'FO',
    'FS': 'O',
    'FT': 'FT',
    'LB': 'LB',
    'LFT PJ BOX': 'O',
    'LFTG BOX': 'O',
    'LG SPRINT': 'SPNT',
    'PO': 'PO',
    'R': 'R',
    'RG': 'RG',
    'RM': 'RM',
    'SB': 'SB',
    'SD': 'SD',
    'SM SPRINT': 'SPNT',
    'SR': 'SR',
    'STRAIGHT': 'SBT',
    'TM BOX': 'SBM',
    'TM SPINT': 'SPNT',
    'V': 'V',
    'VA': 'VA',
    'VF': 'VF',
    'VL': 'VL',
    'VM': 'VM',
    'VN': 'VN',
    'VR': 'VR',
    'VZ': 'VZ',
  };

  // Convert the input to uppercase for case-insensitive comparison
  const uppercaseEquipmentCode = fkEquipmentCode.toUpperCase();

  // Check if the equipment code exists in the mapping, return the corresponding powerbroker code
  if (Object.hasOwn(equipmentCodeMapping, uppercaseEquipmentCode)) {
    return equipmentCodeMapping[uppercaseEquipmentCode];
  }
  return '';
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
  sumNumericValues,
  fetchDataFromTablesList,
  populateStops,
  mapEquipmentCodeToFkPowerbrokerCode,
  getPieces,
};
