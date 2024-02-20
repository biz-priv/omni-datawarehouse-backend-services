'use strict';
const _ = require('lodash');
const AWS = require('aws-sdk');
const moment = require('moment-timezone');
const { getLocationId, createLocation } = require('./apis');

const {
  SHIPMENT_APAR_TABLE,
  SHIPMENT_HEADER_TABLE,
  SHIPMENT_DESC_TABLE,
  CONSOL_STOP_HEADERS,
  CONSOL_STOP_ITEMS,
  CUSTOMER_TABLE,
  CONFIRMATION_COST,
  CONFIRMATION_COST_INDEX_KEY_NAME,
  CONSIGNEE_TABLE,
  SHIPPER_TABLE,
  TIMEZONE_MASTER,
  TIMEZONE_ZIP_CR,
  REFERENCES_TABLE,
  USERS_TABLE,
  TRACKING_NOTES_TABLE,
  REFERENCES_INDEX_KEY_NAME,
  TRACKING_NOTES_CONSOLENO_INDEX_KEY,
  SHIPMENT_APAR_INDEX_KEY_NAME,
} = process.env;

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
  return _.get(refTypeMap, reftypeId, 'NA');
}

function generateReferenceNumbers({ references }) {
  return getUniqueObjects(
    _.map(references, (ref) => ({
      __type: 'reference_number',
      __name: 'referenceNumbers',
      company_id: 'TMS',
      element_id: '128',
      partner_id: 'TMS',
      reference_number: _.get(ref, 'ReferenceNo', 'NA'),
      reference_qual: getPowerBrokerCode(_.get(ref, 'FK_RefTypeId', 'NA')),
      send_to_driver: true,
      version: '004010',
    }))
  );
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
  type,
  userData,
  shipmentAparData
) {
  const { schedArriveEarly, schedArriveLate, timeZone, state } = await processStopDataNonConsol(
    stopType,
    shipmentHeader,
    stateData
  );

  const timeZoneMaster = await queryDynamoDB(
    getParamsByTableName('', 'omni-wt-rt-timezone-master', timeZone)
  );

  const instructions = await queryDynamoDB(
    getParamsByTableName(_.get(shipmentHeader, 'PK_OrderNo', ''), 'omni-wt-rt-instructions', timeZone)
  );

  const specialInstruction = _.filter(_.get(instructions, 'Items'), { 'Type': 'S' })

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
        comments: getNote(stateData, type),
      },
    ],
    referenceNumbers: type === 'shipper' ? generateReferenceNumbers({ references }) : [],
  };

  stopData.stopNotes.push(
    {
      __type: 'stop_note',
      __name: 'stopNotes',
      company_id: 'TMS',
      comment_type: 'OC',
      comments: _.get(specialInstruction, 'Note', ''),
    })
  if (type === 'shipper') {
    const userEmail = _.get(userData, 'UserEmail', '') || 'NA';
    const equipmentCode = _.get(shipmentHeader, 'FK_EquipmentCode', '') || 'NA';
    const total = _.get(shipmentAparData, 'Total', 0);

    stopData.stopNotes.push(
      {
        __type: 'stop_note',
        __name: 'stopNotes',
        company_id: 'TMS',
        comment_type: 'OC',
        comments: userEmail,
      },
      {
        __type: 'stop_note',
        __name: 'stopNotes',
        company_id: 'TMS',
        comment_type: 'OC',
        comments: equipmentCode,
      },
      {
        __type: 'stop_note',
        __name: 'stopNotes',
        company_id: 'TMS',
        comment_type: 'OC',
        comments: total,
      }
    );
  }

  return stopData;
}

async function processStopDataNonConsol(stopType, shipmentHeader, stateData) {
  let pickupZip = '';
  let deliveryZip = '';
  let schedArriveEarly = '';
  let schedArriveLate = '';
  let timeZone = '';
  let state = '';

  if (stopType === 'PU') {
    pickupZip = _.get(stateData, 'ShipZip', '');
    schedArriveEarly = _.get(shipmentHeader, 'ReadyDateTime', '');
    schedArriveLate = _.get(shipmentHeader, 'ReadyDateTimeRange', '');
    timeZone = await getTimeZoneCode(pickupZip);
    state = _.get(stateData, 'FK_ShipState', '');
  } else if (stopType === 'SO') {
    deliveryZip = _.get(stateData, 'ConZip', '');
    schedArriveEarly = _.get(shipmentHeader, 'ScheduledDateTime', '');
    schedArriveLate = _.get(shipmentHeader, 'ScheduledDateTimeRange', '');
    timeZone = await getTimeZoneCode(deliveryZip);
    state = _.get(stateData, 'FK_ConState', '');
  }

  return { schedArriveEarly, schedArriveLate, timeZone, state };
}

async function generateStopforConsole(
  shipmentHeaderData,
  references,
  orderSequence,
  stopType,
  locationId,
  confirmationCostData,
  type,
  userData,
  shipmentAparConsoleData
) {
  const { schedArriveEarly, schedArriveLate, timeZone, state } = await processStopData(
    stopType,
    confirmationCostData
  );

  const timeZoneMaster = await queryDynamoDB(
    getParamsByTableName('', 'omni-wt-rt-timezone-master', timeZone)
  );

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
        comments: getNote(confirmationCostData, type),
      },
    ],
    referenceNumbers: type === 'shipper' ? generateReferenceNumbers({ references }) : [],
  };

  if (type === 'shipper') {
    const userEmail = _.get(userData, 'UserEmail', '') || 'NA';
    const equipmentCode = _.get(shipmentHeaderData, 'equipmentCode', '') || 'NA';
    const total = sumNumericValues(shipmentAparConsoleData, 'Total');

    stopData.stopNotes.push(
      {
        __type: 'stop_note',
        __name: 'stopNotes',
        company_id: 'TMS',
        comment_type: 'OC',
        comments: userEmail,
      },
      {
        __type: 'stop_note',
        __name: 'stopNotes',
        company_id: 'TMS',
        comment_type: 'OC',
        comments: equipmentCode,
      },
      {
        __type: 'stop_note',
        __name: 'stopNotes',
        company_id: 'TMS',
        comment_type: 'OC',
        comments: total,
      }
    );
  }

  return stopData;
}

async function processStopData(stopType, confirmationCostData) {
  let pickupZip = '';
  let deliveryZip = '';
  let schedArriveEarly = '';
  let schedArriveLate = '';
  let timeZone = '';
  let state = '';

  if (stopType === 'PU') {
    pickupZip = _.get(confirmationCostData, 'ShipZip', '');
    schedArriveEarly = _.get(confirmationCostData, 'PickupDateTime', '');
    schedArriveLate = _.get(confirmationCostData, 'PickupTimeRange', '');
    timeZone = await getTimeZoneCode(pickupZip);
    state = _.get(confirmationCostData, 'FK_ShipState', '');
  } else if (stopType === 'SO') {
    deliveryZip = _.get(confirmationCostData, 'ConZip', '');
    schedArriveEarly = _.get(confirmationCostData, 'DeliveryDateTime', '');
    schedArriveLate = _.get(confirmationCostData, 'DeliveryTimeRange', '');
    timeZone = await getTimeZoneCode(deliveryZip);
    state = _.get(confirmationCostData, 'FK_ConState', '');
  }

  return { schedArriveEarly, schedArriveLate, timeZone, state };
}

function getNote(confirmationCostData, type) {
  let note;

  if (type === 'shipper') {
    note = _.get(confirmationCostData, 'PickupNote', '');
  } else {
    note = _.get(confirmationCostData, 'DeliveryNote', '');
  }

  if (note === '') {
    note = 'NA';
  }

  return note;
}

function getParamsByTableName(orderNo, tableName, timezone, billno, userId, consoleNo) {
  switch (tableName) {
    case 'omni-wt-rt-confirmation-cost':
      return {
        TableName: CONFIRMATION_COST,
        IndexName: CONFIRMATION_COST_INDEX_KEY_NAME,
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
        TableName: SHIPMENT_HEADER_TABLE,
        KeyConditionExpression: 'PK_OrderNo = :orderNo',
        ExpressionAttributeValues: {
          ':orderNo': orderNo,
        },
      };
    case 'omni-wt-rt-references':
      return {
        TableName: REFERENCES_TABLE,
        IndexName: REFERENCES_INDEX_KEY_NAME,
        KeyConditionExpression: 'FK_OrderNo = :FK_OrderNo',
        ExpressionAttributeValues: {
          ':FK_OrderNo': orderNo,
        },
      };
    case 'omni-wt-rt-shipper':
      return {
        TableName: SHIPPER_TABLE,
        KeyConditionExpression: 'FK_ShipOrderNo = :orderNo',
        ExpressionAttributeValues: {
          ':orderNo': orderNo,
        },
      };
    case 'omni-wt-rt-consignee':
      return {
        TableName: CONSIGNEE_TABLE,
        KeyConditionExpression: 'FK_ConOrderNo = :orderNo',
        ExpressionAttributeValues: {
          ':orderNo': orderNo,
        },
      };
    case 'omni-wt-rt-shipment-apar':
      return {
        TableName: SHIPMENT_APAR_TABLE,
        KeyConditionExpression: 'FK_OrderNo = :orderNo',
        ExpressionAttributeValues: {
          ':orderNo': orderNo,
        },
      };
    case 'omni-wt-rt-shipment-apar-console':
      return {
        TableName: SHIPMENT_APAR_TABLE,
        KeyConditionExpression: 'FK_OrderNo = :orderNo',
        FilterExpression: 'Consolidation = :consolidation',
        ExpressionAttributeValues: {
          ':orderNo': orderNo,
          ':consolidation': 'N',
        },
      };
    case 'omni-wt-rt-shipment-desc':
      return {
        TableName: SHIPMENT_DESC_TABLE,
        KeyConditionExpression: 'FK_OrderNo = :orderNo',
        ExpressionAttributeValues: {
          ':orderNo': orderNo,
        },
      };
    case 'omni-wt-rt-timezone-master':
      return {
        TableName: TIMEZONE_MASTER,
        KeyConditionExpression: 'PK_TimeZoneCode = :code',
        ExpressionAttributeValues: {
          ':code': timezone,
        },
      };
    case 'omni-wt-rt-customers':
      return {
        TableName: CUSTOMER_TABLE,
        KeyConditionExpression: 'PK_CustNo = :PK_CustNo',
        ExpressionAttributeValues: {
          ':PK_CustNo': billno,
        },
      };
    case 'omni-wt-rt-users':
      return {
        TableName: USERS_TABLE,
        KeyConditionExpression: 'PK_UserId = :PK_UserId',
        ExpressionAttributeValues: {
          ':PK_UserId': userId,
        },
      };
    case 'omni-wt-rt-tracking-notes':
      return {
        TableName: TRACKING_NOTES_TABLE,
        IndexName: TRACKING_NOTES_CONSOLENO_INDEX_KEY,
        KeyConditionExpression: 'ConsolNo = :ConsolNo',
        ExpressionAttributeValues: {
          ':ConsolNo': String(consoleNo),
        },
      };
    case 'omni-wt-rt-instructions':
      return {
        TableName: "omni-wt-rt-instructions-dev",
        IndexName: "omni-wt-instructions-orderNo-index-dev",
        KeyConditionExpression: 'FK_OrderNo = :orderNo',
        ExpressionAttributeValues: {
          ':orderNo': orderNo,
        },
      }
    default:
      return false;
  }
}

async function queryDynamoDB(params) {
  console.info('🚀 ~ file: helper.js:348 ~ queryDynamoDB ~ params:', params);
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
  const tables = ['omni-wt-rt-confirmation-cost', 'omni-wt-rt-shipper', 'omni-wt-rt-consignee'];

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
      'omni-wt-rt-references',
      'omni-wt-rt-shipment-desc',
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
      'omni-wt-rt-customers',
      '',
      _.get(shipmentHeaderData, '[0].BillNo', '')
    );
    console.info('🚀 ~ file: helper.js:471 ~ BillNo:', _.get(shipmentHeaderData, '[0].BillNo', ''));
    console.info('🙂 -> file: index.js:61 -> customersParams:', customersParams);
    const tables2 = ['omni-wt-rt-customers', 'omni-wt-rt-users'];
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
    console.info('🚀 ~ file: helper.js:491 ~ userData:', userData);
    return { shipmentHeaderData, referencesData, shipmentDescData, customersData, userData };
  } catch (err) {
    console.error('🙂 -> file: helper.js:494 -> err:', err);
    return { shipmentHeaderData: [], referencesData: [], shipmentDescData: [], customersData: [] };
  }
}

async function fetchConsoleTableData({ shipmentAparData }) {
  try {
    const tables = [
      'omni-wt-rt-shipment-header-console',
      'omni-wt-rt-references',
      'omni-wt-rt-shipment-desc',
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
    const tables2 = ['omni-wt-rt-customers'];
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
    userData = [userData.filter((item) => item && _.has(item, 'UserEmail'))[0]];
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
      TableName: SHIPMENT_APAR_TABLE,
      IndexName: SHIPMENT_APAR_INDEX_KEY_NAME,
      KeyConditionExpression: 'ConsolNo = :ConsolNo',
      FilterExpression: 'Consolidation = :consolidation',
      ExpressionAttributeValues: {
        ':ConsolNo': String(_.get(shipmentAparData, 'ConsolNo', '')),
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
  const customersParams = getParamsByTableName('', 'omni-wt-rt-customers', '', billno);
  console.info('🙂 -> file: helper.js:558 -> customersParams:', customersParams);
  return _.get(await queryDynamoDB(customersParams), 'Items.[0].CustName', '');
}

async function getWeightForConsole({ shipmentAparConsoleData: aparData }) {
  const descData = await Promise.all(
    aparData.map(async (data) => {
      const shipmentDescParams = {
        TableName: SHIPMENT_DESC_TABLE,
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
        TableName: SHIPMENT_DESC_TABLE,
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
        TableName: SHIPMENT_HEADER_TABLE,
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

async function getShipmentHeaderData({ shipmentAparConsoleData: aparData }) {
  const data = aparData[0];
  const shipmentHeaderParams = {
    TableName: SHIPMENT_HEADER_TABLE,
    KeyConditionExpression: 'PK_OrderNo = :orderNo',
    ExpressionAttributeValues: {
      ':orderNo': _.get(data, 'FK_OrderNo'),
    },
  };
  console.info('🙂 -> file: helper.js:551 -> shipmentAparParams:', shipmentHeaderParams);

  const queryResult = await queryDynamoDB(shipmentHeaderParams);
  const items = _.get(queryResult, 'Items', []);

  // Extracting FK_EquipmentCode, FK_ServiceLevelId, and OrderDate from the first object
  const firstItem = items.length > 0 ? items[0] : null;
  const equipmentCode = _.get(firstItem, 'FK_EquipmentCode');
  const serviceLevelId = _.get(firstItem, 'FK_ServiceLevelId');
  const Housebill = _.get(firstItem, 'Housebill');

  const headerData = { equipmentCode, serviceLevelId, Housebill };
  return headerData;
}

function getPieces({ shipmentDesc }) {
  return _.sumBy(shipmentDesc, (data) => parseInt(_.get(data, 'Pieces', 0), 10));
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
      TableName: SHIPMENT_APAR_TABLE,
      IndexName: SHIPMENT_APAR_INDEX_KEY_NAME,
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
        TableName: SHIPMENT_HEADER_TABLE,
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
        TableName: SHIPMENT_DESC_TABLE,
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
        TableName: CONSOL_STOP_ITEMS,
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
          TableName: CONSOL_STOP_HEADERS,
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
        TableName: CUSTOMER_TABLE,
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
      TableName: REFERENCES_TABLE,
      IndexName: REFERENCES_INDEX_KEY_NAME,
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
      TableName: TRACKING_NOTES_TABLE,
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
      TableName: USERS_TABLE,
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

async function populateStops(
  consolStopHeaders,
  references,
  users,
  shipmentHeader,
  shipmentDesc,
  shipmentApar
) {
  const stops = [];

  const locationIds = await fetchLocationIds(consolStopHeaders);

  for (const [index, stopHeader] of consolStopHeaders.entries()) {
    const locationId = locationIds[index];
    const isPickup = stopHeader.ConsolStopPickupOrDelivery === 'false';
    const stoptype = isPickup ? 'PU' : 'SO';
    const timeZoneCode = await getTimeZoneCode(stopHeader.ConsolStopZip);

    const referenceNumbersArray = [
      generateReferenceNumbers({ references }),
      populateHousebillNumbers(shipmentHeader, shipmentDesc),
    ];

    const stopNotes = [
      {
        __type: 'stop_note',
        __name: 'stopNotes',
        company_id: 'TMS',
        comment_type: 'DC',
        comments: stopHeader.ConsolStopNotes || 'NA',
      },
      {
        __type: 'stop_note',
        __name: 'stopNotes',
        company_id: 'TMS',
        comment_type: 'OC',
        comments: users[0]?.UserEmail || 'NA',
      },
    ];

    if (stoptype === 'PU') {
      stopNotes.push(
        {
          __type: 'stop_note',
          __name: 'stopNotes',
          company_id: 'TMS',
          comment_type: 'OC',
          comments: shipmentHeader[0]?.FK_EquipmentCode || 'NA',
        },
        {
          __type: 'stop_note',
          __name: 'stopNotes',
          company_id: 'TMS',
          comment_type: 'OC',
          comments: sumNumericValues(shipmentApar, 'Total'),
        }
      );
    }

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
      order_sequence: parseInt(stopHeader.ConsolStopNumber || 0, 10) + 1,
      stop_type: stoptype,
      requested_service: false,
      prior_uncleared_stops: false,
      stopNotes,
    };

    if (stoptype === 'PU') {
      stop.referenceNumbers = [].concat(...referenceNumbersArray);
    }

    stops.push(stop);
  }

  return stops.sort((a, b) => a.order_sequence - b.order_sequence);
}

function populateHousebillNumbers(shipmentHeader, shipmentDesc) {
  return _.map(shipmentHeader, (header) => {
    const matchingDesc = _.find(shipmentDesc, (desc) => desc.FK_OrderNo === header.PK_OrderNo);
    return {
      __type: 'reference_number',
      __name: 'referenceNumbers',
      partner_id: 'TMS',
      version: '004010',
      company_id: 'TMS',
      element_id: '128',
      pieces: _.get(matchingDesc, 'Pieces', 0),
      weight: _.get(matchingDesc, 'Weight', 0),
      reference_number: _.get(header, 'Housebill', 0),
      reference_qual: 'MB',
      send_to_driver: false,
    };
  });
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
      TableName: TIMEZONE_ZIP_CR,
      KeyConditionExpression: 'ZipCode = :code',
      ExpressionAttributeValues: {
        ':code': zipcode,
      },
    };
    console.info('🚀 ~ file: helper.js:998 ~ getTimeZoneCode ~ params:', params);
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
      TableName: TIMEZONE_MASTER,
      KeyConditionExpression: 'PK_TimeZoneCode = :code',
      ExpressionAttributeValues: {
        ':code': timeZoneCode,
      },
    };
    console.info('🚀 ~ file: helper.js:1033 ~ getHoursAwayFromTimeZone ~ params:', params);
    const result = await dynamoDB.query(params).promise();
    const hoursAway = _.get(result, 'Items[0].HoursAway.S');
    // Check if hoursAway is defined and convert to integer
    return hoursAway ? null : parseInt(hoursAway, 10);
  } catch (error) {
    console.error('Error querying hours away:', error);
    throw error;
  }
}

async function calculateSchedArriveEarly(stopHeader, timeZoneCode) {
  try {
    const consolStopDate = stopHeader.ConsolStopDate;
    const consolStopTimeBegin = stopHeader.ConsolStopTimeBegin;
    const { timeZoneOffset, hoursAway } = await getConsolTimeZoneOffset(
      stopHeader.FK_ConsolStopState,
      consolStopDate,
      timeZoneCode
    );

    // Formatted date
    const formattedDate = moment(consolStopDate, 'YYYY-MM-DD').format('YYYYMMDD');

    // Format time with offset and hours away
    const formattedTime = moment(consolStopTimeBegin, 'HH:mm:ss.SSS')
      .add(hoursAway, 'hours')
      .utcOffset(timeZoneOffset, true)
      .format('HHmmssZZ');

    // Combine date and time
    const formattedDateTime = formattedDate + formattedTime;
    console.info('🚀 ~ file: test.js:652 ~ formattedDateTime:', formattedDateTime);
    return formattedDateTime;
  } catch (error) {
    console.error('🚀 ~ file: helper.js:1068 ~ calculateSchedArriveEarly ~ error:', error);
    throw error;
  }
}

async function calculateSchedArriveLate(stopHeader, timeZoneCode) {
  try {
    const consolStopDate = stopHeader.ConsolStopDate;
    const consolStopTimeEnd = stopHeader.ConsolStopTimeEnd;
    const { timeZoneOffset, hoursAway } = await getConsolTimeZoneOffset(
      stopHeader.FK_ConsolStopState,
      consolStopDate,
      timeZoneCode
    );
    // Formatted date
    const formattedDate = moment(consolStopDate, 'YYYY-MM-DD').format('YYYYMMDD');
    // Format time with offset and hours away
    const formattedTime = moment(consolStopTimeEnd, 'HH:mm:ss.SSS')
      .add(hoursAway, 'hours')
      .utcOffset(timeZoneOffset, true)
      .format('HHmmssZZ');

    // Combine date and time
    const formattedDateTime = formattedDate + formattedTime;

    console.info('🚀 ~ file: test.js:652 ~ formattedDateTime:', formattedDateTime);

    return formattedDateTime;
  } catch (error) {
    console.error('🚀 ~ file: helper.js:1097 ~ calculateSchedArriveLate ~ error:', error);
    throw error;
  }
}

async function getConsolTimeZoneOffset(fKConsolStopState, consolStopDate, timeZoneCode) {
  try {
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
  } catch (error) {
    console.error('🚀 ~ file: helper.js:1116 ~ getConsolTimeZoneOffset ~ error:', error);
    throw error;
  }
}

function getUniqueObjects(array) {
  const uniqueSet = new Set(array.map(JSON.stringify));
  return Array.from(uniqueSet, JSON.parse);
}

// Define the mapping function
function mapOrderTypeToMcLeod(fkEquipmentCode) {
  const orderTypeMapping = {
    '22 BOX': 'BOX',
    '24 BOX': 'BOX',
    '26 BOX': 'BOX',
    '2M BOX': 'BOX',
    '48': 'V',
    '48 FT AIR': 'V',
    '53': 'V',
    '53 FT AIR': 'V',
    'CARGO VAN': 'V',
    'CN': 'FB',
    'DD': 'FB',
    'F2': 'FB',
    'FA': 'FB',
    'FC': 'FB',
    'FD': 'FB',
    'FH': 'FB',
    'FM': 'FB',
    'FN': 'FB',
    'FO': 'FB',
    'FR': 'FB',
    'FS': 'FB',
    'FT': 'FB',
    'FZ': 'FB',
    'HB': 'PO',
    'HVD': 'V',
    'HVDF': 'FB',
    'HVDR': 'R',
    'LA': 'FB',
    'LB': 'FB',
    'LG SPRINT': 'SPNTR',
    'LO': 'FB',
    'LR': 'FB',
    'MV': 'V',
    'MX': 'FB',
    'OT': 'FB',
    'PO': 'PO',
    'R': 'R',
    'R2': 'R',
    'RA': 'R',
    'RG': 'R',
    'RL': 'R',
    'RM': 'R',
    'RN': 'R',
    'RP': 'R',
    'RV': 'R',
    'RZ': 'R',
    'SB': 'BOX',
    'SD': 'FB',
    'SM SPRINT': 'SPNTR',
    'SN': 'FB',
    'SR': 'FB',
    'STRAIGHT': 'BOX',
    'TM CARGO': 'V',
    'TM SPINT': 'SPNTR',
    'TN': 'IMDL',
    'V': 'V',
    'V2': 'V',
    'V3': 'V',
    'VA': 'V',
    'VB': 'V',
    'VC': 'V',
    'VF': 'V',
    'VG': 'V',
    'VH': 'V',
    'VI': 'V',
    'VL': 'V',
    'VM': 'V',
    'VN': 'V',
    'VP': 'V',
    'VR': 'VR',
    'VS': 'V',
    'VT': 'V',
    'VV': 'V',
    'VW': 'V',
    'VZ': 'V',
  };
  // Convert the input to uppercase for case-insensitive comparison
  const upperCaseOrderCode = fkEquipmentCode.toUpperCase();

  if (Object.hasOwn(orderTypeMapping, upperCaseOrderCode)) {
    return orderTypeMapping[upperCaseOrderCode];
  }
  return 'NA';
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
  return 'NA';
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
  mapOrderTypeToMcLeod,
  getShipmentHeaderData,
  generateStopforConsole,
};
