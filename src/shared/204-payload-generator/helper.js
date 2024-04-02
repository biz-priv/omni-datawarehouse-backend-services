'use strict';
const _ = require('lodash');
const AWS = require('aws-sdk');
const moment = require('moment-timezone');
const { getLocationId, createLocation } = require('./apis');

const ses = new AWS.SES();
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
  USERS_TABLE,
  TRACKING_NOTES_TABLE,
  TRACKING_NOTES_CONSOLENO_INDEX_KEY,
  SHIPMENT_APAR_INDEX_KEY_NAME,
  INSTRUCTIONS_INDEX_KEY_NAME,
  INSTRUCTIONS_TABLE,
  REFERENCES_TABLE,
  REFERENCES_INDEX_KEY_NAME,
  OMNI_NO_REPLY_EMAIL,
  OMNI_DEV_EMAIL,
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

async function generateStop(
  shipmentHeader,
  references,
  orderSequence,
  stopType,
  locationId,
  stateData,
  type,
  userData,
  shipmentAparData,
  shipmentDesc
) {
  const { schedArriveEarly, schedArriveLate } = await processStopDataNonConsol(
    stopType,
    shipmentHeader,
    stateData
  );

  const instructions = await queryDynamoDB(
    getParamsByTableName(_.get(shipmentHeader, 'PK_OrderNo', ''), 'omni-wt-rt-instructions')
  );

  const specialInstruction = _.filter(_.get(instructions, 'Items'), {
    Type: 'S',
  });

  const stopData = {
    __type: 'stop',
    __name: 'stops',
    company_id: 'TMS',
    appt_required: false,
    confirmed: false,
    driver_load_unload: 'N',
    location_id: locationId,
    contact_name:
      type === 'shipper'
        ? _.get(stateData, 'ShipContact', 'NA')
        : _.get(stateData, 'ConContact', 'NA'),
    phone:
      type === 'shipper' ? _.get(stateData, 'ShipPhone', 'NA') : _.get(stateData, 'ConPhone', 'NA'),
    sched_arrive_early: await getCstTime({
      datetime: schedArriveEarly,
    }),
    sched_arrive_late: await getCstTime({
      datetime: schedArriveLate,
    }),
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

  specialInstruction.map((item) => {
    return stopData.stopNotes.push({
      __type: 'stop_note',
      __name: 'stopNotes',
      company_id: 'TMS',
      comment_type: 'OC',
      comments: _.get(item, 'Note', 'NA'),
    });
  });
  if (type === 'shipper') {
    const userEmail = _.get(userData, 'UserEmail', '') || 'NA';
    const equipmentCode = _.get(shipmentHeader, 'FK_EquipmentCode', '') || 'NA';
    const total = _.get(shipmentAparData, 'Total', 0);
    const dims = populateDims(shipmentHeader, shipmentDesc);

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
      },
      dims
    );
  }

  return stopData;
}

async function processStopDataNonConsol(stopType, shipmentHeader) {
  let schedArriveEarly = '';
  let schedArriveLate = '';

  if (stopType === 'PU') {
    schedArriveEarly = _.get(shipmentHeader, 'ReadyDateTime', '');
    schedArriveLate = _.get(shipmentHeader, 'ReadyDateTimeRange', '');
  } else if (stopType === 'SO') {
    schedArriveEarly = _.get(shipmentHeader, 'ScheduledDateTime', '');
    schedArriveLate = _.get(shipmentHeader, 'ScheduledDateTimeRange', '');
  }

  return { schedArriveEarly, schedArriveLate };
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
  shipmentAparConsoleData,
  housebillData,
  descData
) {
  const { schedArriveEarly, schedArriveLate, timeZone } = await processStopData(
    stopType,
    confirmationCostData
  );

  const instructions = await queryDynamoDB(
    getParamsByTableName(
      _.get(shipmentHeaderData, '[0].orderNo', ''),
      'omni-wt-rt-instructions',
      timeZone
    )
  );

  const specialInstruction = _.filter(_.get(instructions, 'Items'), {
    Type: 'S',
  });

  const stopData = {
    __type: 'stop',
    __name: 'stops',
    company_id: 'TMS',
    appt_required: false,
    confirmed: false,
    driver_load_unload: 'N',
    location_id: locationId,
    contact_name:
      type === 'shipper'
        ? _.get(confirmationCostData, 'ShipContact', 'NA')
        : _.get(confirmationCostData, 'ConContact', 'NA'),
    phone:
      type === 'shipper'
        ? _.get(confirmationCostData, 'ShipPhone', 'NA')
        : _.get(confirmationCostData, 'ConPhone', 'NA'),
    sched_arrive_early: await getCstTime({
      datetime: schedArriveEarly,
    }),
    sched_arrive_late: await getCstTime({
      datetime: schedArriveLate,
    }),
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
    referenceNumbers:
      type === 'shipper'
        ? [
            ...populateHousebillNumbers(housebillData, descData),
            ...generateReferenceNumbers({ references }),
          ]
        : [],
  };
  stopData.stopNotes.push({
    __type: 'stop_note',
    __name: 'stopNotes',
    company_id: 'TMS',
    comment_type: 'OC',
    comments: _.get(specialInstruction, 'Note', 'NA'),
  });
  if (type === 'shipper') {
    const userEmail = _.get(userData, 'UserEmail', '') || 'NA';
    const equipmentCode = _.get(shipmentHeaderData, '[0].equipmentCode', '') || 'NA';
    const total = sumNumericValues(shipmentAparConsoleData, 'Total');
    const dims = populateDims(housebillData, descData);

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
      },
      dims
    );
  }

  return stopData;
}

async function processStopData(stopType, confirmationCostData) {
  let schedArriveEarly = '';
  let schedArriveLate = '';

  if (stopType === 'PU') {
    schedArriveEarly = _.get(confirmationCostData, 'PickupDateTime', '');
    schedArriveLate = _.get(confirmationCostData, 'PickupTimeRange', '');
  } else if (stopType === 'SO') {
    schedArriveEarly = _.get(confirmationCostData, 'DeliveryDateTime', '');
    schedArriveLate = _.get(confirmationCostData, 'DeliveryTimeRange', '');
  }

  return { schedArriveEarly, schedArriveLate };
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
        TableName: SHIPMENT_HEADER_TABLE,
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
        IndexName: SHIPMENT_APAR_INDEX_KEY_NAME,
        KeyConditionExpression: 'ConsolNo = :ConsolNo',
        FilterExpression: 'Consolidation = :consolidation',
        ExpressionAttributeValues: {
          ':ConsolNo': String(consoleNo),
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
        TableName: INSTRUCTIONS_TABLE,
        IndexName: INSTRUCTIONS_INDEX_KEY_NAME,
        KeyConditionExpression: 'FK_OrderNo = :orderNo',
        ExpressionAttributeValues: {
          ':orderNo': orderNo,
        },
      };
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

async function fetchLocationId({ finalShipperData, finalConsigneeData, orderId, houseBill }) {
  // Get location ID for shipper
  let shipperLocationId = await getLocationId(
    finalShipperData.ShipName,
    finalShipperData.ShipAddress1,
    finalShipperData.ShipAddress2,
    finalShipperData.FK_ShipState
  );

  // Get location ID for consignee
  let consigneeLocationId = await getLocationId(
    finalConsigneeData.ConName,
    finalConsigneeData.ConAddress1,
    finalConsigneeData.ConAddress2,
    finalConsigneeData.FK_ConState
  );

  // Use the obtained location IDs to create locations if needed
  if (!shipperLocationId) {
    shipperLocationId = await createLocation({
      data: {
        __type: 'location',
        company_id: 'TMS',
        address1: finalShipperData.ShipAddress1,
        address2: finalShipperData.ShipAddress2,
        city_name: finalShipperData.ShipCity,
        is_active: true,
        name: finalShipperData.ShipName,
        state: finalShipperData.FK_ShipState,
        zip_code: finalShipperData.ShipZip,
      },
      orderId,
      country: finalShipperData.FK_ShipCountry,
      houseBill,
    });
  }

  if (!consigneeLocationId) {
    consigneeLocationId = await createLocation({
      data: {
        __type: 'location',
        company_id: 'TMS',
        address1: finalConsigneeData.ConAddress1,
        address2: finalConsigneeData.ConAddress2,
        city_name: finalConsigneeData.ConCity,
        is_active: true,
        name: finalConsigneeData.ConName,
        state: finalConsigneeData.FK_ConState,
        zip_code: finalConsigneeData.ConZip,
      },
      orderId,
      country: finalConsigneeData.FK_ConCountry,
      houseBill,
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

async function fetchCommonTableData({ shipmentAparData }) {
  const tables = ['omni-wt-rt-confirmation-cost', 'omni-wt-rt-shipper', 'omni-wt-rt-consignee'];

  try {
    const [confirmationCostData, shipperData, consigneeData] = await Promise.all(
      tables.map(async (table) => {
        const param = getParamsByTableName(_.get(shipmentAparData, 'FK_OrderNo'), table);
        console.info('🙂 -> file: index.js:35 -> tables.map -> param:', table, param);
        const response = await queryDynamoDB(param);
        return _.get(response, 'Items', []);
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
        return _.get(response, 'Items', []);
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
        return _.get(response, 'Items', []);
      })
    );
    const userData = await queryUserTable({ userId: _.get(shipmentAparData, 'UpdatedBy') });
    console.info('🚀 ~ file: helper.js:491 ~ userData:', userData);
    return {
      shipmentHeaderData,
      referencesData,
      shipmentDescData,
      customersData,
      userData,
    };
  } catch (err) {
    console.error('🙂 -> file: helper.js:494 -> err:', err);
    return {
      shipmentHeaderData: [],
      referencesData: [],
      shipmentDescData: [],
      customersData: [],
      userData: [],
    };
  }
}

async function queryUserTable({ userId }) {
  try {
    const params = {
      TableName: USERS_TABLE,
      KeyConditionExpression: 'PK_UserId = :PK_UserId',
      ExpressionAttributeValues: {
        ':PK_UserId': userId,
      },
    };
    console.info('🚀 ~ file: helper.js:759 ~ queryTrackingNotes ~ param:', params);
    const response = await dynamoDB.query(params).promise();
    return _.get(response, 'Items', []);
  } catch (error) {
    console.error('🙂 -> file: helper.js:759 -> error:', error);
    throw error;
  }
}

async function fetchConsoleTableData({ shipmentAparData }) {
  try {
    const tables = ['omni-wt-rt-shipment-apar-console'];
    const [shipmentAparConsolData] = await Promise.all(
      tables.map(async (table) => {
        const param = getParamsByTableName(
          '',
          table,
          '',
          '',
          '',
          _.get(shipmentAparData, 'ConsolNo')
        );
        console.info('🙂 -> file: index.js:35 -> tables.map -> param:', table, param);
        const response = await queryDynamoDB(param);
        console.info('🚀 ~ file: helper.js:779 ~ tables.map ~ response:', response);
        return _.get(response, 'Items[0]', {});
      })
    );
    console.info(
      '🚀 ~ file: helper.js:782 ~ fetchConsoleTableData ~ shipmentAparConsolData:',
      shipmentAparConsolData
    );

    const tables1 = ['omni-wt-rt-shipment-header-console'];

    const [shipmentHeaderData] = await Promise.all(
      tables1.map(async (table) => {
        const param = getParamsByTableName(_.get(shipmentAparConsolData, 'FK_OrderNo'), table);
        console.info('🙂 -> file: index.js:35 -> tables.map -> param:', table, param);
        const response = await queryDynamoDB(param);
        return _.get(response, 'Items', []);
      })
    );
    if (shipmentHeaderData.length === 0) {
      throw new Error('Missing data in shipment header');
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
        return _.get(response, 'Items', []);
      })
    );
    const param = getParamsByTableName(
      '',
      'omni-wt-rt-users',
      '',
      '',
      _.get(shipmentAparData, 'UpdatedBy')
    );
    console.info('🚀 ~ file: helper.js:824 User Params:', param);
    let userData = await queryDynamoDB(param);
    console.info('🚀 ~ file: helper.js:822 ~ userData:', userData);
    userData = userData.Items;
    return {
      shipmentHeaderData,
      customersData,
      userData,
    };
  } catch (err) {
    console.info('🙂 -> file: helper.js:526 -> err:', err);
    return {
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

async function getWeightPiecesForConsole({ shipmentAparConsoleData }) {
  const descData = await Promise.all(
    shipmentAparConsoleData.map(async (data) => {
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
  console.info('🚀 ~ file: helper.js:795 ~ getWeightPiecesForConsole ~ descData:', descData);
  return descData;
}

async function descDataForConsole({ shipmentAparConsoleData: aparData }) {
  const descData = await Promise.all(
    aparData.map(async (data) => {
      const shipmentDescParams = {
        TableName: SHIPMENT_DESC_TABLE,
        KeyConditionExpression: 'FK_OrderNo = :orderNo',
        ExpressionAttributeValues: {
          ':orderNo': _.get(data, 'FK_OrderNo'),
        },
      };

      const queryResult = await queryDynamoDB(shipmentDescParams);
      const descItems = _.get(queryResult, 'Items', []);

      // Assuming you want to push all items from the query result
      return descItems;
    })
  );

  // Flatten the array of arrays
  const descDataFlatten = _.flatten(descData);

  return descDataFlatten;
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
  const headerDataArray = [];

  // Iterate over each object in aparData
  await Promise.all(
    aparData.map(async (data) => {
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

      // Extracting FK_EquipmentCode, FK_ServiceLevelId, and OrderDate from any object
      const firstItem = items.length > 0 ? items[0] : null;
      const equipmentCode = _.get(firstItem, 'FK_EquipmentCode');
      const serviceLevelId = _.get(firstItem, 'FK_ServiceLevelId');
      const orderNo = _.get(firstItem, 'PK_OrderNo');

      // Collecting Housebill from all objects
      const housebills = items.map((item) => _.get(item, 'Housebill'));

      const headerData = { equipmentCode, serviceLevelId, orderNo, housebills };
      headerDataArray.push(headerData);
    })
  );

  return headerDataArray;
}

async function getHousebillData({ shipmentAparConsoleData: aparData }) {
  const housebillData = await Promise.all(
    aparData.map(async (dataItem) => {
      const shipmentHeaderParams = {
        TableName: SHIPMENT_HEADER_TABLE,
        KeyConditionExpression: 'PK_OrderNo = :orderNo',
        ExpressionAttributeValues: {
          ':orderNo': _.get(dataItem, 'FK_OrderNo'),
        },
      };

      const queryResult = await queryDynamoDB(shipmentHeaderParams);
      const headerData = _.get(queryResult, 'Items', []);

      return headerData; // Return all items from the query result
    })
  );

  // Flatten the array of arrays
  const housebillDataFlatten = _.flatten(housebillData);

  return housebillDataFlatten;
}

async function getReferencesData({ shipmentAparConsoleData: aparData }) {
  const refTableData = await Promise.all(
    aparData.map(async (dataItem) => {
      const referenceTableParams = {
        TableName: REFERENCES_TABLE,
        IndexName: REFERENCES_INDEX_KEY_NAME,
        KeyConditionExpression: 'FK_OrderNo = :FK_OrderNo',
        ExpressionAttributeValues: {
          ':FK_OrderNo': _.get(dataItem, 'FK_OrderNo'),
        },
      };

      const queryResult = await queryDynamoDB(referenceTableParams);
      return _.get(queryResult, 'Items', []);
    })
  );

  // Flatten the array of arrays
  const refTableDataFlatten = _.flatten(refTableData);

  return refTableDataFlatten;
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

    let users = [];
    const userparams = {
      TableName: USERS_TABLE,
      KeyConditionExpression: 'PK_UserId = :PK_UserId',
      ExpressionAttributeValues: {
        ':PK_UserId': _.get(uniqueShipmentApar, '[0]UpdatedBy'),
      },
    };
    console.info('🚀 ~ file: helper.js:787 ~ userparams:', userparams);
    const usersResult = await dynamoDB.query(userparams).promise();
    users = usersResult.Items;
    if (users.length === 0) {
      throw new Error(
        `No users data found for PK_UserId: ${_.get(uniqueShipmentApar, '[0]UpdatedBy')}`
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
    throw error;
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
  const orderId = _.join(_.map(shipmentApar, 'FK_OrderNo'));
  const houseBill = _.join(_.map(shipmentHeader, 'Housebill'));
  const locationIds = await fetchLocationIds(consolStopHeaders, orderId, houseBill);

  for (const [index, stopHeader] of consolStopHeaders.entries()) {
    const locationId = locationIds[index];
    const isPickup = stopHeader.ConsolStopPickupOrDelivery === 'false';
    const stoptype = isPickup ? 'PU' : 'SO';

    const referenceNumbersArray = [
      generateReferenceNumbers({ references }),
      populateHousebillNumbers(shipmentHeader, shipmentDesc),
    ];
    const dims = populateDims(shipmentHeader, shipmentDesc);

    let stopNotes = [
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
        },
        dims
      );
    }
    // Split date and time for sched_arrive_early
    const retrievedDateEarly = stopHeader.ConsolStopDate.split(' ')[0];
    const retrievedTimeEarly = stopHeader.ConsolStopTimeBegin.split(' ')[1];
    const earlyDatetime = `${retrievedDateEarly} ${retrievedTimeEarly}`;

    // Split date and time for sched_arrive_late
    const retrievedDateLate = stopHeader.ConsolStopDate.split(' ')[0];
    const retrievedTimeLate = stopHeader.ConsolStopTimeEnd.split(' ')[1];
    const lateDatetime = `${retrievedDateLate} ${retrievedTimeLate}`;
    // Populate special instructions
    const specialInstructions = await populateSpecialInstructions(shipmentHeader);
    stopNotes = [...stopNotes, ...specialInstructions.flat()];

    const stop = {
      __type: 'stop',
      __name: 'stops',
      company_id: 'TMS',
      appt_required: false,
      confirmed: false,
      driver_load_unload: 'N',
      late_eta_colorcode: false,
      location_id: locationId,
      contact_name: _.get(stopHeader, 'ConsolStopContact', 'NA'),
      phone: _.get(stopHeader, 'ConsolStopPhone', 0),
      sched_arrive_early: await getCstTime({
        datetime: earlyDatetime,
      }),
      sched_arrive_late: await getCstTime({
        datetime: lateDatetime,
      }),
      status: 'A',
      order_sequence: parseInt(stopHeader.ConsolStopNumber ?? 0, 10) + 1,
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

async function populateSpecialInstructions(shipmentHeader) {
  const specialInstructions = await Promise.all(
    shipmentHeader.map(async (header) => {
      const instructions = await queryDynamoDB(
        getParamsByTableName(_.get(header, 'PK_OrderNo', ''), 'omni-wt-rt-instructions')
      );

      const specialInstruction = _.filter(_.get(instructions, 'Items'), {
        Type: 'S',
      });

      return specialInstruction.map((instruction) => ({
        __type: 'stop_note',
        __name: 'stopNotes',
        company_id: 'TMS',
        comment_type: 'OC',
        comments: _.get(instruction, 'Note', ''),
      }));
    })
  );

  return specialInstructions;
}

function populateHousebillNumbers(shipmentHeader, shipmentDesc) {
  return _.map(shipmentHeader, (header) => {
    // Initialize variables to store the sum of pieces and weights for each header
    let totalPieces = 0;
    let totalWeight = 0;

    const matchingDesc = _.filter(shipmentDesc, (desc) => desc.FK_OrderNo === header.PK_OrderNo);
    console.info('🙂 -> matchingDesc:', matchingDesc);

    // Iterate over matchingDesc objects to accumulate pieces and weights
    _.forEach(matchingDesc, (desc) => {
      totalPieces += parseInt(_.get(desc, 'Pieces', 0), 10);
      totalWeight += parseFloat(_.get(desc, 'Weight', 0));
    });

    return {
      __type: 'reference_number',
      __name: 'referenceNumbers',
      partner_id: 'TMS',
      version: '004010',
      company_id: 'TMS',
      element_id: '128',
      pieces: totalPieces,
      weight: totalWeight,
      reference_number: _.get(header, 'Housebill', 0),
      reference_qual: 'MB',
      send_to_driver: false,
    };
  });
}

function populateDims(shipmentHeader, shipmentDesc) {
  // Check if shipmentHeader is not an array, then wrap it into an array
  if (!Array.isArray(shipmentHeader)) {
    shipmentHeader = [shipmentHeader];
  }
  console.info('🚀 ~ file: test.js:627 ~ populateDims ~ shipmentHeader:', shipmentHeader);

  const combinedDims = {};

  // Iterate over each shipment header
  shipmentHeader.forEach((header) => {
    const matchingDesc = shipmentDesc.filter((desc) => desc.FK_OrderNo === header.PK_OrderNo);

    matchingDesc.forEach((desc) => {
      const housebill = header.Housebill;
      combinedDims[housebill] = combinedDims[housebill] || [];
      combinedDims[housebill].push({
        pieces: desc.Pieces,
        length: parseFloat(desc.Length),
        width: parseFloat(desc.Width),
        height: parseFloat(desc.Height),
      });
    });
  });

  // Generate comments for each housebill
  const comments = Object.entries(combinedDims).map(([housebill, dimsArray]) => {
    const dimsComments = dimsArray
      .map((dim) => `Pieces: ${dim.pieces}, Dims: L/W/H: ${dim.length}/${dim.width}/${dim.height}`)
      .join(', ');
    return `Housebill: ${housebill} ~ ${dimsComments}`;
  });

  return {
    __type: 'stop_note',
    __name: 'stopNotes',
    company_id: 'TMS',
    comment_type: 'OC',
    comments: comments.join('\n'),
  };
}

async function fetchLocationIds(stopsData, orderId, houseBill) {
  const locationPromises = stopsData.map(async (stopData) => {
    const locationId = await getLocationId(
      stopData.ConsolStopName,
      stopData.ConsolStopAddress1,
      stopData.ConsolStopAddress2,
      stopData.FK_ConsolStopState
    );

    if (!locationId) {
      return createLocation({
        data: {
          __type: 'location',
          company_id: 'TMS',
          address1: stopData.ConsolStopAddress1,
          address2: stopData.ConsolStopAddress2,
          city_name: stopData.ConsolStopCity,
          is_active: true,
          name: stopData.ConsolStopName,
          state: stopData.FK_ConsolStopState,
          zip_code: stopData.ConsolStopZip,
        },
        consolNo: stopData.FK_ConsolNo,
        orderId,
        country: stopData.FK_ConsolStopCountry,
        houseBill,
      });
    }

    return locationId;
  });

  const locationIds = await Promise.all(locationPromises);

  return locationIds;
}

function getUniqueObjects(array) {
  const uniqueSet = new Set(array.map(JSON.stringify));
  return Array.from(uniqueSet, JSON.parse);
}

async function getCstTime({ datetime }) {
  try {
    // Calculate UTC offset (including DST) for the timezone
    const utcOffset = getNormalizedUtcOffset(datetime, 'America/Chicago');
    console.info('🚀 ~ updated offset:', utcOffset);

    // Combine date and time
    let formattedDateTime = datetime + utcOffset;
    console.info('🚀 ~ file: helper.js:1315 ~ getCstTime ~ formattedDateTime:', formattedDateTime);
    formattedDateTime = moment(formattedDateTime, 'YYYY-MM-DD HH:mm:ss.SSSZZ')
      .tz('America/Chicago')
      .format('YYYYMMDDHHmmssZZ');
    return formattedDateTime;
  } catch (error) {
    console.error(error);
    throw error;
  }
}

function getNormalizedUtcOffset(formattedDate, timezone) {
  try {
    const momentTimezone = moment.tz(formattedDate, 'YYYY-MM-DD HH:mm:ss.SSS', timezone);

    const offsetFormatted = momentTimezone.format('ZZ');
    console.info('🚀 ~ offset:', offsetFormatted);

    return offsetFormatted;
  } catch (error) {
    console.error(error);
    throw error;
  }
}

function mapEquipmentCodeToFkPowerbrokerCode(fkEquipmentCode) {
  const equipmentCodeMapping = {
    '22 BOX': 'SBT',
    '24 BOX': 'SBT',
    '26 BOX': 'SBT',
    '48': 'TT',
    '48 FT AIR': 'TT',
    '53': 'V',
    '53 FT AIR': 'VA',
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

function stationCodeInfo(stationCode) {
  const billingInfo = {
    ACN: { PbBillTo: 'ACUNACMX' },
    LAX: { PbBillTo: 'OMNICOT3' },
    LGB: { PbBillTo: 'OMNICO28' },
    IAH: { PbBillTo: 'HOUSHOTX' },
    BNA: { PbBillTo: 'NASHNATN' },
    AUS: { PbBillTo: 'OMNIAUTX' },
    COE: { PbBillTo: 'OMNICO24' },
    PHL: { PbBillTo: 'OMNICO27' },
    ORD: { PbBillTo: 'OMNICOT1' },
    SAT: { PbBillTo: 'OMNICOT2' },
    BOS: { PbBillTo: 'OMNICOT4' },
    SLC: { PbBillTo: 'OMNICOT8' },
    ATL: { PbBillTo: 'ATLACOTX' },
    BRO: { PbBillTo: 'OMNICOT9' },
    CMH: { PbBillTo: 'COLUCOTX' },
    CVG: { PbBillTo: 'CINCCOTX' },
    DAL: { PbBillTo: 'OMNICOT9' },
    DTW: { PbBillTo: 'DETRCOTX' },
    EXP: { PbBillTo: 'OMNICOT9' },
    GSP: { PbBillTo: 'TIGECONC' },
    LRD: { PbBillTo: 'OMNILATX' },
    MKE: { PbBillTo: 'MILWCOTX' },
    MSP: { PbBillTo: 'STPACOTX' },
    PHX: { PbBillTo: 'PHEOCOTX' },
    TAN: { PbBillTo: 'OMNICOT9' },
    YYZ: { PbBillTo: 'ONTACOTX' },
    DFW: { PbBillTo: 'OMNICOTX' },
    IND: { PbBillTo: 'OMNIDATX' },
    ELP: { PbBillTo: 'OMNIELTX' },
    PIT: { PbBillTo: 'OMNIOAPA' },
    MFE: { PbBillTo: 'OMNIPHTX' },
    OLH: { PbBillTo: 'OMNITEAZ' },
    OTR: { PbBillTo: 'OTRODATX' },
    PDX: { PbBillTo: 'PORTPOOR' },
    SAN: { PbBillTo: 'SANDSACA' },
    SFO: { PbBillTo: 'SFOOCOTX' },
  };

  const stationInfo = billingInfo[stationCode];
  if (stationInfo) {
    const PbBillTo = stationInfo.PbBillTo;
    return PbBillTo;
  }
  return 'NA';
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

async function getUserEmail({ userId }) {
  try {
    const userEmail = await fetchUserEmail({ userId });
    return userEmail;
  } catch (error) {
    console.error('🚀 ~ file: helper.js:770 ~ getUserData ~ error:', error);
    throw error;
  }
}

async function fetchUserEmail({ userId }) {
  try {
    const params = {
      TableName: USERS_TABLE,
      KeyConditionExpression: 'PK_UserId = :PK_UserId',
      ExpressionAttributeValues: {
        ':PK_UserId': userId,
      },
    };
    console.info('🚀 ~ file: helper.js:759 ~ fetchUserEmail ~ param:', params);
    const response = await dynamoDB.query(params).promise();
    return _.get(response, 'Items[0].UserEmail', []);
  } catch (error) {
    console.error('🙂 -> file: helper.js:1722 -> error:', error);
    throw error;
  }
}

async function sendSESEmail({ message, userEmail, subject, functionName }) {
  try {
    const params = {
      Destination: {
        ToAddresses: [userEmail, OMNI_DEV_EMAIL],
      },
      Message: {
        Body: {
          Text: {
            Data: `An error occurred in ${functionName}: ${message}`,
            Charset: 'UTF-8',
          },
        },
        Subject: subject,
      },
      Source: OMNI_NO_REPLY_EMAIL,
    };
    console.info('🚀 ~ file: helper.js:1747 ~ sendSESEmail ~ params:', params);

    await ses.sendEmail(params).promise();
  } catch (error) {
    console.error('Error sending email with SES:', error);
    throw error;
  }
}

async function getEquipmentCodeForMT(consolNo) {
  try {
    const params = {
      TableName: SHIPMENT_APAR_TABLE,
      IndexName: SHIPMENT_APAR_INDEX_KEY_NAME,
      KeyConditionExpression: 'ConsolNo = :ConsolNo',
      FilterExpression: 'Consolidation = :consolidation',
      ExpressionAttributeValues: {
        ':ConsolNo': consolNo,
        ':consolidation': 'Y',
      },
    };

    const data = await dynamoDB.query(params).promise();

    // Use lodash chain to process the result set
    const equipmentCode = _.chain(data.Items)
      .map('FK_EquipmentCode') // Extract FK_EquipmentCode from items
      .compact() // Remove any falsy values
      .sample() // Select a random value if there are multiple
      .value(); // Retrieve the final value

    return equipmentCode || 'NA';
  } catch (error) {
    console.error('Error querying shipment apar table:', error);
    throw error;
  }
}

module.exports = {
  getPowerBrokerCode,
  getCstTime,
  generateStop,
  getParamsByTableName,
  queryDynamoDB,
  fetchLocationId,
  getFinalShipperAndConsigneeData,
  fetchCommonTableData,
  fetchNonConsoleTableData,
  fetchConsoleTableData,
  getVesselForConsole,
  getWeightPiecesForConsole,
  getHazmat,
  getHighValue,
  getAparDataByConsole,
  sumNumericValues,
  fetchDataFromTablesList,
  populateStops,
  mapEquipmentCodeToFkPowerbrokerCode,
  getPieces,
  getShipmentHeaderData,
  generateStopforConsole,
  getHousebillData,
  descDataForConsole,
  stationCodeInfo,
  getReferencesData,
  getUserEmail,
  sendSESEmail,
  getEquipmentCodeForMT,
};
