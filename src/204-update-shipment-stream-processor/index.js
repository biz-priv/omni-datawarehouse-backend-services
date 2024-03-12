'use strict';

const AWS = require('aws-sdk');
const { get, isEqual } = require('lodash');
const { updateOrders } = require('../shared/204-payload-generator/apis');
const { getCstTime, getTimezone } = require('../shared/204-payload-generator/helper');
const { STATUSES } = require('../shared/constants/204_create_shipment');

const dynamoDb = new AWS.DynamoDB.DocumentClient();
const { STATUS_TABLE, CONFIRMATION_COST, CONFIRMATION_COST_INDEX_KEY_NAME } = process.env;

module.exports.handler = async (event) => {
  try {
    await Promise.all(event.Records.map(processRecord));
  } catch (error) {
    console.error('Error processing records:', error);
    throw error;
  }
};

async function processRecord(record) {
  const body = JSON.parse(get(record, 'body', ''));
  const message = JSON.parse(get(body, 'Message', ''));
  console.info('Message:', typeof message);

  const newImage = AWS.DynamoDB.Converter.unmarshall(get(message, 'NewImage', {}));
  console.info('🚀 ~ file: index.js:27 ~ processRecord ~ newImage:', newImage);
  const oldImage = AWS.DynamoDB.Converter.unmarshall(get(message, 'OldImage', {}));
  console.info('🚀 ~ file: index.js:29 ~ processRecord ~ oldImage:', oldImage);
  const dynamoTableName = get(message, 'dynamoTableName', '');
  console.info('Dynamo Table Name:', dynamoTableName);

  const orderNo = get(newImage, 'FK_OrderNo');
  console.info('🚀 ~ file: index.js:34 ~ processRecord ~ orderNo:', orderNo);

  const changedFields = getChangedFields(newImage, oldImage);
  console.info('🚀 ~ file: index.js:37 ~ processRecord ~ changedFields:', changedFields);

  await updateChangedFields(orderNo, changedFields);

  console.info('Record processed successfully.');
}

function getChangedFields(newImage, oldImage) {
  const changedFields = {};
  for (const key in newImage) {
    if (!isEqual(newImage[key], oldImage[key])) {
      changedFields[key] = newImage[key];
    }
  }
  console.info('Changed fields:', changedFields);
  return changedFields;
}

async function updateChangedFields(orderNo, changedFields) {
  if (shouldUpdateTimeFields(changedFields)) {
    const { shipperTimeZone, consigneeTimeZone } = await getShipperAndConsigneeTimezones(orderNo);
    await Promise.all(
      Object.entries(changedFields).map(async ([field, value]) => {
        changedFields[field] = await getCstTime({
          datetime: value,
          timezone: getAffectedTimeZone(field, shipperTimeZone, consigneeTimeZone),
        });
      })
    );
  }

  const logQueryResult = await queryOrderStatusTable(orderNo);
  if (logQueryResult.length > 0 && logQueryResult[0].Status === STATUSES.FAILED) {
    await updateOrderStatusTable(orderNo);
  } else if (logQueryResult.length > 0 && logQueryResult[0].Status === STATUSES.SENT) {
    const response = logQueryResult[0].Response;
    const updatedStops = await updateStopsFromChangedFields(response.stops, changedFields);
    console.info('🚀 ~ file: index.js:74 ~ updateChangedFields ~ updatedStops:', updatedStops);
    const updatedResponse = { ...response, stops: updatedStops };
    console.info(
      '🚀 ~ file: index.js:75 ~ updateChangedFields ~ updatedResponse:',
      updatedResponse
    );
    await updateOrders({ payload: updatedResponse });
    // Compare oldPayload with payload constructed now
    if (isEqual(response, updatedResponse)) {
      console.info('Payload is the same as the old payload. Skipping further processing.');
      throw new Error(`Payload is the same as the old payload. Skipping further processing.
              \n Payload: ${JSON.stringify(updatedResponse)}
              `);
    }
    // UPDATE updatecount in orderstatus table - new columns - UpdatePayload, UpdateCount
    await setUpdateCount({
      orderNo,
      UpdateCount: get(logQueryResult, '[0].UpdateCount', 0),
      updatedResponse,
    });
  } else {
    console.info('Update not found in log table.');
  }
}

function shouldUpdateTimeFields(changedFields) {
  return ['PickupTimeRange', 'PickUpDateTime', 'DeliveryDateTime', 'DeliveryTimeRange'].some(
    (field) => changedFields[field]
  );
}

function getAffectedTimeZone(field, shipperTimeZone, consigneeTimeZone) {
  return field.includes('PickUp') ? shipperTimeZone : consigneeTimeZone;
}

async function queryOrderStatusTable(orderNo) {
  const params = {
    TableName: STATUS_TABLE,
    KeyConditionExpression: 'FK_OrderNo = :orderNo',
    ExpressionAttributeValues: {
      ':orderNo': orderNo,
    },
  };
  console.info('Order status table query parameters:', params);
  try {
    const result = await dynamoDb.query(params).promise();
    return result.Items;
  } catch (error) {
    console.error('Error querying status table:', error);
    throw error;
  }
}

async function updateOrderStatusTable(orderNo) {
  const updateParams = {
    TableName: STATUS_TABLE,
    Key: { FK_OrderNo: orderNo },
    UpdateExpression: 'set #Status = :status, ResetCount = :resetCount',
    ExpressionAttributeNames: { '#Status': 'Status' },
    ExpressionAttributeValues: {
      ':status': STATUSES.PENDING,
      ':resetCount': 0,
    },
  };
  console.info('Update status table parameters:', updateParams);
  try {
    await dynamoDb.update(updateParams).promise();
  } catch (error) {
    console.error('Error updating status table:', error);
    throw error;
  }
}

async function setUpdateCount({ orderNo, UpdateCount, updatedResponse }) {
  const updateParams = {
    TableName: STATUS_TABLE,
    Key: { FK_OrderNo: orderNo },
    UpdateExpression: 'set UpdateCount = :updatecount, UpdatePayload = :updatepayload',
    ExpressionAttributeValues: {
      ':updatepayload': updatedResponse,
      ':updatecount': UpdateCount + 1,
    },
  };
  console.info('🚀 ~ file: index.js:145 ~ setUpdateCount ~ updateParams:', updateParams);
  console.info('Update status table parameters:', updateParams);
  try {
    await dynamoDb.update(updateParams).promise();
  } catch (error) {
    console.error('Error updating status table:', error);
    throw error;
  }
}

async function updateStopsFromChangedFields(stops, changedFields) {
  return stops.map((stop) => {
    if (stop.order_sequence === 1) {
      updatePickupFields(stop, changedFields);
    } else if (stop.order_sequence === 2) {
      updateDeliveryFields(stop, changedFields);
    }
    return stop;
  });
}

function updatePickupFields(stop, changedFields) {
  const mapping = {
    ShipName: 'location_name',
    ShipContact: 'contact_name',
    ShipZip: 'zip_code',
    ShipCity: 'city_name',
    ShipPhone: 'phone',
    ShipAddress1: 'address',
    ShipAddress2: 'address2',
    FK_ShipState: 'state',
    PickUpDateTime: 'sched_arrive_early',
    PickupTimeRange: 'sched_arrive_late',
  };
  Object.entries(changedFields).forEach(([field, value]) => {
    if (mapping[field]) stop[mapping[field]] = value;
  });
}

function updateDeliveryFields(stop, changedFields) {
  const mapping = {
    ConAddress1: 'address',
    ConAddress2: 'address2',
    ConCity: 'city_name',
    ConContact: 'contact_name',
    ConName: 'location_name',
    ConPhone: 'phone',
    DeliveryDateTime: 'sched_arrive_early',
    DeliveryTimeRange: 'sched_arrive_late',
    FK_ConState: 'state',
    ConZip: 'zip_code',
  };
  Object.entries(changedFields).forEach(([field, value]) => {
    if (mapping[field]) stop[mapping[field]] = value;
  });
}

async function getShipperAndConsigneeTimezones(orderNo) {
  const params = {
    TableName: CONFIRMATION_COST,
    IndexName: CONFIRMATION_COST_INDEX_KEY_NAME,
    KeyConditionExpression: 'FK_OrderNo = :orderNo',
    ExpressionAttributeValues: {
      ':orderNo': orderNo,
    },
  };

  const stateData = await dynamoDb.query(params).promise();

  const shipperData = extractDataForTimezone(stateData.Items[0], true);
  const consigneeData = extractDataForTimezone(stateData.Items[0], false);

  const [shipperTimeZone, consigneeTimeZone] = await Promise.all([
    getTimezone(shipperData),
    getTimezone(consigneeData),
  ]);

  return { shipperTimeZone, consigneeTimeZone };
}

function extractDataForTimezone(item, isShipper) {
  const prefix = isShipper ? 'Ship' : 'Con';
  const countryKey = isShipper ? 'FK_ShipCountry' : 'FK_ConCountry';
  const stateKey = isShipper ? 'FK_ShipState' : 'FK_ConState';

  return {
    stopCity: item[`${prefix}City`],
    state: item[stateKey],
    country: item[countryKey],
    address1: item[`${prefix}Address1`],
  };
}
