'use strict';
const AWS = require('aws-sdk');
const { get, isEqual, cloneDeep, isArray, includes } = require('lodash');
const { updateOrders, getOrders } = require('../shared/204-payload-generator/apis');
const { getCstTime, getTimezone } = require('../shared/204-payload-generator/helper');
const { STATUSES } = require('../shared/constants/204_create_shipment');

const dynamoDb = new AWS.DynamoDB.DocumentClient();

const { STATUS_TABLE, CONFIRMATION_COST, CONFIRMATION_COST_INDEX_KEY_NAME } = process.env;

module.exports.handler = async (event) => {
  console.info('event:', event);
  await Promise.all(
    event.Records.map(async (record) => {
      const body = JSON.parse(get(record, 'body', ''));
      const message = JSON.parse(get(body, 'Message', ''));
      console.info('🙂 -> file: index.js:9 -> module.exports.handler= -> message:', typeof message);
      const newImage = AWS.DynamoDB.Converter.unmarshall(get(message, 'NewImage', {}));
      console.info('🙂 -> file: index.js:11 -> module.exports.handler= -> newImage:', newImage);
      const oldImage = AWS.DynamoDB.Converter.unmarshall(get(message, 'OldImage', {}));
      console.info('🙂 -> file: index.js:13 -> module.exports.handler= -> oldImage:', oldImage);
      const dynamoTableName = get(message, 'dynamoTableName', '');
      console.info(
        '🚀 ~ file: index.js:16 ~ event.Records.map ~ dynamoTableName:',
        dynamoTableName
      );
      const orderNo = get(newImage, 'FK_OrderNo');

      // Find changed fields using lodash
      const changedFields = {};
      for (const key in newImage) {
        if (!isEqual(newImage[key], oldImage[key])) {
          changedFields[key] = newImage[key];
        }
      }

      console.info('🚀 ~ file: index.js:66 ~ event.Records.map ~ changedFields:', changedFields);

      // Query log table
      const logQueryResult = await queryOrderStatusTable(orderNo);
      if (logQueryResult.length > 0 && logQueryResult[0].Status === STATUSES.FAILED) {
        // retrigger the process by updating the status to 'PENDING'
        return await updateOrderStatusTable({ orderNo });
      } else if (logQueryResult.length > 0 && logQueryResult[0].Status === STATUSES.SENT) {
        const response = get(logQueryResult, '[0]Response');

        const id = get(response, 'id', '');
        console.info('🚀 ~ file: index.js:46 ~ event.Records.map ~ id:', id);
        if (id === '') {
          console.info('Skipping the process as the shipment id is not available');
          return true;
        }
        const orderResponse = await getOrders({ id });
        console.info('🚀 ~ file: index.js:43 ~ event.Records.map ~ orderResponse:', orderResponse);

        const movements = get(orderResponse, 'movements');

        // Check if movements is an array and if its length is greater than 1
        if (isArray(movements) && movements.length > 1) {
          console.info('There are more than 1 movement arrays in the object.');
          return true;
        }
        console.info('There are only 1 movement arrays in the object.');
        // check if the movements array has brokerage_status as  "OPEN", "BOOKED", "NEWOMNI", or "COVERED"
        const brokerageStatus = get(movements, '[0].brokerage_status', '');
        if (!includes(['OPEN', 'BOOKED', 'NEWOMNI', 'COVERED'], brokerageStatus)) {
          console.info('Brokerage status is not OPEN, BOOKED, NEWOMNI, or COVERED.');
          return true;
        }
        if (
          get(changedFields, 'PickupTimeRange') ||
          get(changedFields, 'PickupDateTime') ||
          get(changedFields, 'DeliveryDateTime') ||
          get(changedFields, 'DeliveryTimeRange')
        ) {
          const { shipperTimeZone, consigneeTimeZone } =
            await getShipperAndConsigneeTimezones(orderNo);
          if (changedFields.PickupDateTime) {
            changedFields.PickupDateTime = await getCstTime({
              datetime: get(changedFields, 'PickupDateTime'),
              timezone: shipperTimeZone,
            });
          }
          if (changedFields.PickupTimeRange) {
            changedFields.PickupTimeRange = await getCstTime({
              datetime: get(changedFields, 'PickupTimeRange'),
              timezone: shipperTimeZone,
            });
          }
          if (changedFields.DeliveryDateTime) {
            changedFields.DeliveryDateTime = await getCstTime({
              datetime: get(changedFields, 'DeliveryDateTime'),
              timezone: consigneeTimeZone,
            });
          }
          if (changedFields.DeliveryTimeRange) {
            changedFields.DeliveryTimeRange = await getCstTime({
              datetime: get(changedFields, 'DeliveryTimeRange'),
              timezone: consigneeTimeZone,
            });
          }
        }

        const stopsFromResponse = cloneDeep(get(orderResponse, 'stops', []));
        console.info(
          '🚀 ~ file: index.js:81 ~ event.Records.map ~ stopsFromResponse:',
          stopsFromResponse
        );
        const updatedStops = await updateStopsFromChangedFields(
          stopsFromResponse,
          changedFields,
          brokerageStatus
        );
        console.info('🚀 ~ file: index.js:82 ~ event.Records.map ~ updatedStops:', updatedStops);

        // In the response we need to update the stops array and need to send the entire response with the updates stops array
        const updatedResponse = cloneDeep(orderResponse);
        updatedResponse.stops = updatedStops;

        if (isEqual(orderResponse, updatedResponse)) {
          console.info(`Payload is the same as the old payload. Skipping further processing.
                \n Payload: ${JSON.stringify(updatedResponse)}`);
          return 'Payload is the same as the old payload. Skipping further processing';
        }
        console.info('Condition Satisfied. calling update orders api');
        // Send update
        await updateOrders({ payload: updatedResponse });
        // UPDATE updatecount in orderstatus table - new columns - UpdatePayload, UpdateCount
        await setUpdateCount({
          orderNo,
          UpdateCount: get(logQueryResult, '[0].UpdateCount', 0),
          updatedResponse,
        });
      } else {
        console.info('Update not found in log table.');
      }
      return true;
    })
  );
};

const queryOrderStatusTable = async (orderNo) => {
  const orderParam = {
    TableName: STATUS_TABLE,
    KeyConditionExpression: 'FK_OrderNo = :orderNo',
    ExpressionAttributeValues: {
      ':orderNo': orderNo,
    },
  };
  console.info('orderParam:', orderParam);
  try {
    const result = await dynamoDb.query(orderParam).promise();
    return get(result, 'Items', []);
  } catch (error) {
    console.error('Error querying status table:', error);
    throw error;
  }
};

async function updateOrderStatusTable({ orderNo }) {
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

// Function to update stops based on changed fields
async function updateStopsFromChangedFields(stops, changedFields, brokerageStatus) {
  for (const stop of stops) {
    // eslint-disable-next-line camelcase
    const { order_sequence } = stop;

    // eslint-disable-next-line camelcase
    if (order_sequence === 1) {
      // Handle Pickup
      updatePickupFields(stop, changedFields, brokerageStatus);
      // eslint-disable-next-line camelcase
    } else if (order_sequence === 2) {
      // Handle Delivery
      updateDeliveryFields(stop, changedFields, brokerageStatus);
    }
  }

  return stops;
}

// Function to update pickup fields
function updatePickupFields(stop, changedFields, brokerageStatus) {
  if (includes(['OPEN', 'BOOKED', 'NEWOMNI', 'COVERED'], brokerageStatus)) {
    if (changedFields.PickupDateTime) {
      stop.sched_arrive_early = changedFields.PickupDateTime;
    }
    if (changedFields.PickupTimeRange) {
      stop.sched_arrive_late = changedFields.PickupTimeRange;
    }
    if (changedFields.ShipContact) {
      stop.contact_name = changedFields.ShipContact;
    }
    if (changedFields.ShipPhone) {
      stop.phone = changedFields.ShipPhone;
    }
  }
  if (includes(['OPEN', 'NEWOMNI'], brokerageStatus)) {
    if (changedFields.ShipName) {
      stop.location_name = changedFields.ShipName;
    }
    if (changedFields.ShipZip) {
      stop.zip_code = changedFields.ShipZip;
    }
    if (changedFields.ShipCity) {
      stop.city_name = changedFields.ShipCity;
    }
    if (changedFields.ShipAddress1) {
      stop.address = changedFields.ShipAddress1;
    }
    if (changedFields.ShipAddress2) {
      stop.address2 = changedFields.ShipAddress2;
    }
    if (changedFields.FK_ShipState) {
      stop.state = changedFields.FK_ShipState;
    }
  }
}

// Function to update delivery fields
function updateDeliveryFields(stop, changedFields, brokerageStatus) {
  if (includes(['OPEN', 'BOOKED', 'NEWOMNI', 'COVERED'], brokerageStatus)) {
    if (changedFields.ConContact) {
      stop.contact_name = changedFields.ConContact;
    }
    if (changedFields.ConPhone) {
      stop.phone = changedFields.ConPhone;
    }
    if (changedFields.DeliveryDateTime) {
      stop.sched_arrive_early = changedFields.DeliveryDateTime;
    }
    if (changedFields.DeliveryTimeRange) {
      stop.sched_arrive_late = changedFields.DeliveryTimeRange;
    }
  }
  if (includes(['OPEN', 'NEWOMNI'], brokerageStatus)) {
    if (changedFields.ConAddress1) {
      stop.address = changedFields.ConAddress1;
    }
    if (changedFields.ConAddress2) {
      stop.address2 = changedFields.ConAddress2;
    }
    if (changedFields.ConCity) {
      stop.city_name = changedFields.ConCity;
    }
    if (changedFields.ConName) {
      stop.location_name = changedFields.ConName;
    }
    if (changedFields.FK_ConState) {
      stop.state = changedFields.FK_ConState;
    }
    if (changedFields.ConZip) {
      stop.zip_code = changedFields.ConZip;
    }
  }
}

async function getShipperAndConsigneeTimezones(orderNo) {
  // Query DynamoDB table to get the necessary shipper data
  const Params = {
    TableName: CONFIRMATION_COST,
    IndexName: CONFIRMATION_COST_INDEX_KEY_NAME,
    KeyConditionExpression: 'FK_OrderNo = :orderNo',
    ExpressionAttributeValues: {
      ':orderNo': orderNo,
    },
  };

  const stateData = await dynamoDb.query(Params).promise();

  // Extract required data from stateData for shipper
  const shipperCity = get(stateData, 'Items[0].ShipCity');
  const shipperState = get(stateData, 'Items[0].FK_ShipState');
  const shipperCountry = get(stateData, 'Items[0].FK_ShipCountry');
  const shipperAddress1 = get(stateData, 'Items[0].ShipAddress1');
  // Extract required data from stateData for consignee
  const consigneeCity = get(stateData, 'Items[0].ConCity');
  const consigneeState = get(stateData, 'Items[0].FK_ConState');
  const consigneeCountry = get(stateData, 'Items[0].FK_ConCountry');
  const consigneeAddress1 = get(stateData, 'Items[0].ConAddress1');

  // Call getTimezone function with extracted data for shipper
  const shipperTimeZone = await getTimezone({
    stopCity: shipperCity,
    state: shipperState,
    country: shipperCountry,
    address1: shipperAddress1,
  });

  // Call getTimezone function with extracted data for consignee
  const consigneeTimeZone = await getTimezone({
    stopCity: consigneeCity,
    state: consigneeState,
    country: consigneeCountry,
    address1: consigneeAddress1,
  });

  return { shipperTimeZone, consigneeTimeZone };
}
