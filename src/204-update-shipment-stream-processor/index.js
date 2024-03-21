'use strict';

const AWS = require('aws-sdk');
const _ = require('lodash');
const moment = require('moment-timezone');

const dynamoDb = new AWS.DynamoDB.DocumentClient();

// Constants from environment variables
const {
  STATUS_TABLE,
  CONFIRMATION_COST,
  CONFIRMATION_COST_INDEX_KEY_NAME,
  CONSOL_STOP_HEADERS,
  CONSOLE_STATUS_TABLE,
} = process.env;

// Constants from shared files
const { STATUSES } = require('../shared/constants/204_create_shipment');
const { updateOrders, getOrders } = require('../shared/204-payload-generator/apis');
const { getCstTime, getTimezone } = require('../shared/204-payload-generator/helper');

module.exports.handler = async (event) => {
  try {
    console.info('Event:', event);

    await Promise.all(_.map(event.Records, processRecord));
  } catch (error) {
    console.error('Error processing records:', error);
    throw error;
  }
};

async function processRecord(record) {
  try {
    const body = JSON.parse(_.get(record, 'body', ''));
    const message = JSON.parse(_.get(body, 'Message', ''));

    const newImage = AWS.DynamoDB.Converter.unmarshall(_.get(message, 'NewImage', {}));
    const oldImage = AWS.DynamoDB.Converter.unmarshall(_.get(message, 'OldImage', {}));
    const dynamoTableName = _.get(message, 'dynamoTableName', '');

    if (dynamoTableName === CONSOL_STOP_HEADERS) {
      await handleUpdatesforMt(newImage, oldImage);
    } else {
      await handleUpdatesForP2P(newImage, oldImage);
    }
  } catch (error) {
    console.error('Error processing record:', error);
    throw error;
  }
}

async function handleUpdatesForP2P(newImage, oldImage) {
  try {
    const orderNo = _.get(newImage, 'FK_OrderNo');
    const changedFields = findChangedFields(newImage, oldImage);

    // Query log table
    const logQueryResult = await queryOrderStatusTable(orderNo);
    const status = _.get(logQueryResult, '[0].Status', '');

    if (!_.isEmpty(logQueryResult) && status === STATUSES.FAILED) {
      await updateOrderStatusTable({ orderNo });
    } else if (!_.isEmpty(logQueryResult) && status === STATUSES.SENT) {
      const response = _.get(logQueryResult, '[0].Response', {});
      const id = _.get(response, 'id', '');

      if (_.isEmpty(id)) {
        console.info('Skipping the process as the shipment id is not available');
        return;
      }

      const orderResponse = await getOrders({ id });
      const movements = _.get(orderResponse, 'movements', []);

      if (_.isArray(movements) && movements.length > 1) {
        console.info('There are more than 1 movement arrays in the object.');
        return;
      }

      const brokerageStatus = _.get(movements, '[0].brokerage_status', '');

      if (!_.includes(['OPEN', 'BOOKED', 'NEWOMNI', 'COVERED'], brokerageStatus)) {
        console.info('Brokerage status is not OPEN, BOOKED, NEWOMNI, or COVERED.');
        return;
      }

      await updateTimeFieldforConsolAndNonConsole(changedFields, orderNo);
      const stopsFromResponse = _.cloneDeep(_.get(orderResponse, 'stops', []));
      const updatedStops = await updateStopsFromChangedFields(
        stopsFromResponse,
        changedFields,
        brokerageStatus
      );

      const updatedResponse = _.cloneDeep(orderResponse);
      updatedResponse.stops = updatedStops;

      if (_.isEqual(orderResponse, updatedResponse)) {
        console.info(`Payload is the same as the old payload. Skipping further processing.
            \n Payload: ${JSON.stringify(updatedResponse)}`);
        return;
      }

      await updateOrders({ payload: updatedResponse });
      await updateRecord({
        orderNo,
        UpdateCount: _.get(logQueryResult, '[0].UpdateCount', 0),
        updatedResponse,
        oldResponse: _.get(logQueryResult, '[0].UpdatedResponse', {}),
      });
    }
  } catch (error) {
    console.error('Error handling order updates:', error);
    throw error;
  }
}

async function handleUpdatesforMt(newImage, oldImage) {
  try {
    const consolNo = _.get(newImage, 'FK_ConsolNo');
    const changedFields = findChangedFields(newImage, oldImage);

    console.info('🚀 ~ file: index.js:66 ~ event.Records.map ~ changedFields:', changedFields);
    // Query log table
    const logQueryResult = await queryConsolStatusTable(consolNo);
    if (!_.isEmpty(logQueryResult) && _.get(logQueryResult[0], 'Status') === STATUSES.FAILED) {
      // retrigger the process by updating the status to 'PENDING'
      return await updateConsolStatusTable({ consolNo });
    } else if (!_.isEmpty(logQueryResult) && _.get(logQueryResult[0], 'Status') === STATUSES.SENT) {
      const response = _.get(logQueryResult, '[0]Response');

      const id = _.get(response, 'id', '');
      console.info('🚀 ~ file: index.js:46 ~ event.Records.map ~ id:', id);
      if (_.isEmpty(id)) {
        console.info('Skipping the process as the shipment id is not available');
        return true;
      }
      const orderResponse = await getOrders({ id });
      console.info('🚀 ~ file: index.js:43 ~ event.Records.map ~ orderResponse:', orderResponse);

      const movements = _.get(orderResponse, 'movements');

      // Check if movements is an array and if its length is greater than 1
      if (_.isArray(movements) && movements.length > 1) {
        console.info('There are more than 1 movement arrays in the object.');
        return true;
      }
      console.info('There are only 1 movement arrays in the object.');
      // check if the movements array has brokerage_status as  "OPEN", "BOOKED", "NEWOMNI", or "COVERED"
      const brokerageStatus = _.get(movements, '[0].brokerage_status', '');
      if (!_.includes(['OPEN', 'BOOKED', 'NEWOMNI', 'COVERED'], brokerageStatus)) {
        console.info('Brokerage status is not OPEN, BOOKED, NEWOMNI, or COVERED.');
        return true;
      }
      console.info('Brokerage status is OPEN, BOOKED, NEWOMNI, or COVERED.');
      // update time fields if there are any changes.
      await updatetimeFields(changedFields, newImage);

      const stopsFromResponse = _.cloneDeep(_.get(orderResponse, 'stops', []));

      console.info(
        '🚀 ~ file: index.js:81 ~ event.Records.map ~ stopsFromResponse:',
        stopsFromResponse
      );
      const updatedStops = await updateStopsForMt(
        stopsFromResponse,
        changedFields,
        brokerageStatus,
        newImage
      );
      console.info('🚀 ~ file: index.js:82 ~ event.Records.map ~ updatedStops:', updatedStops);

      // In the response we need to update the stops array and need to send the entire response with the updates stops array
      const updatedResponse = _.cloneDeep(orderResponse);
      updatedResponse.stops = updatedStops;

      if (_.isEqual(orderResponse, updatedResponse)) {
        console.info(`Payload is the same as the old payload. Skipping further processing.
            \n Payload: ${JSON.stringify(updatedResponse)}`);
        return true;
      }

      await updateOrders({ payload: updatedResponse });
      await updateRecordForMt({
        consolNo,
        UpdateCount: _.get(logQueryResult, '[0].UpdateCount', 0),
        updatedResponse,
        oldResponse: _.get(logQueryResult, '[0].UpdatedResponse', {}),
      });
    }
    return true;
  } catch (error) {
    console.error('Error handling consolidation updates:', error);
    throw error;
  }
}

function findChangedFields(newImage, oldImage) {
  const changedFields = {};
  Object.keys(newImage).forEach((key) => {
    if (newImage[key] !== oldImage[key]) {
      changedFields[key] = newImage[key];
    }
  });
  return changedFields;
}

async function updateTimeFieldforConsolAndNonConsole(changedFields, orderNo) {
  if (
    _.get(changedFields, 'PickupTimeRange') ||
    _.get(changedFields, 'PickupDateTime') ||
    _.get(changedFields, 'DeliveryDateTime') ||
    _.get(changedFields, 'DeliveryTimeRange')
  ) {
    const { shipperTimeZone, consigneeTimeZone } = await getShipperAndConsigneeTimezones(orderNo);
    if (changedFields.PickupDateTime) {
      changedFields.PickupDateTime = await getCstTime({
        datetime: _.get(changedFields, 'PickupDateTime'),
        timezone: shipperTimeZone,
      });
    }
    if (changedFields.PickupTimeRange) {
      changedFields.PickupTimeRange = await getCstTime({
        datetime: _.get(changedFields, 'PickupTimeRange'),
        timezone: shipperTimeZone,
      });
    }
    if (changedFields.DeliveryDateTime) {
      changedFields.DeliveryDateTime = await getCstTime({
        datetime: _.get(changedFields, 'DeliveryDateTime'),
        timezone: consigneeTimeZone,
      });
    }
    if (changedFields.DeliveryTimeRange) {
      changedFields.DeliveryTimeRange = await getCstTime({
        datetime: _.get(changedFields, 'DeliveryTimeRange'),
        timezone: consigneeTimeZone,
      });
    }
  }
}

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
    return _.get(result, 'Items', []);
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

async function updateRecord({ orderNo, UpdateCount, updatedResponse, oldResponse }) {
  const timestamp = moment.tz('America/Chicago').format();
  // Create a new object for the updated response with the current timestamp
  oldResponse[timestamp] = updatedResponse;
  const updateParams = {
    TableName: STATUS_TABLE,
    Key: { FK_OrderNo: orderNo },
    UpdateExpression: 'set UpdateCount = :updatecount, #updatedresponse = :updatedresponse',
    ExpressionAttributeNames: {
      '#updatedresponse': 'UpdatedResponse',
    },
    ExpressionAttributeValues: {
      ':updatedresponse': oldResponse,
      ':updatecount': UpdateCount + 1,
    },
  };

  console.info('🚀 ~ file: index.js:145 ~ updateRecord ~ updateParams:', updateParams);
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
  const isBrokerageStatusOBNC = _.includes(
    ['OPEN', 'BOOKED', 'NEWOMNI', 'COVERED'],
    brokerageStatus
  );
  const isBrokerageStatusON = _.includes(['OPEN', 'NEWOMNI'], brokerageStatus);

  if (isBrokerageStatusOBNC) {
    updateONBCPickupFields(stop, changedFields);
  }

  if (isBrokerageStatusON) {
    updateONPickupFields(stop, changedFields);
  }
}

// Function to update delivery fields
function updateDeliveryFields(stop, changedFields, brokerageStatus) {
  const isBrokerageStatusOBNC = _.includes(
    ['OPEN', 'BOOKED', 'NEWOMNI', 'COVERED'],
    brokerageStatus
  );
  const isBrokerageStatusON = _.includes(['OPEN', 'NEWOMNI'], brokerageStatus);

  if (isBrokerageStatusOBNC) {
    updateONBCDeliveryFields(stop, changedFields);
  }

  if (isBrokerageStatusON) {
    updateONDeliveryFields(stop, changedFields);
  }
}

// Function to update pickup fields for  'OPEN', 'BOOKED', 'NEWOMNI', 'COVERED' brokerage status
function updateONBCPickupFields(stop, changedFields) {
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

// Function to update pickup fields for OPEN, NEWOMNI brokerage status
function updateONPickupFields(stop, changedFields) {
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

// Function to update delivery fields for 'OPEN', 'BOOKED', 'NEWOMNI', 'COVERED' brokerage status
function updateONBCDeliveryFields(stop, changedFields) {
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

// Function to update delivery fields for OPEN, NEWOMNI brokerage status
function updateONDeliveryFields(stop, changedFields) {
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

async function getShipperAndConsigneeTimezones(orderNo) {
  try {
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
    const shipperCity = _.get(stateData, 'Items[0].ShipCity');
    const shipperState = _.get(stateData, 'Items[0].FK_ShipState');
    const shipperCountry = _.get(stateData, 'Items[0].FK_ShipCountry');
    const shipperAddress1 = _.get(stateData, 'Items[0].ShipAddress1');
    // Extract required data from stateData for consignee
    const consigneeCity = _.get(stateData, 'Items[0].ConCity');
    const consigneeState = _.get(stateData, 'Items[0].FK_ConState');
    const consigneeCountry = _.get(stateData, 'Items[0].FK_ConCountry');
    const consigneeAddress1 = _.get(stateData, 'Items[0].ConAddress1');

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
  } catch (error) {
    console.error('🚀 ~ file: index.js:381 ~ getShipperAndConsigneeTimezones ~ error:', error);
    throw error;
  }
}

async function updateRecordForMt({ consolNo, UpdateCount, updatedResponse, oldResponse }) {
  const timestamp = moment.tz('America/Chicago').format();
  // Create a new object for the updated response with the current timestamp
  oldResponse[timestamp] = updatedResponse;
  const updateParams = {
    TableName: CONSOLE_STATUS_TABLE,
    Key: { ConsolNo: consolNo },
    UpdateExpression: 'set UpdateCount = :updatecount, #updatedresponse = :updatedresponse',
    ExpressionAttributeNames: {
      '#updatedresponse': 'UpdatedResponse',
    },
    ExpressionAttributeValues: {
      ':updatedresponse': oldResponse,
      ':updatecount': UpdateCount + 1,
    },
  };

  console.info('🚀 ~ file: index.js:145 ~ updateRecord ~ updateParams:', updateParams);
  console.info('Update status table parameters:', updateParams);

  try {
    await dynamoDb.update(updateParams).promise();
  } catch (error) {
    console.error('Error updating status table:', error);
    throw error;
  }
}

function updateStopsForMt(stopsFromResponse, changedFields, brokerageStatus, newImage) {
  try {
    // Parse newImage to get ConsolStopNumber
    const { ConsolStopNumber } = newImage;
    const orderSequence = _.toNumber(ConsolStopNumber) + 1;
    console.info('🚀 ~ file: index.js:493 ~ orderSequence:', orderSequence);
    // Iterate over stopsFromResponse to find the stop with the matching order_sequence
    for (const stop of stopsFromResponse) {
      // Update fields based on brokerageStatus and changedFields
      if (stop.order_sequence === orderSequence) {
        updateChangedFieldsforMt(stop, changedFields, brokerageStatus);
        break;
      }
    }
    return stopsFromResponse;
  } catch (error) {
    console.error(error);
    throw error;
  }
}

function updateChangedFieldsforMt(stop, changedFields, brokerageStatus) {
  const isBrokerageStatusOBNC = _.includes(
    ['OPEN', 'BOOKED', 'NEWOMNI', 'COVERED'],
    brokerageStatus
  );
  const isBrokerageStatusON = _.includes(['OPEN', 'NEWOMNI'], brokerageStatus);

  if (isBrokerageStatusOBNC) {
    updateONBCFields(stop, changedFields);
  }

  if (isBrokerageStatusON) {
    updateONFields(stop, changedFields);
  }
}

function updateONBCFields(stop, changedFields) {
  if (changedFields.ConsolStopContact) {
    stop.contact_name = changedFields.ConsolStopContact;
  }
  if (changedFields.ConsolStopPhone) {
    stop.phone = changedFields.ConsolStopPhone;
  }
  if (changedFields.ConsolStopTimeBegin) {
    stop.sched_arrive_early = changedFields.ConsolStopTimeBegin;
  }
  if (changedFields.ConsolStopTimeEnd) {
    stop.sched_arrive_late = changedFields.ConsolStopTimeEnd;
  }
}

function updateONFields(stop, changedFields) {
  if (changedFields.ConsolStopAddress1) {
    stop.address = changedFields.ConsolStopAddress1;
  }
  if (changedFields.ConsolStopAddress2) {
    stop.address2 = changedFields.ConsolStopAddress2;
  }
  if (changedFields.ConsolStopCity) {
    stop.city_name = changedFields.ConsolStopCity;
  }
  if (changedFields.ConsolStopName) {
    stop.location_name = changedFields.ConsolStopName;
  }
  if (changedFields.FK_ConsolStopState) {
    stop.state = changedFields.FK_ConsolStopState;
  }
  if (changedFields.ConsolStopZip) {
    stop.zip_code = changedFields.ConsolStopZip;
  }
}

async function updatetimeFields(changedFields, newImage) {
  console.info('🚀 ~ file: index.js:591 ~ updatetimeFields ~ newImage:', newImage)
  if (
    _.get(changedFields, 'ConsolStopTimeBegin') ||
    _.get(changedFields, 'ConsolStopTimeEnd') ||
    _.get(changedFields, 'ConsolStopDate')
  ) {
    const { timezone } = await getTimezone({
      stopCity: _.get(newImage, 'ConsolStopCity'),
      state: _.get(newImage, 'FK_ConsolStopState'),
      country: _.get(newImage, 'FK_ConsolStopCountry'),
      address1: _.get(newImage, 'ConsolStopAddress1'),
    });
    console.info('🚀 ~ file: index.js:605 ~ updatetimeFields ~ timezone:', timezone)
    if (changedFields.ConsolStopTimeBegin) {
      const consolStopDate = _.get(changedFields, 'ConsolStopDate');
      console.info('🚀 ~ file: index.js:611 ~ updatetimeFields ~ consolStopDate:', consolStopDate)
      const consolStopTimeBegin = _.get(changedFields, 'ConsolStopTimeBegin');
      console.info('🚀 ~ file: index.js:613 ~ updatetimeFields ~ consolStopTimeBegin:', consolStopTimeBegin)

      if (consolStopTimeBegin) {
        let datePart = '';
        if (consolStopDate) {
          datePart = consolStopDate.split(' ')[0];
        } else {
          const newImageConsolStopDate = _.get(newImage, 'ConsolStopDate');
          if (newImageConsolStopDate) {
            datePart = newImageConsolStopDate.split(' ')[0];
          }
        }

        const timePart = consolStopTimeBegin.split(' ')[1];

        if (datePart && timePart) {
          const datetime = `${datePart} ${timePart}`;
          console.info('🚀 ~ file: index.js:629 ~ updatetimeFields ~ datetime:', datetime)
          changedFields.ConsolStopTimeBegin = await getCstTime({ datetime, timezone });
          console.info('🚀 ~ file: index.js:629 ~ updatetimeFields ~ changedFields.ConsolStopTimeBegin:', changedFields.ConsolStopTimeBegin)
        }
      }
    }

    if (changedFields.ConsolStopTimeEnd) {
      const consolStopDate = _.get(changedFields, 'ConsolStopDate');
      console.info('🚀 ~ file: index.js:633 ~ updatetimeFields ~ consolStopDate:', consolStopDate)
      const consolStopTimeEnd = _.get(changedFields, 'ConsolStopTimeEnd');
      console.info('🚀 ~ file: index.js:635 ~ updatetimeFields ~ consolStopTimeEnd:', consolStopTimeEnd)

      if (consolStopTimeEnd) {
        let datePart = '';
        if (consolStopDate) {
          datePart = consolStopDate.split(' ')[0];
        } else {
          const newImageConsolStopDate = _.get(newImage, 'ConsolStopDate');
          if (newImageConsolStopDate) {
            datePart = newImageConsolStopDate.split(' ')[0];
          }
        }

        const timePart = consolStopTimeEnd.split(' ')[1];

        if (datePart && timePart) {
          const datetime = `${datePart} ${timePart}`;
          changedFields.ConsolStopTimeBegin = await getCstTime({ datetime, timezone });
          console.info('🚀 ~ file: index.js:653 ~ updatetimeFields ~ datetime:', datetime)
        }
      }
    }

    console.info('updated the time fields successfully');
  }
}

async function queryConsolStatusTable(consolNo) {
  try {
    const queryParams = {
      TableName: CONSOLE_STATUS_TABLE,
      KeyConditionExpression: 'ConsolNo = :consolNo',
      ExpressionAttributeValues: {
        ':consolNo': consolNo,
      },
    };
    console.info('Query status table parameters:', queryParams);
    const result = await dynamoDb.query(queryParams).promise();
    return _.get(result, 'Items', []);
  } catch (error) {
    console.error('🚀 ~ file: index.js:460 ~ queryConsolStatusTable ~ error:', error);
    throw error;
  }
}

async function updateConsolStatusTable({ consolNo }) {
  try {
    const updateParams = {
      TableName: CONSOLE_STATUS_TABLE,
      Key: { ConsolNo: consolNo },
      UpdateExpression: 'set #Status = :status, ResetCount = :resetCount',
      ExpressionAttributeNames: { '#Status': 'Status' },
      ExpressionAttributeValues: {
        ':status': STATUSES.PENDING,
        ':resetCount': 0,
      },
    };
    console.info('Update status table parameters:', updateParams);
    await dynamoDb.update(updateParams).promise();
    return 'success';
  } catch (error) {
    console.error('🚀 ~ file: index.js:481 ~ updateConsolStatusTable ~ error:', error);
    throw error;
  }
}
