'use strict';
const AWS = require('aws-sdk');
const { prepareBatchFailureObj } = require('../shared/dataHelper');
const _ = require('lodash');
const { nonConsolPayload } = require('./payloads');
const { queryDynamoDB, getParamsByTableName } = require('./helper');

module.exports.handler = async (event) => {
  console.info(
    '🙂 -> file: index.js:8 -> module.exports.handler= -> event:',
    JSON.stringify(event)
  );
  const dynamoEventRecords = event.Records;

  try {
    const promises = dynamoEventRecords.map(async (record) => {
      try {
        const shipmentAparData = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);

        const tables = [
          'omni-wt-rt-confirmation-cost-dev',
          'omni-wt-rt-shipment-header-dev',
          'omni-wt-rt-references-dev',
          'omni-wt-rt-shipper-dev',
          'omni-wt-rt-consignee-dev',
          'omni-wt-rt-shipment-apar-dev',
          'omni-wt-rt-shipment-desc-dev',
          'omni-wt-rt-timezone-master-dev',
          'omni-wt-rt-customers-dev',
        ];

        const tablesResponse = await Promise.all(
          tables.map(async (table) => {
            const param = _.get(getParamsByTableName, table, false);
            console.info('🙂 -> file: index.js:35 -> tables.map -> param:', param);
            const response = await queryDynamoDB(param);
            return _.get(response, 'Items', false);
          })
        );
        console.info('🙂 -> file: index.js:40 -> promises -> tablesResponse:', tablesResponse);

        if (_.get(shipmentAparData, 'ConsolNo') === '0') {
          await nonConsolPayload(shipmentAparData);
        }
        // else if (
        //   _.parseInt(_.get(shipmentAparData, 'ConsolNo', 0)) > 0 &&
        //   _.parseInt(_.get(shipmentAparData, 'SeqNo', 0)) < 9999
        // ) {
        //   if (
        //     _.includes(['HS', 'TL'], _.get(shipmentAparData, 'FK_ServiceId')) &&
        //     _.get(shipmentAparData, 'Consolidation') === 'Y'
        //   ) {
        //     await loadP2PConsole(dynamoData, shipmentAparData);
        //   } else if (_.get(shipmentAparData, 'FK_ServiceId') === 'MT') {
        //     return
        //     // await loadMultistopConsole(dynamoData, shipmentAparData);
        //   } else {
        //     console.info('Exception consol> 0 and shipmentApar.FK_ServiceId is not in HS/TL/MT');
        //     throw new Error('Exception');
        //   }
        // } else {
        //   console.info('Exception global');
        //   throw new Error('Exception');
        // }
      } catch (error) {
        console.info('Error', error);
      }
    });

    await Promise.all(promises);

    return prepareBatchFailureObj([]); // Modify if needed
  } catch (error) {
    console.error('Error', error);
    return prepareBatchFailureObj(dynamoEventRecords); // Modify if needed
  }
};
