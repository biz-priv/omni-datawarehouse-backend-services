'use strict';
const AWS = require('aws-sdk');
const { prepareBatchFailureObj } = require('../shared/dataHelper');
const _ = require('lodash');
// const { nonConsolPayload } = require('./payloads');
const { queryDynamoDB, getParamsByTableName, fetchLocationId } = require('./helper');

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
          'omni-wt-rt-shipment-desc-dev',
          // 'omni-wt-rt-timezone-master-dev',
          // 'omni-wt-rt-customers-dev',
        ];

        const [
          confirmationCostData,
          shipmentHeaderData,
          referencesData,
          shipperData,
          consigneeData,
          shipmentDescData,
        ] = await Promise.all(
          tables.map(async (table) => {
            const param = getParamsByTableName(_.get(shipmentAparData, 'FK_OrderNo'), table);
            console.info('🙂 -> file: index.js:35 -> tables.map -> param:', param);
            const response = await queryDynamoDB(param);
            return _.get(response, 'Items.[0]', false);
          })
        );

        const customersParams = getParamsByTableName(
          '',
          'omni-wt-rt-customers-dev',
          '',
          _.get(shipmentHeaderData, 'Items[0].BillNo', '')
        );
        const customersData = _.get(await queryDynamoDB(customersParams), 'Items.[0]', false);

        console.info('🙂 -> file: index.js:54 -> promises -> customersData:', customersData);
        console.info('🙂 -> file: index.js:38 -> promises -> shipmentDescData:', shipmentDescData);
        console.info('🙂 -> file: index.js:73 -> promises -> consigneeData:', consigneeData);
        console.info('🙂 -> file: index.js:73 -> promises -> shipperData:', shipperData);
        console.info('🙂 -> file: index.js:73 -> promises -> referencesData:', referencesData);
        console.info(
          '🙂 -> file: index.js:73 -> promises -> shipmentHeaderData:',
          shipmentHeaderData
        );
        console.info(
          '🙂 -> file: index.js:73 -> promises -> confirmationCostData:',
          confirmationCostData
        );

        if (
          !customersData ||
          !shipmentDescData ||
          !consigneeData ||
          !shipperData ||
          !referencesData ||
          !shipmentHeaderData ||
          !confirmationCostData
        ) {
          console.error('All tables are not populated.');
          throw new Error('All tables are not populated.');
        }

        const { shipperLocationId, consigneeLocationId } = await fetchLocationId();
        console.info(
          '🙂 -> file: index.js:83 -> promises ->  shipperLocationId, consigneeLocationId:',
          shipperLocationId,
          consigneeLocationId
        );
        if (!shipperLocationId || !consigneeLocationId) {
          console.error('Could fetch location id.');
          throw new Error('Could fetch location id.');
        }
        return;
        // if (_.get(shipmentAparData, 'ConsolNo') === '0') {
        //   const customerName = _.get(customersData, 'Items[0].CustName');
        //   await nonConsolPayload({
        //     shipmentHeaderData,
        //     shipmentDescData,
        //     referencesData,
        //     customerName,
        //   });
        // }
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
