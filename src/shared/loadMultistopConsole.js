'use strict';
const AWS = require('aws-sdk');
const {
  getLiftGate,
  getUnNum,
  validatePayload,
  getHazardous,
  getGMTDiff,
  getStatus,
  sortObjByStopNo,
  checkAddressByGoogleApi,
  checkIfShipmentHeaderOrderDatePass,
} = require('./dataHelper');
const moment = require('moment-timezone');
const { v4: uuidv4 } = require('uuid');
const { snsPublishMessage } = require('./sns');
const { get } = require('lodash');
const { Put: put } = require('./dynamo');

const ddb = new AWS.DynamoDB.DocumentClient({
  region: process.env.REGION,
});

const {
  SHIPMENT_APAR_TABLE, // "T19262"
  SHIPMENT_HEADER_TABLE,
  INSTRUCTIONS_TABLE,
  SHIPMENT_DESC_TABLE,
  CONSOL_STOP_HEADERS,
  CONSOL_STOP_ITEMS,
  CUSTOMER_TABLE,
  INSTRUCTIONS_INDEX_KEY_NAME,
  IVIA_DDB,
  IVIA_VENDOR_ID,
  EQUIPMENT_TABLE,
  IVIA_CARRIER_ID,
  STAGE,
} = process.env;
// const IVIA_CARRIER_ID = "102"; //NOTE:- for stage IVIA need to change it later
const globalConsolIndex = `omni-ivia-ConsolNo-index-${STAGE}`;

const loadMultistopConsole = async (dynamoData, shipmentAparData) => {
  console.info('load-Multi-stop-Console');

  const CONSOL_NO = shipmentAparData.ConsolNo;

  /**
   * get data from all the requied tables
   * shipmentApar
   * shipmentInstructions
   * shipmentHeader
   * customer
   * shipmentDesc
   * consolStopHeaders
   * consolStopItems
   */
  const dataSet = await fetchDataFromTablesList(CONSOL_NO);
  const shipmentApar = dataSet.shipmentApar;
  const shipmentHeader = dataSet.shipmentHeader;
  const shipmentDesc = dataSet.shipmentDesc;
  const customer = get(dataSet, 'customer[0]', {});
  const consolStopHeaders = dataSet.consolStopHeaders;
  const consolStopItems = dataSet.consolStopItems;
  // const shipmentInstructions = dataSet.shipmentInstructions;
  const equipment = dataSet.equipment.length > 0 ? dataSet.equipment[0] : {};

  /**
   * we need to check in the shipmentHeader.OrderDate >= '2023:04:01 00:00:00' - for both nonconsol and consol -> if this condition satisfies, we send the event to Ivia, else we ignore
   * Ignore the event if there is no OrderDate or it is "1900
   */
  if (!checkIfShipmentHeaderOrderDatePass(shipmentHeader)) {
    console.info('event IGNORED shipmentHeader.OrderDate LESS THAN 2023:04:01 00:00:00 ');
    return {};
  }

  // only used for liftgate
  const shipmentAparCargo = dataSet.shipmentAparCargo;

  // get all the FK_OrderNo from shipmentApar
  const ORDER_NO_LIST = shipmentApar.map((e) => e.FK_OrderNo);

  let dataArr = [];
  /**
   * merging consolStopItems with consolStopHeaders
   */
  consolStopItems.map((e) => {
    const csh = consolStopHeaders
      .filter((es) => es.PK_ConsolStopId === e.FK_ConsolStopId)
      .map((es) => {
        let houseBillList = shipmentHeader.filter((esh) => esh.PK_OrderNo === e.FK_OrderNo);
        houseBillList = houseBillList.length > 0 ? houseBillList[0].Housebill : '';
        return { ...es, ...e, Housebill: houseBillList };
      });
    dataArr = [...dataArr, ...csh];
    return dataArr;
  });

  /**
   * grouping by pickup type records based on ConsolStopNumber and
   * filtering with ConsolStopPickupOrDelivery === "false"
   */
  const pTypeShipment = groupBy(
    dataArr.filter((e) => e.ConsolStopPickupOrDelivery === 'false'),
    'ConsolStopNumber'
  );

  /**
   * grouping by delivery type records based on ConsolStopNumber and
   * filtering with ConsolStopPickupOrDelivery === "true"
   */
  const dTypeShipment = groupBy(
    dataArr.filter((e) => e.ConsolStopPickupOrDelivery === 'true'),
    'ConsolStopNumber'
  );

  /**
   * prepare the Pickup type obj from consolStopHeader
   */
  const pTypeShipmentMap = Object.keys(pTypeShipment).map((e) => {
    const ele = pTypeShipment[e];
    const csh = ele[0];
    console.info(csh);

    /**
     * preparing cargo obj form table shipmentDesc based on shipmentApar.FK_OrderNo
     */
    const cargo = getCargoData(shipmentDesc, ele);

    // const fkOrderNoListIns = [...new Set(ele.map((element) => element.FK_OrderNo))];
    // fetch notes from Instructions table based on shipment_apar table FK_OrderNo data
    // getting pickup type notes based on Type == "P"
    // const sInsNotes = shipmentInstructions
    //   .filter((si) => si.Type.toUpperCase() === 'P' && fkOrderNoListIns.includes(si.FK_OrderNo))
    //   .map((ei) => ei.Note)
    //   .join(' ');

    /**
     * prepare pickup notes based on consolStopHeader.ConsolStopTimeEnd
     * and consolStopHeader.ConsolStopTimeBegin
     */
    let spInsMsg = 'Pickup ';
    spInsMsg +=
      csh.ConsolStopTimeEnd > csh.ConsolStopTimeBegin
        ? `between ${moment(csh.ConsolStopTimeBegin).format('HH:mm')} and ${moment(
            csh.ConsolStopTimeEnd
          ).format('HH:mm')}`
        : `at ${moment(csh.ConsolStopTimeBegin).format('HH:mm')}`;

    /**
     * prepare the Pickup type obj from consolStopHeader
     */
    let pcutoffVal;
    const pickUpcutoffTime = get(csh, 'ConsolStopTimeEnd', '');
    const pickUpCutoffDate = get(csh, 'ConsolStopDate', '');
    if (
      pickUpcutoffTime &&
      pickUpCutoffDate &&
      pickUpcutoffTime.length > 11 &&
      pickUpCutoffDate.length > 0
    ) {
      if (pickUpcutoffTime.slice(11) !== '00:00:00.000') {
        pcutoffVal = pickUpCutoffDate.slice(0, 11) + pickUpcutoffTime.slice(11);
      } else {
        pcutoffVal = null;
      }
    } else {
      pcutoffVal = null;
    }

    const stopPayload = {
      stopType: 'P',
      stopNum: e,
      housebills: [...new Set(ele.map((element) => element.Housebill))],
      address: {
        address1: csh.ConsolStopAddress1,
        address2: csh.ConsolStopAddress2,
        city: csh.ConsolStopCity,
        country: csh.FK_ConsolStopCountry,
        state: csh.FK_ConsolStopState,
        zip: csh.ConsolStopZip,
      },
      companyName: csh?.ConsolStopName,
      cargo,
      scheduledDate: `${csh.ConsolStopDate.split(' ')[0]} ${
        csh.ConsolStopTimeBegin.split(' ')?.[1] ?? ''
      }`,
      specialInstructions: `${spInsMsg}\r\n${
        csh?.ConsolStopContact.length > 0 || csh?.ConsolStopPhone.length > 0
          ? `Contact ${csh?.ConsolStopContact} ${csh?.ConsolStopPhone}\r\n`
          : ''
      }${csh.ConsolStopNotes}`.slice(0, 200),
      cutoffDate: pcutoffVal,
    };
    return stopPayload;
  });

  /**
   * prepare the Delivery type obj from consolStopHeader
   */
  const dTypeShipmentMap = Object.keys(dTypeShipment).map((e) => {
    const ele = dTypeShipment[e];
    const csh = ele[0];
    console.info(csh);

    // const fkOrderNoListIns = [...new Set(ele.map((element) => element.FK_OrderNo))];
    // fetch notes from Instructions table based on shipment_apar table FK_OrderNo data
    // getting pickup type notes based on Type == "P"
    // const sInsNotes = shipmentInstructions
    //   .filter((si) => si.Type.toUpperCase() === 'D' && fkOrderNoListIns.includes(si.FK_OrderNo))
    //   .map((ei) => ei.Note)
    //   .join(' ');

    /**
     * prepare Delivery notes based on consolStopHeader.ConsolStopTimeEnd
     * and consolStopHeader.ConsolStopTimeBegin
     */
    let spInsMsg = 'Deliver ';
    spInsMsg +=
      csh.ConsolStopTimeEnd > csh.ConsolStopTimeBegin
        ? `between ${moment(csh.ConsolStopTimeBegin).format('HH:mm')} and ${moment(
            csh.ConsolStopTimeEnd
          ).format('HH:mm')}`
        : `at ${moment(csh.ConsolStopTimeBegin).format('HH:mm')}`;

    /**
     * prepare Delivery type cutoffDate value
     */
    let dcutoffVal;
    const deliverycutoffTime = get(csh, 'ConsolStopTimeEnd', '');
    const deliveryCutoffDate = get(csh, 'ConsolStopDate', '');
    if (
      deliverycutoffTime &&
      deliveryCutoffDate &&
      deliverycutoffTime.length > 11 &&
      deliveryCutoffDate.length > 0
    ) {
      if (deliverycutoffTime.slice(11) !== '00:00:00.000') {
        dcutoffVal = deliveryCutoffDate.slice(0, 11) + deliverycutoffTime.slice(11);
      } else {
        dcutoffVal = null;
      }
    } else {
      dcutoffVal = null;
    }

    /**
     * prepare the Delivery type obj from consolStopHeader
     */
    const stopPayload = {
      stopType: 'D',
      stopNum: e,
      housebills: [...new Set(ele.map((element) => element.Housebill))],
      address: {
        address1: csh.ConsolStopAddress1,
        address2: csh.ConsolStopAddress2,
        city: csh.ConsolStopCity,
        country: csh.FK_ConsolStopCountry,
        state: csh.FK_ConsolStopState,
        zip: csh.ConsolStopZip,
      },
      companyName: csh?.ConsolStopName,
      scheduledDate: `${csh.ConsolStopDate.split(' ')?.[0]} ${
        csh.ConsolStopTimeBegin.split(' ')?.[1] ?? ''
      }`,
      specialInstructions: `${spInsMsg}\r\n${
        csh?.ConsolStopContact.length > 0 || csh?.ConsolStopPhone.length > 0
          ? `Contact ${csh?.ConsolStopContact} ${csh?.ConsolStopPhone}\r\n`
          : ''
      }${csh.ConsolStopNotes}`.slice(0, 200),
      cutoffDate: dcutoffVal,
    };
    return stopPayload;
  });

  /**
   * merging Pickup and delivery type stopes
   */
  const margedStops = [...pTypeShipmentMap, ...dTypeShipmentMap];
  console.info('margedStops', JSON.stringify(margedStops));

  /**
   * looping all the records and calculating the scheduledDate unix time
   */
  let stopsList = [];
  for (let index = 0; index < margedStops.length; index++) {
    const element = margedStops[index];
    const addressData = await checkAddressByGoogleApi(element.address);
    const cutoffDateDiff = await getGMTDiff(element.cutoffDate, addressData);
    stopsList = [
      ...stopsList,
      {
        ...element,
        address: addressData,
        scheduledDate: await getGMTDiff(element.scheduledDate, addressData),
        cutoffDate: cutoffDateDiff !== '' ? cutoffDateDiff : null,
      },
    ];
  }

  /**
   * filtered shipmentDesc data based on shipmentApar.FK_OrderNo to get hazardous and unNum
   */
  const filteredSD = shipmentDesc.filter((e) => ORDER_NO_LIST.includes(e.FK_OrderNo));

  const totalArray = [];
  for (let i = 0; i < shipmentApar.length; i++) {
    const element = shipmentApar[i].Total;
    totalArray.push(element);
  }

  const total = totalArray.reduce((accumulator, currentValue) => {
    return accumulator + parseFloat(currentValue);
  }, 0);

  const iviaPayload = {
    carrierId: IVIA_CARRIER_ID, // IVIA_CARRIER_ID = 1000025
    refNums: {
      refNum1: CONSOL_NO ?? '', // shipmentApar.ConsolNo
      refNum2: customer?.CustName?.slice(0, 19) ?? '', // ignore
      refNum3: get(shipmentHeader[0], 'HandlingStation', ''), // HandlingStation
    },
    shipmentDetails: {
      stops: sortObjByStopNo(stopsList, 'stopNum'),
      dockHigh: 'N', // req [Y / N]
      hazardous: getHazardous(filteredSD),
      liftGate: getLiftGate(shipmentAparCargo, shipmentHeader),
      unNum: getUnNum(filteredSD), // accepts only 4 degit number as string
      notes: equipment?.Description ?? '',
      revenue: total ?? '',
    },
  };
  console.info('iviaPayload', JSON.stringify(iviaPayload));

  /**
   * validate the payload and check if it is already processed
   */
  const { check, errorMsg, isError } = await validateAndCheckIfDataSentToIvia(
    iviaPayload,
    CONSOL_NO
  );

  if (!check) {
    // save to dynamo DB
    let houseBillList = [];
    iviaPayload.shipmentDetails.stops
      .filter((e) => e.stopType === 'P')
      .map((e) => {
        houseBillList = [...houseBillList, ...e.housebills];
        return e;
      });

    // preparing obj for dynamoDB omni-ivia
    const iviaTableData = {
      id: uuidv4(),
      data: JSON.stringify(iviaPayload),
      Housebill: houseBillList.join(','),
      ConsolNo: CONSOL_NO,
      FK_OrderNo: ORDER_NO_LIST.join(','),
      payloadType: 'MultistopConsole',
      InsertedTimeStamp: moment.tz('America/Chicago').format('YYYY:MM:DD HH:mm:ss').toString(),
      status: isError ? getStatus().FAILED : getStatus().IN_PROGRESS,
      errorMsg: isError ? JSON.stringify(errorMsg) : '',
      errorReason: isError ? 'validation error' : '',
    };
    if (isError) {
      const Message = `Reason for failure: \n 
                          ErrorMSG:- ${iviaTableData.errorMsg} \n 
                          errorReason:- ${iviaTableData.errorReason} \n 
                          ConsolNo:- ${iviaTableData.ConsolNo} \n 
                          FK_OrderNo:- ${iviaTableData.FK_OrderNo} \n 
                          payloadType:- ${iviaTableData.payloadType} \n\n 
                          IVIA Payload:- ${iviaTableData.data} \n\n 
                          DB OBJ:- ${JSON.stringify(iviaTableData)}
                          `;
      await snsPublishMessage(
        process.env.ERROR_NOTIFICATION_SNS_ARN,
        `IVIA ERROR NOTIFICATION - ${process.env.STAGE}`,
        Message
      );
    }
    const params = {
      TableName: IVIA_DDB,
      Item: iviaTableData,
    };
    await put(params);
  }

  return false;
};

/**
 * creates a group of data based on key
 * @param {*} xs
 * @param {*} key
 * @returns
 */
function groupBy(xs, key) {
  return xs.reduce((rv, x) => {
    (rv[x[key]] = rv[x[key]] || []).push(x);
    return rv;
  }, {});
}

/**
 * cargo data based on shipmentDesc table based on shipmentApar.FK_OrderNo
 */
function getCargoData(shipmentDesc, ele) {
  const fkOrderNoList = ele.map((e) => e.FK_OrderNo);
  return shipmentDesc
    .filter((e) => fkOrderNoList.includes(e.FK_OrderNo))
    .map((e) => {
      const checkIfZero =
        parseInt(e?.Length !== '' ? e?.Length : 0, 10) +
          parseInt(e?.Width !== '' ? e?.Width : 0, 10) +
          parseInt(e?.Height !== '' ? e?.Height : 0, 10) ===
        0;
      let packageType;
      if (e.FK_PieceTypeId === 'BOX') packageType = 'BOX';
      else if (e.FK_PieceTypeId === 'PLT') packageType = 'PAL';
      else packageType = 'PIE';
      return {
        packageType,
        quantity: e?.Pieces ?? '',
        length: checkIfZero ? 1 : parseInt(e?.Length, 10),
        width: checkIfZero ? 1 : parseInt(e?.Width, 10),
        height: checkIfZero ? 1 : parseInt(e?.Height, 10),
        weight: e?.Weight ? parseInt(e?.Weight, 10) : '',
        stackable: 'Y', // hardcode
        turnable: 'Y', // hardcode
      };
    })
    .filter((e) => e.quantity !== '' && e.quantity !== 0 && e.quantity !== '0');
}

/**
 * validate the payload structure and check from dynamodb if the data is sent to ivia priviously.
 * @param {*} payload
 * @param {*} ConsolNo
 * @returns 3 variables
 *  1> check :- true/false  if omni-ivia don't have record with success status then false else true
 *  2> isError: true/false if we have validation then true else false
 *  3> errorMsg: "" if isErroris true then this variable will contain the validation error msg
 */
function validateAndCheckIfDataSentToIvia(payload, ConsolNo) {
  // eslint-disable-next-line no-async-promise-executor
  return new Promise(async (resolve) => {
    const errorMsg = validatePayload(payload);
    console.info('errorMsg', errorMsg);

    try {
      // fetch from ivia table and check if data processed or not
      const params = {
        TableName: IVIA_DDB,
        IndexName: 'omni-ivia-ConsolNo-FK_OrderNo-index',
        KeyConditionExpression: 'ConsolNo = :ConsolNo',
        ExpressionAttributeValues: {
          ':ConsolNo': ConsolNo.toString(),
        },
      };
      const data = await ddb.query(params).promise();

      if (data.Items.length > 0) {
        // check if payload is processed or or in progress
        const latestData = data.Items.filter(
          (e) => e.status === getStatus().SUCCESS || e.status === getStatus().IN_PROGRESS
        );
        if (latestData.length > 0) {
          resolve({ check: true, errorMsg: '', isError: false });
        } else {
          // check if the latest failed payload is same with upcomming payload or not.
          let errorObj = data.Items.filter((e) => e.status === getStatus().FAILED);
          errorObj = errorObj.sort((x, y) => {
            return x.InsertedTimeStamp < y.InsertedTimeStamp ? 1 : -1;
          })[0];

          if (get(errorObj, 'data') && errorObj.data !== JSON.stringify(payload)) {
            if (errorMsg !== '') {
              resolve({ check: false, errorMsg, isError: true });
            } else {
              resolve({ check: false, errorMsg: '', isError: false });
            }
          } else {
            resolve({ check: true, errorMsg: '', isError: false });
          }
        }
      } else if (errorMsg !== '') {
        resolve({ check: false, errorMsg, isError: true });
      } else {
        resolve({ check: false, errorMsg: '', isError: false });
      }
    } catch (error) {
      console.error('dynamoError:', error);
      resolve({ check: false, errorMsg: '', isError: false });
    }
  });
}

/**
 * fetch data from the tables
 * @param {*} tableList
 * @param {*} primaryKeyValue
 * @returns
 */
async function fetchDataFromTablesList(CONSOL_NO) {
  try {
    /**
     * shipment apar
     */
    const sapparams = {
      TableName: SHIPMENT_APAR_TABLE,
      IndexName: globalConsolIndex,
      KeyConditionExpression: 'ConsolNo = :ConsolNo',
      FilterExpression:
        'FK_VendorId = :FK_VendorId and Consolidation = :Consolidation and FK_ServiceId = :FK_ServiceId and SeqNo <> :SeqNo and FK_OrderNo <> :FK_OrderNo',
      ExpressionAttributeValues: {
        ':ConsolNo': CONSOL_NO.toString(),
        ':FK_VendorId': IVIA_VENDOR_ID.toString(),
        ':Consolidation': 'N',
        ':FK_ServiceId': 'MT',
        ':SeqNo': '9999',
        ':FK_OrderNo': CONSOL_NO.toString(),
      },
    };

    let shipmentApar = await ddb.query(sapparams).promise();
    shipmentApar = shipmentApar.Items;

    let shipmentInstructions = [];
    let shipmentHeader = [];
    let shipmentDesc = [];
    let consolStopHeaders = [];
    let consolStopItems = [];
    for (let index = 0; index < shipmentApar.length; index++) {
      const element = shipmentApar[index];
      /**
       * shipmentInstructions
       */
      const iparams = {
        TableName: INSTRUCTIONS_TABLE,
        IndexName: INSTRUCTIONS_INDEX_KEY_NAME,
        KeyConditionExpression: 'FK_OrderNo = :FK_OrderNo',
        ExpressionAttributeValues: {
          ':FK_OrderNo': element.FK_OrderNo.toString(),
        },
      };
      const ins = await ddb.query(iparams).promise();
      shipmentInstructions = [...shipmentInstructions, ...ins.Items];

      /**
       * shipmentHeader
       */
      const shparams = {
        TableName: SHIPMENT_HEADER_TABLE,
        KeyConditionExpression: 'PK_OrderNo = :PK_OrderNo',
        ExpressionAttributeValues: {
          ':PK_OrderNo': element.FK_OrderNo.toString(),
        },
      };
      const sh = await ddb.query(shparams).promise();
      shipmentHeader = [...shipmentHeader, ...sh.Items];

      /**
       * shipmentDesc
       */
      const sdparams = {
        TableName: SHIPMENT_DESC_TABLE,
        KeyConditionExpression: 'FK_OrderNo = :FK_OrderNo',
        ExpressionAttributeValues: {
          ':FK_OrderNo': element.FK_OrderNo.toString(),
        },
      };
      const sd = await ddb.query(sdparams).promise();
      shipmentDesc = [...shipmentDesc, ...sd.Items];

      /**
       * consolStopItems
       */
      const cstparams = {
        TableName: CONSOL_STOP_ITEMS,
        KeyConditionExpression: 'FK_OrderNo = :FK_OrderNo',
        ExpressionAttributeValues: {
          ':FK_OrderNo': element.FK_OrderNo.toString(),
        },
      };
      const cst = await ddb.query(cstparams).promise();
      consolStopItems = [...consolStopItems, ...cst.Items];

      /**
       * consolStopHeader
       */
      for (let index1 = 0; index1 < consolStopItems.length; index1++) {
        const csitem = consolStopItems[index1];
        const cshparams = {
          TableName: CONSOL_STOP_HEADERS,
          KeyConditionExpression: 'PK_ConsolStopId = :PK_ConsolStopId',
          FilterExpression: 'FK_ConsolNo = :ConsolNo',
          ExpressionAttributeValues: {
            ':PK_ConsolStopId': csitem.FK_ConsolStopId.toString(),
            ':ConsolNo': CONSOL_NO.toString(),
          },
        };
        const csh = await ddb.query(cshparams).promise();
        consolStopHeaders = [...consolStopHeaders, ...csh.Items];
      }
    }

    /**
     * Fetch shipment apar for liftgate based on shipmentDesc.FK_OrderNo
     */
    const fkOrderNoList = [...new Set(shipmentDesc.map((e) => e.FK_OrderNo))];

    let shipmentAparCargo = [];
    for (let index = 0; index < fkOrderNoList.length; index++) {
      const fkOrderNo = fkOrderNoList[index];
      const sapcParams = {
        TableName: SHIPMENT_APAR_TABLE,
        KeyConditionExpression: 'FK_OrderNo = :FK_OrderNo',
        ExpressionAttributeValues: {
          ':FK_OrderNo': fkOrderNo.toString(),
        },
      };

      const sac = await ddb.query(sapcParams).promise();
      shipmentAparCargo = [...shipmentAparCargo, ...sac.Items];
    }

    /**
     * EQUIPMENT_TABLE
     */
    const shAparForEQParam = {
      TableName: SHIPMENT_APAR_TABLE,
      IndexName: globalConsolIndex,
      KeyConditionExpression: 'ConsolNo = :ConsolNo',
      FilterExpression: 'FK_VendorId = :FK_VendorId and FK_OrderNo = :FK_OrderNo',
      ExpressionAttributeValues: {
        ':ConsolNo': CONSOL_NO.toString(),
        ':FK_VendorId': IVIA_VENDOR_ID.toString(),
        ':FK_OrderNo': CONSOL_NO.toString(),
      },
    };
    let shAparForEQData = await ddb.query(shAparForEQParam).promise();
    shAparForEQData = shAparForEQData.Items;

    let equipment = [];
    if (shAparForEQData.length > 0 && shAparForEQData[0].FK_EquipmentCode) {
      const fkEquipmentCode = shAparForEQData[0].FK_EquipmentCode;
      const equipmentParam = {
        TableName: EQUIPMENT_TABLE,
        KeyConditionExpression: 'PK_EquipmentCode = :PK_EquipmentCode',
        ExpressionAttributeValues: {
          ':PK_EquipmentCode': fkEquipmentCode.toString(),
        },
      };

      const eqData = await ddb.query(equipmentParam).promise();
      equipment = eqData.Items;
    }

    /**
     * CUSTOMER_TABLE
     */
    let customer = [];
    if (shipmentHeader.length > 0 && shipmentHeader[0].BillNo !== '') {
      const customerParam = {
        TableName: CUSTOMER_TABLE,
        KeyConditionExpression: 'PK_CustNo = :PK_CustNo',
        ExpressionAttributeValues: {
          ':PK_CustNo': shipmentHeader[0].BillNo,
        },
      };
      customer = await ddb.query(customerParam).promise();
      customer = customer.Items;
    }
    return {
      shipmentApar,
      shipmentInstructions,
      shipmentHeader,
      shipmentDesc,
      consolStopHeaders,
      consolStopItems,
      shipmentAparCargo,
      equipment,
      customer,
    };
  } catch (error) {
    console.error('error', error);
    return {};
  }
}

module.exports = { loadMultistopConsole };
