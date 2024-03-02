'use strict';
const _ = require('lodash');
const {
  generateStop,
  getVesselForConsole,
  getHazmat,
  getHighValue,
  getAparDataByConsole,
  sumNumericValues,
  populateStops,
  mapEquipmentCodeToFkPowerbrokerCode,
  mapOrderTypeToMcLeod,
  getShipmentHeaderData,
  generateStopforConsole,
  getHousebillData,
  descDataForConsole,
  getCstTime,
  getTimezone,
  stationCodeInfo,
} = require('./helper');

async function nonConsolPayload({
  shipmentHeader,
  shipmentDesc,
  referencesData,
  shipperLocationId,
  consigneeLocationId,
  finalShipperData,
  finalConsigneeData,
  customersData,
  userData,
  shipmentAparData,
}) {
  let hazmat = _.get(shipmentDesc, '[0]Hazmat', false);
  // Check if Hazmat is equal to 'Y'
  if (hazmat === 'Y') {
    // Set hazmat to true if it is 'Y'
    hazmat = true;
  } else {
    // Set hazmat to false for null or other values
    hazmat = false;
  }
  const stationCode =
    _.get(shipmentHeader, 'ControllingStation', '') ||
    _.get(shipmentAparData, 'FK_HandlingStation', '');
  console.info('🚀 ~ file: payloads.js:44 ~ stationCode:', stationCode);

  if (!stationCode) {
    throw new Error('Please populate the Controlling Station');
  }

  // Call the function to get customer ID and email based on the station code
  const customerId = stationCodeInfo(stationCode);
  console.info('🚀 ~ file: payloads.js:53 ~ customerId:', customerId);

  const equipmentCode =
    _.get(shipmentHeader, 'FK_EquipmentCode', '') ||
    _.get(shipmentAparData, 'FK_EquipmentCode', 'NA');

  const deliveryStop = await generateStop(
    shipmentHeader,
    referencesData,
    2,
    'SO',
    consigneeLocationId,
    finalConsigneeData,
    'consignee',
    shipmentAparData,
    shipmentDesc
  );

  const timezone = await getTimezone({
    stopCity: _.get(finalShipperData, 'ShipCity', ''),
    state: _.get(finalShipperData, 'FK_ShipState', ''),
    country: _.get(finalShipperData, 'FK_ShipCountry', ''),
    address1: _.get(finalShipperData, 'ShipAddress1', ''),
  });
  const payload = {
    __type: 'orders',
    company_id: 'TMS',
    allow_relay: true,
    bill_distance_um: 'MI',
    bol_received: false,
    dispatch_opt: true,
    collection_method: 'P',
    commodity: _.get(shipmentDesc, '[0]Description', ''),
    customer_id: customerId,
    blnum: _.get(shipmentHeader, 'Housebill', ''),
    entered_user_id: 'apiuser',
    equipment_type_id: mapEquipmentCodeToFkPowerbrokerCode(equipmentCode),
    order_type_id: mapOrderTypeToMcLeod(equipmentCode),
    excise_disable_update: false,
    excise_taxable: false,
    force_assign: true,
    hazmat,
    high_value: _.get(shipmentHeader, 'Insurance', 0) > 100000,
    hold_reason: 'NEW API',
    include_split_point: false,
    is_autorate_dist: false,
    is_container: false,
    is_dedicated: false,
    ltl: _.includes(['FT', 'HS'], _.get(shipmentHeader, 'FK_ServiceLevelId')),
    on_hold: false,
    ordered_date: await getCstTime({
      datetime: _.get(shipmentHeader, 'OrderDate', ''),
      timezone,
    }),
    rad_date: deliveryStop.sched_arrive_late,
    ordered_method: 'M',
    order_value: _.get(shipmentHeader, 'Insurance', 0),
    pallets_required: false,
    pieces: sumNumericValues(shipmentDesc, 'Pieces'),
    preloaded: false,
    ready_to_bill: false,
    reply_transmitted: false,
    revenue_code_id: 'SPOT',
    round_trip: false,
    ship_status_to_edi: false,
    status: 'A',
    swap: true,
    teams_required: false,
    vessel: _.get(customersData, 'CustName', ''),
    weight: sumNumericValues(shipmentDesc, 'Weight', 0),
    weight_um: 'LB',
    order_mode: 'T',
    operational_status: 'CLIN',
    lock_miles: false,
    def_move_type: 'A',
    stops: [],
  };
  // Conditionally set commodity_id if hazmat is true
  if (hazmat) {
    _.set(payload, 'commodity_id', 'HAZMAT');
  }
  payload.stops.push(
    await generateStop(
      shipmentHeader,
      referencesData,
      1,
      'PU',
      shipperLocationId,
      finalShipperData,
      'shipper',
      userData,
      shipmentAparData,
      shipmentDesc
    ),
    deliveryStop
  );

  return payload;
}

async function consolPayload({
  referencesData,
  shipperLocationId,
  consigneeLocationId,
  finalShipperData,
  finalConsigneeData,
  // customersData,
  shipmentAparData,
  userData,
}) {
  const shipmentAparConsoleData = await getAparDataByConsole({
    shipmentAparData,
  });
  const shipmentHeaderData = await getShipmentHeaderData({
    shipmentAparConsoleData,
  });
  console.info('🚀 ~ file: payloads.js:143 ~ shipmentHeaderData:', shipmentHeaderData);
  const housebillData = await getHousebillData({ shipmentAparConsoleData });
  console.info('🚀 ~ file: payloads.js:145 ~ housebillData:', housebillData);
  const descData = await descDataForConsole({ shipmentAparConsoleData });
  console.info('🚀 ~ file: payloads.js:147 ~ descData:', descData);
  const equipmentCode =
    _.get(shipmentHeaderData, '[0].equipmentCode', '') ||
    _.get(shipmentAparData, 'FK_EquipmentCode', 'NA');

  const deliveryStop = await generateStopforConsole(
    shipmentHeaderData,
    referencesData,
    2,
    'SO',
    consigneeLocationId,
    finalConsigneeData,
    'consignee',
    '',
    shipmentAparConsoleData,
    descData
  );
  const timezone = await getTimezone({
    stopCity: _.get(finalShipperData, 'ShipCity', ''),
    state: _.get(finalShipperData, 'FK_ShipState', ''),
    country: _.get(finalShipperData, 'FK_ShipCountry', ''),
    address1: _.get(finalShipperData, 'ShipAddress1', ''),
  });
  const hazmat = await getHazmat({ shipmentAparConsoleData }); // Obtain hazmat information

  const payload = {
    __type: 'orders',
    company_id: 'TMS',
    allow_relay: true,
    bill_distance_um: 'MI',
    bol_received: false,
    dispatch_opt: true,
    collection_method: 'P',
    commodity: _.get(descData, '[0].Description', ''),
    customer_id: 'OTRODATX',
    blnum: _.get(shipmentAparData, 'ConsolNo', ''),
    entered_user_id: 'apiuser',
    equipment_type_id: mapEquipmentCodeToFkPowerbrokerCode(equipmentCode),
    order_type_id: mapOrderTypeToMcLeod(equipmentCode),
    excise_disable_update: false,
    excise_taxable: false,
    force_assign: true,
    hazmat,
    high_value: await getHighValue({ shipmentAparConsoleData }),
    hold_reason: 'NEW API',
    include_split_point: false,
    is_autorate_dist: false,
    is_container: false,
    is_dedicated: false,
    ltl: _.includes(['FT', 'HS'], _.get(shipmentHeaderData, '[0].serviceLevelId', '')),
    on_hold: false,
    ordered_date: await getCstTime({
      datetime: _.get(shipmentAparData, 'CreateDateTime', ''),
      timezone,
    }),
    rad_date: deliveryStop.sched_arrive_late, // Set rad_date to scheduled arrival late of SO stop
    ordered_method: 'M',
    order_value: await getHighValue({
      shipmentAparConsoleData,
      type: 'order_value',
    }),
    pallets_required: false,
    pieces: sumNumericValues(descData, 'Pieces'),
    preloaded: false,
    ready_to_bill: false,
    reply_transmitted: false,
    revenue_code_id: 'SPOT',
    round_trip: false,
    ship_status_to_edi: false,
    status: 'A',
    swap: true,
    teams_required: false,
    vessel: await getVesselForConsole({ shipmentAparConsoleData }),
    weight: sumNumericValues(descData, 'Weight'),
    weight_um: 'LB',
    order_mode: 'T',
    operational_status: 'CLIN',
    lock_miles: false,
    def_move_type: 'A',
    stops: [],
  };
  // Conditionally set commodity_id if hazmat is true
  if (hazmat) {
    _.set(payload, 'commodity_id', 'HAZMAT');
  }
  payload.stops.push(
    await generateStopforConsole(
      shipmentHeaderData,
      referencesData,
      1,
      'PU',
      shipperLocationId,
      finalShipperData,
      'shipper',
      userData,
      shipmentAparConsoleData,
      housebillData,
      descData
    ),
    deliveryStop
  );

  return payload;
}

async function mtPayload(
  shipmentApar,
  shipmentHeader,
  shipmentDesc,
  consolStopHeaders,
  customer,
  references,
  users
) {
  console.info('entered payload function');

  // Populate stops
  const stops = await populateStops(
    consolStopHeaders,
    references,
    users,
    shipmentHeader,
    shipmentDesc,
    shipmentApar
  );
  // Get the last stop
  const lastStop = stops[stops.length - 1];
  const timezone = await getTimezone({
    stopCity: _.get(consolStopHeaders, '[0].ConsolStopCity', ''),
    state: _.get(consolStopHeaders, '[0].FK_ConsolStopState', ''),
    country: _.get(consolStopHeaders, '[0].FK_ConsolStopCountry', ''),
    address1: _.get(consolStopHeaders, '[0].ConsolStopAddress1', ''),
  });
  const hazmat = await getHazmat({ shipmentAparConsoleData: shipmentApar }); // Obtain hazmat information

  const payload = {
    __type: 'orders',
    company_id: 'TMS',
    allow_relay: true,
    bill_distance_um: 'MI',
    bol_received: false,
    dispatch_opt: true,
    collection_method: 'P',
    commodity: _.get(shipmentDesc, '[0]Description', ''),
    customer_id: 'OTRODATX',
    blnum: _.get(consolStopHeaders, '[0]FK_ConsolNo', ''),
    entered_user_id: 'apiuser',
    equipment_type_id: mapEquipmentCodeToFkPowerbrokerCode(
      _.get(shipmentHeader, '[0]FK_EquipmentCode', '') ||
        _.get(shipmentApar, '[0]FK_EquipmentCode', '')
    ),
    order_type_id: mapOrderTypeToMcLeod(
      _.get(shipmentHeader, '[0]FK_EquipmentCode', '') ||
        _.get(shipmentApar, '[0]FK_EquipmentCode', '')
    ),
    excise_disable_update: false,
    excise_taxable: false,
    force_assign: true,
    hazmat,
    high_value: sumNumericValues(shipmentHeader, 'Insurance') > 100000,
    hold_reason: 'NEW API',
    include_split_point: false,
    is_autorate_dist: false,
    is_container: false,
    is_dedicated: false,
    ltl: _.includes(['FT', 'HS'], _.get(shipmentHeader, '[0]FK_ServiceLevelId')),
    on_hold: false,
    ordered_date: await getCstTime({
      datetime: _.get(shipmentApar, '[0]CreateDateTime', ''),
      timezone,
    }),
    rad_date: lastStop.sched_arrive_late,
    ordered_method: 'M',
    order_value: sumNumericValues(shipmentHeader, 'Insurance'),
    pallets_required: false,
    pieces: sumNumericValues(shipmentDesc, 'Pieces'),
    preloaded: false,
    ready_to_bill: false,
    reply_transmitted: false,
    revenue_code_id: 'SPOT',
    round_trip: false,
    ship_status_to_edi: false,
    status: 'A',
    swap: true,
    teams_required: false,
    vessel: _.get(customer, '[0]CustName'),
    weight: sumNumericValues(shipmentDesc, 'Weight'),
    weight_um: 'LB',
    order_mode: 'T',
    operational_status: 'CLIN',
    lock_miles: false,
    def_move_type: 'A',
    stops,
  };
  // Conditionally set commodity_id if hazmat is true
  if (hazmat) {
    _.set(payload, 'commodity_id', 'HAZMAT');
  }
  return payload;
}

module.exports = { nonConsolPayload, consolPayload, mtPayload };
