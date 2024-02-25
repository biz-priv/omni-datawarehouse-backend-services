'use strict';
const _ = require('lodash');
const {
  generateStop,
  getVesselForConsole,
  // getWeightPiecesForConsole,
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
  getFormattedDateTime,
  getTimezone,
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
  let hazmat = _.get(shipmentDesc, 'Hazmat', false);
  // Check if Hazmat is equal to 'Y'
  if (hazmat === 'Y') {
    // Set hazmat to true if it is 'Y'
    hazmat = true;
  } else {
    // Set hazmat to false for null or other values
    hazmat = false;
  }
  const deliveryStop = await generateStop(
    shipmentHeader,
    referencesData,
    2,
    'SO',
    consigneeLocationId,
    finalConsigneeData,
    'consignee',
    shipmentAparData
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
    commodity_id: 'TVS',
    customer_id: 'OMNICOT8',
    blnum: _.get(shipmentHeader, 'Housebill', ''),
    entered_user_id: 'apiuser',
    equipment_type_id: mapEquipmentCodeToFkPowerbrokerCode(
      _.get(shipmentHeader, 'FK_EquipmentCode', '')
    ),
    order_type_id: mapOrderTypeToMcLeod(_.get(shipmentHeader, 'FK_EquipmentCode', '')),
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
    ordered_date: getFormattedDateTime(_.get(shipmentHeader, 'OrderDate', ''), timezone),
    rad_date: deliveryStop.sched_arrive_late,
    ordered_method: 'M',
    order_value: _.get(shipmentHeader, 'Insurance', 0),
    pallets_required: false,
    pieces: sumNumericValues(shipmentDesc, 'Pieces'),
    preloaded: false,
    ready_to_bill: false,
    reply_transmitted: false,
    revenue_code_id: 'PRI',
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
      shipmentAparData
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
  const shipmentAparConsoleData = await getAparDataByConsole({ shipmentAparData });
  const shipmentHeaderData = await getShipmentHeaderData({ shipmentAparConsoleData });
  console.info('🚀 ~ file: payloads.js:143 ~ shipmentHeaderData:', shipmentHeaderData);
  const housebillData = await getHousebillData({ shipmentAparConsoleData });
  const descData = await descDataForConsole({ shipmentAparConsoleData });
  // Convert housebillData to array of objects
  const housebillObjects = housebillData.map((array) => array.map((item) => ({ ...item })));
  console.info('🚀 ~ file: payloads.js:147 ~ housebillObjects:', housebillObjects);

  // Convert descData to array of objects
  const descObjects = descData.map((array) => array.map((item) => ({ ...item })));
  console.info('🚀 ~ file: payloads.js:151 ~ descObjects:', descObjects);
  const deliveryStop = await generateStopforConsole(
    shipmentHeaderData,
    referencesData,
    2,
    'SO',
    consigneeLocationId,
    finalConsigneeData,
    'consignee',
    '',
    shipmentAparConsoleData
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
    commodity_id: 'TVS',
    customer_id: 'OMNICOT8',
    blnum: _.get(shipmentAparData, 'ConsolNo', ''),
    entered_user_id: 'apiuser',
    equipment_type_id: mapEquipmentCodeToFkPowerbrokerCode(
      _.get(shipmentHeaderData, '[0].equipmentCode', '')
    ),
    order_type_id: mapOrderTypeToMcLeod(_.get(shipmentHeaderData, '[0].equipmentCode', '')),
    excise_disable_update: false,
    excise_taxable: false,
    force_assign: true,
    hazmat: await getHazmat({ shipmentAparConsoleData }),
    high_value: await getHighValue({ shipmentAparConsoleData }),
    hold_reason: 'NEW API',
    include_split_point: false,
    is_autorate_dist: false,
    is_container: false,
    is_dedicated: false,
    ltl: _.includes(['FT', 'HS'], _.get(shipmentHeaderData, '[0].serviceLevelId', '')),
    on_hold: false,
    ordered_date: getFormattedDateTime(_.get(shipmentAparData, 'CreateDateTime', ''), timezone),
    rad_date: deliveryStop.sched_arrive_late, // Set rad_date to scheduled arrival late of SO stop
    ordered_method: 'M',
    order_value: await getHighValue({ shipmentAparConsoleData, type: 'order_value' }),
    pallets_required: false,
    pieces: sumNumericValues(descObjects, 'Pieces'),
    preloaded: false,
    ready_to_bill: false,
    reply_transmitted: false,
    revenue_code_id: 'PRI',
    round_trip: false,
    ship_status_to_edi: false,
    status: 'A',
    swap: true,
    teams_required: false,
    vessel: await getVesselForConsole({ shipmentAparConsoleData }),
    weight: sumNumericValues(descObjects, 'Weight'),
    weight_um: 'LB',
    order_mode: 'T',
    operational_status: 'CLIN',
    lock_miles: false,
    def_move_type: 'A',
    stops: [],
  };

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
      housebillObjects,
      descObjects
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
  let hazmat = _.get(shipmentDesc, '[0]Hazmat', false);

  // Check if Hazmat is equal to 'Y'
  if (hazmat === 'Y') {
    // Set hazmat to true if it is 'Y'
    hazmat = true;
  } else {
    // Set hazmat to false for null or other values
    hazmat = false;
  }

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

  const payload = {
    __type: 'orders',
    company_id: 'TMS',
    allow_relay: true,
    bill_distance_um: 'MI',
    bol_received: false,
    dispatch_opt: true,
    collection_method: 'P',
    commodity_id: 'TVS',
    customer_id: 'OMNICOT8',
    blnum: _.get(consolStopHeaders, '[0]FK_ConsolNo', ''),
    entered_user_id: 'apiuser',
    equipment_type_id: mapEquipmentCodeToFkPowerbrokerCode(
      _.get(shipmentHeader, '[0]FK_EquipmentCode', '')
    ),
    order_type_id: mapOrderTypeToMcLeod(_.get(shipmentHeader, '[0]FK_EquipmentCode', '')),
    excise_disable_update: false,
    excise_taxable: false,
    force_assign: true,
    hazmat,
    high_value: _.get(shipmentHeader, '[0]Insurance', 0) > 100000,
    hold_reason: 'NEW API',
    include_split_point: false,
    is_autorate_dist: false,
    is_container: false,
    is_dedicated: false,
    ltl: _.includes(['FT', 'HS'], _.get(shipmentHeader, '[0]FK_ServiceLevelId')),
    on_hold: false,
    ordered_date: getFormattedDateTime(_.get(shipmentApar, '[0]CreateDateTime', '')),
    rad_date: lastStop.sched_arrive_late,
    ordered_method: 'M',
    order_value: sumNumericValues(shipmentHeader, 'Insurance'),
    pallets_required: false,
    pieces: sumNumericValues(shipmentDesc, 'Pieces'),
    preloaded: false,
    ready_to_bill: false,
    reply_transmitted: false,
    revenue_code_id: 'PRI',
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

  return payload;
}

module.exports = { nonConsolPayload, consolPayload, mtPayload };
