'use strict';
const _ = require('lodash');
const {
  generateStop,
  formatTimestamp,
  getVesselForConsole,
  getWeightForConsole,
  getHazmat,
  getHighValue,
  getAparDataByConsole,
  sumNumericValues,
  populateStops,
  mapEquipmentCodeToFkPowerbrokerCode,
  getPieces,
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
    excise_disable_update: false,
    excise_taxable: false,
    force_assign: true,
    hazmat,
    high_value: _.get(shipmentHeader, 'Insurance', 0) > 100000, // TODO: validate this filed
    hold_reason: 'NEW API',
    include_split_point: false,
    is_autorate_dist: false,
    is_container: false,
    is_dedicated: false,
    ltl: _.includes(['FT', 'HS'], _.get(shipmentHeader, 'FK_ServiceLevelId')),
    on_hold: true,
    ordered_date: formatTimestamp(_.get(shipmentHeader, 'OrderDate', '')),
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
      '',
      'shipper',
      userData
    ),
    await generateStop(
      shipmentHeader,
      referencesData,
      2,
      'SO',
      consigneeLocationId,
      finalConsigneeData
    )
  );

  return payload;
}

async function consolPayload({
  shipmentHeader,
  shipmentDesc,
  referencesData,
  shipperLocationId,
  consigneeLocationId,
  finalShipperData,
  finalConsigneeData,
  // customersData,
  shipmentAparData,
  confirmationCostData,
  userData,
}) {
  const shipmentAparConsoleData = await getAparDataByConsole({ shipmentAparData });
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
    equipment_type_id: '',
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
    ltl: _.includes(['FT', 'HS'], _.get(shipmentHeader, 'FK_ServiceLevelId')),
    on_hold: true,
    ordered_date: formatTimestamp(_.get(shipmentHeader, 'OrderDate', '')),
    ordered_method: 'M',
    order_value: await getHighValue({ shipmentAparConsoleData, type: 'order_value' }),
    pallets_required: false,
    pieces: getPieces({ shipmentDesc }),
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
    weight: await getWeightForConsole({ shipmentAparConsoleData }),
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
      confirmationCostData,
      'shipper',
      userData
    ),
    await generateStop(
      shipmentHeader,
      referencesData,
      2,
      'SO',
      consigneeLocationId,
      finalConsigneeData,
      confirmationCostData,
      'consignee'
    )
  );

  return payload;
}

async function mtPayload(
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
    on_hold: true,
    ordered_date: formatTimestamp(_.get(shipmentHeader, '[0]OrderDate', '')),
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
    stops: await populateStops(consolStopHeaders, references, users),
  };

  return payload;
}

module.exports = { nonConsolPayload, consolPayload, mtPayload };
