'use strict';
const _ = require('lodash');
const { generateStop, formatTimestamp, getVesselForConsole } = require('./helper');

async function nonConsolPayload({
  shipmentHeader,
  shipmentDesc,
  referencesData,
  shipperLocationId,
  consigneeLocationId,
  finalShipperData,
  finalConsigneeData,
  customersData,
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
    equipment_type_id: _.get(shipmentHeader, 'FK_EquipmentCode', ''),
    excise_disable_update: false,
    excise_taxable: false,
    force_assign: true,
    hazmat,
    high_value: _.get(shipmentHeader, 'Insurance', 0) > 100000, // TODO: validate this filed
    include_split_point: false,
    is_autorate_dist: false,
    is_container: false,
    is_dedicated: false,
    ltl: _.includes(['FT', 'HS'], _.get(shipmentHeader, 'FK_ServiceLevelId')),
    on_hold: false,
    ordered_date: await formatTimestamp(_.get(shipmentHeader, 'OrderDate', '')),
    ordered_method: 'M',
    pallets_required: false,
    pieces: _.get(shipmentDesc, 'Pieces', 0),
    preloaded: false,
    ready_to_bill: false,
    reply_transmitted: false,
    // revenue_code_id: 'PRI',
    round_trip: false,
    ship_status_to_edi: false,
    status: 'A',
    swap: true,
    teams_required: false,
    vessel: _.get(customersData, 'CustName', ''),
    weight: _.get(shipmentDesc, 'Weight', 0),
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
      finalConsigneeData
    ),
    await generateStop(
      shipmentHeader,
      referencesData,
      2,
      'SO',
      consigneeLocationId,
      finalShipperData
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
    blnum: _.get(shipmentAparData, 'ConsolNo', ''),
    entered_user_id: 'apiuser',
    equipment_type_id: '',
    excise_disable_update: false,
    excise_taxable: false,
    force_assign: true,
    hazmat,
    high_value: _.get(shipmentHeader, 'Insurance', 0) > 100000,
    include_split_point: false,
    is_autorate_dist: false,
    is_container: false,
    is_dedicated: false,
    ltl: _.includes(['FT', 'HS'], _.get(shipmentHeader, 'FK_ServiceLevelId')),
    on_hold: false,
    ordered_date: formatTimestamp(_.get(shipmentHeader, 'OrderDate', '')),
    ordered_method: 'M',
    pallets_required: false,
    pieces: _.get(shipmentDesc, 'Pieces', 0),
    preloaded: false,
    ready_to_bill: false,
    reply_transmitted: false,
    // revenue_code_id: 'PRI',
    round_trip: false,
    ship_status_to_edi: false,
    status: 'A',
    swap: true,
    teams_required: false,
    vessel: await getVesselForConsole(),
    weight: _.get(shipmentDesc, 'Weight', 0),
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
      finalConsigneeData,
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
      finalShipperData,
      confirmationCostData,
      'consignee'
    )
  );

  return payload;
}

module.exports = { nonConsolPayload, consolPayload };
