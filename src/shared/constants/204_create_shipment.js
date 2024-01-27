'use strict';

const STATUSES = {
  PENDING: 'PENDING',
  READY: 'READY',
  SENT: 'SENT',
  FAILED: 'FAILED',
};

const TYPES = {
  CONSOLE: 'CONSOLE',
  NON_CONSOLE: 'NON_CONSOLE',
  MULTI_STOP: 'MULTI_STOP',
};

const CONSOLE_WISE_TABLES = {
  [TYPES.CONSOLE]: {
    tbl_ConfirmationCost: STATUSES.PENDING,
    tbl_Shipper: STATUSES.PENDING,
    tbl_Consignee: STATUSES.PENDING,
    tbl_ShipmentDesc: STATUSES.PENDING,
    tbl_ShipmentHeader: STATUSES.PENDING,
    tbl_Customers: STATUSES.PENDING,
    tbl_TimeZoneMaster: STATUSES.READY,
    tbl_TrackingNotes: STATUSES.PENDING,
  },
  [TYPES.NON_CONSOLE]: {
    tbl_ConfirmationCost: STATUSES.PENDING,
    tbl_Shipper: STATUSES.PENDING,
    tbl_Consignee: STATUSES.PENDING,
    tbl_ShipmentDesc: STATUSES.PENDING,
    tbl_ShipmentHeader: STATUSES.PENDING,
    tbl_Customers: STATUSES.PENDING,
    tbl_TimeZoneMaster: STATUSES.READY,
    tbl_TrackingNotes: STATUSES.PENDING,
  },
  [TYPES.MULTI_STOP]: {
    tbl_ShipmentHeader: STATUSES.PENDING,
    tbl_ConsolStopItems: STATUSES.PENDING,
    tbl_ConsolStopHeaders: STATUSES.PENDING,
    tbl_Users: STATUSES.PENDING,
    tbl_TimeZoneMaster: STATUSES.READY,
    tbl_Customers: STATUSES.PENDING,
    tbl_ShipmentDesc: STATUSES.PENDING,
  },
};

const VENDOR = 'LIVELOGI';

module.exports = { STATUSES, TYPES, CONSOLE_WISE_TABLES, VENDOR };
