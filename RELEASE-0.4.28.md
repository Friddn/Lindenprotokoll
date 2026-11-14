# Lindenprotokoll 0.4.28 – Electricity statistics interpolation

## Heat-pump and household comparison

- Daily heat-pump electricity now falls back to integration of stored 15-minute screenshot-curve power estimates.
- Power in kW is integrated over elapsed time to energy in kWh; adjacent samples are connected only across gaps of 30 minutes or less.
- Confirmed screenshot daily totals take precedence, followed by measured hourly data, then graph-derived estimates.
- The household total series now uses the existing absolute electricity-meter deltas. Each meter interval's kWh delta is distributed as an explicitly labelled average per day across that interval.
- The graph and KPIs use the currently selected Strom date range.
- The chart no longer requires a separate non-heat-pump daily record in order to show household electricity consumption.
- Labels distinguish measured/confirmed heat-pump data, estimated curve data and interval-average household consumption.

## Validation

- Python source compilation and archive integrity checked.
- Live database and Home Assistant runtime validation still required. Back up `/data` before updating.
