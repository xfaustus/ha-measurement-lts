# Measurement LTS Backfill

A Home Assistant custom integration that rebuilds missing hourly statistics from state history retained by Recorder. Use it after adding `state_class: measurement` to an existing sensor, or to reconstruct gaps in a sensor's recorded statistics.

## Features

- Search for recorded numerical sensors with `state_class: measurement`. Select one sensor or several.
- Find candidates automatically. The search finds sensors with retained state history and no hourly statistics, or with history older than their first hourly statistic.
- Preview additions, matching hours, discrepancies, and gaps for each sensor before making changes.
- Rebuild hourly mean, minimum, and maximum using Recorder's five-minute aggregation approach. Import missing hours by default. Replacing different hourly values requires a separate opt-in.
- Optionally rebuild missing five-minute statistics within the applicable Recorder retention window. Existing five-minute rows are preserved by the import.
- Optionally purge old detailed history and five-minute statistics for the selected sensors using the same retention period. Hourly statistics are preserved by purge.
- Follow the current sensor and progress during the preview and import. Cancel before commit to roll back the operation and prevent the associated purge from being queued.

## Installation

### HACS

1. In HACS, open **Custom repositories** from the menu.
2. Add `https://github.com/xfaustus/ha-measurement-lts` as an **Integration**.
3. Install **Measurement LTS Backfill** and restart Home Assistant.
4. Go to **Settings ? Devices & services ? Add integration**, then select **Measurement LTS Backfill**.

### Manual installation

Copy `custom_components/measurement_lts` into your Home Assistant configuration folder under `custom_components/`. Restart Home Assistant and add the integration. Restart Home Assistant Core after updating the integration so that the new Python code is loaded.

## Usage

1. Open the integration's **Configure** dialog. Choose sensors manually or run the search for sensors without historical LTS.
2. Set the date range. The default start is the earliest retained state among the selected sensors. Dates use the Home Assistant time zone and statistics are grouped into UTC hours.
3. Review the results per sensor. A newly enabled sensor may need its first native five-minute statistics cycle before its Recorder metadata is available. Sensors with missing or incompatible metadata are listed separately.
4. Confirm to write the missing statistics. Enable replacement only if you want to update the differing hourly statistics shown in the preview. The integration checks the records again before it writes.
5. View the backfilled statistics in a **Statistics graph** card. Use the **Hour** period for long-term statistics or **5 minutes** for short-term statistics still within retention.

### Purge and cancellation

Purge is optional. It uses the chosen number of days for both detailed history and five-minute statistics for the selected sensors, including data outside the preview date range. The purge of detailed history is queued only after the statistics transaction commits. The global Recorder retention still applies. If you want to verify the backfill before deleting any detailed history, first run it with purge disabled.

Close the progress dialog to request cancellation. To wait for confirmation while leaving the progress dialog open, run the administrator action `measurement_lts.cancel` from **Developer tools ? Actions** in another tab. The `cancelled` result confirms that the current operation was stopped and that any uncommitted changes were rolled back. `too_late` means the commit has already begun. No new batch can start while the previous one is still active.

## Limitations

- Recorder must still have the historical states. The integration cannot recover source readings that have already been purged.
- Supports sensors with `state_class: measurement` and compatible Recorder metadata. The current implementation requires matching units for the historical samples it aggregates.
- Five-minute statistics follow Recorder's short-term retention. Home Assistant's native long-term statistics are hourly.
- Uses internal Recorder models and tasks. These can change between Home Assistant releases. The development target is Home Assistant Core 2026.10.2.

## Tests

Run the aggregation and cancellation tests from the repository root.

```sh
python -m unittest discover -s tests
```
