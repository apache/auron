# Commit 3: Temporal Wrappers

**Commit**: `[AURON #1851] Add temporal Arrow ColumnVector wrappers`

## What This Commit Does

Adds 3 wrappers for date, time, and timestamp types. Time and timestamp require precision conversion (microseconds ↔ milliseconds) to match the writer's normalization.

## Files to Review (3)

| File | Arrow Vector | Flink Interface | Conversion |
|------|-------------|-----------------|------------|
| `ArrowDateColumnVector` | `DateDayVector` | `IntColumnVector` | Direct passthrough (epoch days as int) |
| `ArrowTimeColumnVector` | `TimeMicroVector` | `IntColumnVector` | `(int)(vector.get(i) / 1000)` — micros → millis |
| `ArrowTimestampColumnVector` | `TimeStampVector` | `TimestampColumnVector` | Splits micros into millis + nanoOfMillisecond |

## What to Look For

- **Time conversion**: Writer (PR #1930) normalizes all TIME to microseconds. Reader reverses: `micros / 1000 = millis`. Max time value (86,399,999) fits in int.
- **Timestamp conversion** (key review point):
  ```
  millis = micros / 1000
  nanoOfMillisecond = ((int)(micros % 1000)) * 1000
  ```
  Explicit parentheses added for clarity. Comment documents pre-epoch rounding behavior.
- **TimeStampVector**: Uses the abstract parent type to handle both `TimeStampMicroVector` (TIMESTAMP) and `TimeStampMicroTZVector` (TIMESTAMP_LTZ) with a single wrapper.
