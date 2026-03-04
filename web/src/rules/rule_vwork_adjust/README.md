# rule_vwork_adjust

Add 1 hour to a datetime. Used for Step Adjusted in Vwork/Inspect UI and for batch execution over DB rows.

## Logic

- Add 1 to the hour.
- If hour >= 24: hour becomes 0, day +1.
- Month/year rollover when day exceeds last day of month.

Example: `23:50` → `00:50` next day.

No timezone conversion. Uses raw numbers only.

## Usage

```ts
import { addOneHour } from '@/rules/rule_vwork_adjust';

// DB format (YYYY-MM-DD HH:mm:ss)
addOneHour('2026-02-19 12:50:54', 'db');   // → '2026-02-19 13:50:54'
addOneHour('2026-02-19 23:50:54', 'db');   // → '2026-02-20 00:50:54'

// Display format (DD/MM/YY HH:mm:ss)
addOneHour('19/02/26 12:50:54', 'display'); // → '19/02/26 13:50:54'
addOneHour('19/02/26 23:50:54', 'display'); // → '20/02/26 00:50:54'
```

## Batch DB execution

Import and call over rows when applying rules in bulk.
