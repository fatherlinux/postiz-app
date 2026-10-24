// Limits find-slot ("next free posting time") to chosen weekdays.
//
// POSTIZ_SLOT_WEEKDAYS: days a slot may fall on, as ranges and/or a comma
// list with 0 = Sunday ("1-5" = Monday to Friday). Unset or unparseable
// means every day, which is upstream behaviour.
// POSTIZ_SLOT_TIMEZONE: IANA zone the weekday is judged in (default UTC).
// Posting times are UTC minutes, so an evening window in the Americas lands
// on the next UTC day; judging in the poster's zone keeps it on the right day.

const WEEKDAY_NAMES = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];

export function parseSlotWeekdays(spec?: string): Set<number> | null {
  if (!spec || !spec.trim()) {
    return null;
  }
  const days = new Set<number>();
  for (const part of spec.split(',')) {
    const [start, end = start] = part.split('-').map((s) => Number(s.trim()));
    if (
      !Number.isInteger(start) ||
      !Number.isInteger(end) ||
      start < 0 ||
      end > 6 ||
      start > end
    ) {
      continue;
    }
    for (let day = start; day <= end; day++) {
      days.add(day);
    }
  }
  return days.size ? days : null;
}

function weekdayFormatter(timeZone: string): Intl.DateTimeFormat {
  try {
    return new Intl.DateTimeFormat('en-US', { weekday: 'short', timeZone });
  } catch {
    console.warn(
      `[posts] POSTIZ_SLOT_TIMEZONE '${timeZone}' is not a valid zone; using UTC`
    );
    return new Intl.DateTimeFormat('en-US', {
      weekday: 'short',
      timeZone: 'UTC',
    });
  }
}

export function slotDayFilter(
  spec = process.env.POSTIZ_SLOT_WEEKDAYS,
  timeZone = process.env.POSTIZ_SLOT_TIMEZONE || 'UTC'
): (when: Date) => boolean {
  const days = parseSlotWeekdays(spec);
  if (!days) {
    return () => true;
  }
  const format = weekdayFormatter(timeZone);
  return (when) => days.has(WEEKDAY_NAMES.indexOf(format.format(when)));
}
