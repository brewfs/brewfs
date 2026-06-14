export function labelsToText(labels: Record<string, string>): string {
  return Object.entries(labels)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => `${key}=${value}`)
    .join('\n');
}

export function labelsFromText(value: string): Record<string, string> {
  const labels: Record<string, string> = {};
  for (const rawLine of value.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line) continue;

    const separator = line.indexOf('=');
    if (separator <= 0) {
      throw new Error(`invalid label: ${line}`);
    }

    const key = line.slice(0, separator).trim();
    const labelValue = line.slice(separator + 1).trim();
    if (!key) {
      throw new Error(`invalid label: ${line}`);
    }
    labels[key] = labelValue;
  }
  return labels;
}
