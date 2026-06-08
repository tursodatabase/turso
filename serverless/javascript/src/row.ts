export function createExpandedRow(row: any[], columns: string[]): any {
  const expanded: any = {};

  columns.forEach((column, index) => {
    expanded[column] = row[index];
    Object.defineProperty(expanded, String(index), {
      value: row[index],
      enumerable: false,
      writable: true,
      configurable: true,
    });
  });

  return expanded;
}
