export function createExpandedRow(row: any[], columns: string[]): any {
  const expanded: any = {};

  columns.forEach((column, index) => {
    expanded[column] = row[index];
  });

  return expanded;
}
