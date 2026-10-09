export type RecordValue = Record<string, unknown>;
export type QuerySpec = {
  filter?: RecordValue;
  orderBy?: { field: string; direction: "ascending" | "descending" }[];
  limit?: number;
};
export function strictJson(value: unknown): string {
  return JSON.stringify(value, (_key, item) => {
    if (typeof item === "number" && !Number.isFinite(item))
      throw Error("JSON numbers must be finite");
    return item;
  });
}
