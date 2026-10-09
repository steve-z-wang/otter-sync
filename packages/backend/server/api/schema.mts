type CallKind = "mutation" | "query";
export type BackendDescriptor = {
  schema?: {
    enums?: { name: string; values?: string[] }[];
    models?: {
      name: string;
      version?: number;
      identity?: string[];
      fields?: { name: string; type: unknown }[];
    }[];
    actions?: {
      name: string;
      version: number;
      kind?: CallKind;
      inputs?: {
        kind: string;
        name: string;
        model?: string;
        cardinality?: string;
        list?: boolean;
        type?: unknown;
      }[];
      outputs?: { source: unknown }[];
      input?: {
        models?: {
          name: string;
          fields?: { name: string; type: unknown }[];
        }[];
      };
    }[];
  };
  models?: {
    name: string;
    version: number;
    fields?: { name: string; type: unknown }[];
  }[];
};
