import { createRequire } from "node:module";
import type { NativeCarrier } from "./bridge.mts";

export const native = createRequire(import.meta.url)(
  "@axtonjs/native",
) as NativeCarrier;
