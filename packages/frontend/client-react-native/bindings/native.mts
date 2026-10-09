import { requireNativeModule } from "expo-modules-core";
import type { NativeCarrier } from "../../client-js/bindings/bridge.mts";

export const native = requireNativeModule<{
  runtimeOpen(request: string): string;
  runtimeSubmit(runtimeId: string, message: string): void;
  runtimeDrain(runtimeId: string): string;
  runtimeDetach(runtimeId: string): void;
  addListener(
    event: "axtonWake",
    listener: (event: { runtimeId: string }) => void,
  ): { remove(): void };
  databasePath(name: string): Promise<string>;
}>("AxtonNative");

/**
 * The shared JS Bridge's carrier over the Expo module. The native side posts
 * `axtonWake` on the main queue whenever a runtime has events; one listener,
 * registered on first open, routes it to that runtime's Bridge.
 */
const wakes = new Map<string, () => void>();
let listening = false;
export const carrier: NativeCarrier = {
  runtimeOpen(request, wake) {
    if (!listening) {
      native.addListener("axtonWake", ({ runtimeId }) =>
        wakes.get(runtimeId)?.(),
      );
      listening = true;
    }
    const runtimeId = native.runtimeOpen(request);
    wakes.set(runtimeId, () => wake(runtimeId));
    return runtimeId;
  },
  runtimeSubmit: (runtimeId, message) =>
    native.runtimeSubmit(runtimeId, message),
  runtimeDrain: (runtimeId) => native.runtimeDrain(runtimeId),
  runtimeDetach(runtimeId) {
    wakes.delete(runtimeId);
    native.runtimeDetach(runtimeId);
  },
};
