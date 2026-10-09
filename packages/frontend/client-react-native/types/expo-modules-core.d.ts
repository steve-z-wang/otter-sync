// Minimal declaration so the root gate can typecheck this package without an Expo install.
// The integration app typechecks against the real expo-modules-core types.
declare module "expo-modules-core" {
  export function requireNativeModule<T>(name: string): T;
}
