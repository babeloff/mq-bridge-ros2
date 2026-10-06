import type { MainTab } from "./runtime-status";

export function resolveTabFromHash(hash: string): MainTab | null {
  if (!hash) return null;
  if (hash === "#publishers" || hash.startsWith("#publishers:")) return "publishers";
  if (hash === "#consumers" || hash.startsWith("#consumers:")) return "consumers";
  if (hash === "#config") return "config";
  if (hash === "#status") return "status";
  return null;
}

export function nextHashForTab(currentHash: string, tab: MainTab, rememberedIndex?: number): string {
  return currentHash === `#${tab}` || currentHash.startsWith(`#${tab}:`)
    ? currentHash
    : tab === "config" || tab === "status"
      ? `#${tab}`
      : typeof rememberedIndex === "number" && Number.isInteger(rememberedIndex) && rememberedIndex >= 0
        ? `#${tab}:${rememberedIndex}`
        : `#${tab}`;
}

export function pickDefaultTab(
  hash: string,
  activeRoutes: string[],
  configuredDefault: unknown,
): MainTab {
  const hashTab = resolveTabFromHash(hash);
  if (hashTab) return hashTab;
  if (
    configuredDefault === "publishers" ||
    configuredDefault === "consumers" ||
    configuredDefault === "config"
  ) {
    return configuredDefault;
  }
  return "publishers";
}
