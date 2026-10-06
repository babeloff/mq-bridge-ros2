import { describe, expect, test } from "vitest";
import { nextHashForTab, pickDefaultTab, resolveTabFromHash } from "../../ui/src/lib/routing";

describe("routing", () => {
  test("resolves supported tabs from hash", () => {
    expect(resolveTabFromHash("#publishers")).toBe("publishers");
    expect(resolveTabFromHash("#publishers:2")).toBe("publishers");
    expect(resolveTabFromHash("#consumers:1")).toBe("consumers");
    expect(resolveTabFromHash("#config")).toBe("config");
    expect(resolveTabFromHash("#status")).toBe("status");
    expect(resolveTabFromHash("#else")).toBeNull();
  });

  test("preserves detail hash for active tab", () => {
    expect(nextHashForTab("#publishers:5", "publishers")).toBe("#publishers:5");
    expect(nextHashForTab("#publishers:1", "consumers", 3)).toBe("#consumers:3");
    expect(nextHashForTab("#publishers:1", "consumers")).toBe("#consumers");
    expect(nextHashForTab("#publishers:1", "status", 2)).toBe("#status");
  });

  test("picks default tab in priority order", () => {
    expect(pickDefaultTab("#consumers:1", [], "publishers")).toBe("consumers");
    expect(pickDefaultTab("", ["live_route"], "publishers")).toBe("publishers");
    expect(pickDefaultTab("", [], "consumers")).toBe("consumers");
    expect(pickDefaultTab("", [], "config")).toBe("config");
    expect(pickDefaultTab("", [], "unknown")).toBe("publishers");
  });
});
