import { describe, expect, it } from "vitest";
import type { SessionsListResult } from "../../api/types.ts";
import { reconcileSessionChanged } from "./reconcile.ts";

function buildResult(sessions: SessionsListResult["sessions"]): SessionsListResult {
  return {
    ts: 1,
    path: "store",
    count: sessions.length,
    defaults: { modelProvider: null, model: null, contextTokens: null },
    sessions,
  };
}

describe("reconcileSessionChanged", () => {
  it("drops a cleared category from the merged row", () => {
    const key = "agent:main:discord:channel:1";
    const result = buildResult([
      { key, kind: "group", updatedAt: 1, sessionId: "s1", category: "Research" },
    ]);
    const next = reconcileSessionChanged(result, {
      sessionKey: key,
      key,
      kind: "group",
      updatedAt: 2,
      sessionId: "s1",
      category: null,
    });
    expect(next.applied).toBe(true);
    expect(next.row?.category).toBeUndefined();
  });

  it("applies an updated category to the merged row", () => {
    const key = "agent:main:discord:channel:1";
    const result = buildResult([{ key, kind: "group", updatedAt: 1, sessionId: "s1" }]);
    const next = reconcileSessionChanged(result, {
      sessionKey: key,
      key,
      kind: "group",
      updatedAt: 2,
      sessionId: "s1",
      category: "Research",
    });
    expect(next.applied).toBe(true);
    expect(next.row?.category).toBe("Research");
  });
});
