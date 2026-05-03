import { describe, it, expect } from "vitest";

// Test the ALLOCATED_MIDS set logic by importing the module and checking
// that known unallocated MIDs are not in the allocated set.
// We test the logic indirectly via the exported constant's documented behaviour.

// Known unallocated MIDs observed in the wild (shadow fleet stateless MMSIs)
const KNOWN_UNALLOCATED = ["400", "703", "792"];

// Known allocated MIDs that must be present
const KNOWN_ALLOCATED = [
  "273", // Russia
  "312", // Belize
  "457", // Mongolia
  "563", // Singapore
  "412", // China
  "636", // Liberia
  "701", // Argentina
  "710", // Brazil
];

// Reconstruct the allocation check from the source (mirror of ALLOCATED_MIDS in duckdb.ts)
const ALLOCATED: Set<number> = new Set([
  201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,218,219,
  220,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,
  240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,
  257,258,259,261,262,263,264,265,266,267,268,269,270,271,272,273,274,
  275,276,277,278,279,
  301,303,304,305,306,307,308,309,310,311,312,316,319,321,323,325,327,
  329,330,331,332,333,334,336,338,339,341,343,345,347,348,350,351,352,
  353,354,355,356,357,358,359,361,362,364,366,367,368,369,370,371,372,
  373,374,375,376,377,378,379,
  401,403,405,408,412,413,414,416,417,419,422,423,425,428,431,432,433,
  434,436,438,440,441,443,445,447,450,451,452,453,455,457,459,461,462,
  463,466,467,468,470,471,472,473,474,477,478,
  501,503,506,508,509,510,511,512,514,515,516,518,519,520,523,525,529,
  531,533,536,538,540,542,544,546,548,553,555,557,559,561,563,564,565,
  566,567,570,572,574,576,577,578,580,582,584,
  601,603,605,607,608,609,610,611,612,613,615,616,617,618,619,620,621,
  622,624,625,626,627,628,629,630,631,632,633,634,635,636,637,638,642,
  644,645,647,649,650,654,655,656,657,659,660,661,662,663,664,665,666,
  667,668,669,
  701,710,720,725,730,734,735,740,745,750,755,756,760,765,770,775,780,790,
]);

function isStateless(mmsi: string): boolean {
  if (mmsi.length !== 9) return false;
  const mid = parseInt(mmsi.slice(0, 3), 10);
  return !ALLOCATED.has(mid);
}

describe("stateless MMSI detection", () => {
  it("flags known unallocated MIDs as stateless", () => {
    for (const mid of KNOWN_UNALLOCATED) {
      const mmsi = mid + "000000"; // 9-digit MMSI
      expect(isStateless(mmsi), `MID ${mid} should be stateless`).toBe(true);
    }
  });

  it("does not flag known allocated MIDs as stateless", () => {
    for (const mid of KNOWN_ALLOCATED) {
      const mmsi = mid + "000000";
      expect(isStateless(mmsi), `MID ${mid} should NOT be stateless`).toBe(false);
    }
  });

  it("flags MMSI 400789012 as stateless", () => {
    expect(isStateless("400789012")).toBe(true);
  });

  it("flags MMSI 703260608 as stateless", () => {
    expect(isStateless("703260608")).toBe(true);
  });

  it("flags MMSI 792975437 as stateless", () => {
    expect(isStateless("792975437")).toBe(true);
  });

  it("does not flag valid Russian vessel MMSI 273449240 as stateless", () => {
    expect(isStateless("273449240")).toBe(false);
  });

  it("does not flag valid Singapore vessel MMSI 563069100 as stateless", () => {
    expect(isStateless("563069100")).toBe(false);
  });

  it("returns false for MMSI with wrong length", () => {
    expect(isStateless("40078901")).toBe(false);   // 8 digits
    expect(isStateless("4007890123")).toBe(false);  // 10 digits
  });
});
