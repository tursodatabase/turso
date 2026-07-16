// Provider-switching connect helper, modeled on
// testing/conformance/javascript. PROVIDER selects the implementation
// under test:
//
//   pglite - @electric-sql/pglite, the reference implementation
//   turso  - @tursodatabase/pg-experimental (postgres/js), whose one
//            exported class is named PGlite (see postgres/PGLITE.md)
//
// Every provider must expose the PGlite API: constructor(dataDir?, options?),
// query(), sql``, exec(), transaction(), close(), ready/closed/waitReady.
export const connect = async (dataDir, options = {}) => {
  const provider = process.env.PROVIDER || "pglite";
  if (provider === "pglite") {
    const { PGlite } = await import("@electric-sql/pglite");
    const db = new PGlite(dataDir, options);
    await db.waitReady;
    return { db, provider };
  }
  if (provider === "turso") {
    const { PGlite } = await import("@tursodatabase/pg-experimental");
    const db = new PGlite(dataDir, options);
    await db.waitReady;
    return { db, provider };
  }
  throw new Error("Unknown provider: " + provider);
};
