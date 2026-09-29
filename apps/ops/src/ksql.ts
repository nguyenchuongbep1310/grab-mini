// Pull queries against ksqlDB's REST API (see ksql/statements.sql for the tables).

export type Row = Record<string, string | number | null>;

// Ask for the delimited format explicitly: without an Accept header the server
// picks one itself, and a browser (Accept: */*) gets a different format than curl.
// Response: one JSON object with columnNames, then one JSON array per row.
const DELIMITED = "application/vnd.ksqlapi.delimited.v1";

export async function pull(query: string): Promise<Row[]> {
  const res = await fetch("/ksql-api/query", {
    method: "POST",
    headers: { "Content-Type": "application/vnd.ksql.v1+json", Accept: DELIMITED },
    body: JSON.stringify({ ksql: query, streamsProperties: {} }),
  });
  const text = await res.text();

  if (!res.ok) {
    let message = `ksqlDB returned ${res.status}`;
    try {
      message = JSON.parse(text).message ?? message;
    } catch {
      // not JSON: keep the status message
    }
    throw new Error(message);
  }

  const [header, ...rows] = text.split("\n").filter((line) => line.trim() !== "");
  const names: string[] = JSON.parse(header).columnNames;
  return rows.map((line) => Object.fromEntries((JSON.parse(line) as Array<string | number | null>).map((value, i) => [names[i], value])));
}
