import { connect } from "@tursodatabase/serverless";

const client = connect({
    url: process.env.TURSO_DATABASE_URL,
    authToken: process.env.TURSO_AUTH_TOKEN,
});

await client.batch(
    [
        "CREATE TABLE IF NOT EXISTS users (email TEXT)",
        "INSERT INTO users VALUES ('first@example.com')",
        "INSERT INTO users VALUES ('second@example.com')",
        "INSERT INTO users VALUES ('third@example.com')",
    ],
    "write",
);

// Using execute method
const result = await client.execute("SELECT * FROM users");
console.log("Users (execute):", result.rows);

// Using prepare and get method
const stmt = client.prepare("SELECT * FROM users LIMIT 1");
const firstUser = await stmt.get();
console.log("First user:", firstUser);

// Using prepare and all method
const allUsers = await stmt.all();
console.log("All users (all):", allUsers);

// Using prepare and iterate method
console.log("Users (iterate):");
const iterateStmt = client.prepare("SELECT * FROM users");
for await (const user of iterateStmt.iterate()) {
    console.log("  -", user[0]);
}
