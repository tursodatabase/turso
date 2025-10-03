import http from "http";
import { readFile } from "fs/promises";
import { extname, join } from "path";
import { fileURLToPath } from "url";

const root = fileURLToPath(new URL("./dist", import.meta.url));
const port = 8080;

const mimeTypes = { ".html": "text/html", ".js": "application/javascript" };

const server = http.createServer(async (req, res) => {
  // COOP / COEP headers necessary for shared WASM memory
  res.setHeader("Cross-Origin-Opener-Policy", "same-origin");
  res.setHeader("Cross-Origin-Embedder-Policy", "require-corp");

  try {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const pathname = url.pathname;
    const filePath = pathname === "/" ? "index.html" : pathname;
    const fullPath = join(root, filePath);

    const data = await readFile(fullPath);
    const ext = extname(fullPath).toLowerCase();
    const contentType = mimeTypes[ext] || "application/octet-stream";

    res.writeHead(200, { "Content-Type": contentType });
    res.end(data);
  } catch (err) {
    res.writeHead(404, { "Content-Type": "text/plain" });
    res.end("Not found");
  }
});

server.listen(port, "0.0.0.0", () => console.log(`Serving on http://localhost:${port}`));
