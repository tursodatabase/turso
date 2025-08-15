import fs from 'fs';
import path from 'path';

// Ruta al package.json
const pkgPath = path.join(process.cwd(), 'package.json');

// Leer el package.json actual
const pkg = JSON.parse(fs.readFileSync(pkgPath, 'utf8'));

// Añadir optionalDependencies usando la misma versión del paquete
pkg.optionalDependencies = {
  '@tursodatabase/database-linux-x64-gnu': pkg.version,
  '@tursodatabase/database-win32-x64-msvc': pkg.version,
  '@tursodatabase/database-darwin-universal': pkg.version,
  '@tursodatabase/database-wasm32-wasi': pkg.version,
};

// Guardar cambios en package.json
fs.writeFileSync(pkgPath, JSON.stringify(pkg, null, 2) + '\n', 'utf8');

console.log('✅ optionalDependencies añadidos a package.json');
