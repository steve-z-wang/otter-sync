/**
 * Split a SQL file such as `migration.sql` into its statements, for a tool
 * that runs one statement per call (Prisma's `$executeRawUnsafe` prepares
 * each one). A semicolon ends a statement only outside a dollar-quoted body,
 * a single-quoted string, a quoted identifier and a comment; `--` and
 * `/* … *\/` comments are dropped. Nested block comments are not supported.
 * A tool that can send several statements at once (`pg`'s `query` without
 * parameters, `psql`) should apply the file whole instead.
 */
export function sqlStatements(text: string): string[] {
  const statements: string[] = [];
  let current = "";
  let at = 0;
  const end = (close: string, from: number): number => {
    const found = text.indexOf(close, from);
    return found === -1 ? text.length : found + close.length;
  };
  while (at < text.length) {
    const rest = text.slice(at);
    const dollar = /^\$([A-Za-z_][A-Za-z0-9_]*)?\$/.exec(rest);
    let next: number;
    if (rest.startsWith("--")) {
      at = end("\n", at);
      current += "\n";
      continue;
    } else if (rest.startsWith("/*")) {
      at = end("*/", at + 2);
      current += " ";
      continue;
    } else if (dollar) {
      next = end(dollar[0], at + dollar[0].length);
    } else if (rest[0] === "'" || rest[0] === '"') {
      // A doubled quote inside is an escaped quote: it closes and reopens.
      next = end(rest[0], at + 1);
    } else if (rest[0] === ";") {
      if (current.trim()) statements.push(current.trim());
      current = "";
      at += 1;
      continue;
    } else {
      next = at + 1;
    }
    current += text.slice(at, next);
    at = next;
  }
  if (current.trim()) statements.push(current.trim());
  return statements;
}
