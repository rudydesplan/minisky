import { readFileSync, readdirSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { describe, expect, it } from 'vitest';
import ts from 'typescript';

const sourceDir = resolve(process.cwd(), 'src');

function sourceFiles(directory = sourceDir) {
  return readdirSync(directory, { withFileTypes: true }).flatMap(entry => {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) return sourceFiles(path);
    return /\.[jt]sx?$/.test(entry.name) && !entry.name.includes('.test.') ? [path] : [];
  });
}

function parse(file) {
  return ts.createSourceFile(
    file,
    readFileSync(file, 'utf8'),
    ts.ScriptTarget.Latest,
    true,
    file.endsWith('.tsx') ? ts.ScriptKind.TSX : ts.ScriptKind.TS,
  );
}

function descendants(node, visit) {
  visit(node);
  ts.forEachChild(node, child => descendants(child, visit));
}

function contains(parent, child) {
  return parent && parent.pos <= child.pos && parent.end >= child.end;
}

function failureBranchForBodyRead(call, responseName, source) {
  for (let node = call.parent; node; node = node.parent) {
    if (!ts.isIfStatement(node)) continue;
    const condition = node.expression.getText(source).replace(/\s/g, '');
    if (condition === `${responseName}.ok`) {
      if (contains(node.thenStatement, call)) return 'success';
      if (contains(node.elseStatement, call)) return 'failure';
    }
    if (condition === `!${responseName}.ok`) {
      if (contains(node.thenStatement, call)) return 'failure';
      if (contains(node.elseStatement, call)) return 'success';
    }
  }
  return 'unknown';
}

function bodyReadIsGuarded(call, responseName, source) {
  for (let node = call.parent; node; node = node.parent) {
    if (ts.isExpressionStatement(node) && /\brequireOk\s*\(/.test(node.getText(source))) return true;
  }
  const scope = (() => {
    for (let node = call.parent; node; node = node.parent) {
      if (ts.isFunctionLike(node)) return node;
    }
    return source;
  })();
  let declaration;
  descendants(scope, node => {
    if (ts.isVariableDeclaration(node)
      && ts.isIdentifier(node.name)
      && node.name.text === responseName
      && node.pos < call.pos
      && (!declaration || node.pos > declaration.pos)) declaration = node;
  });
  const initializer = declaration?.initializer;
  const initializerText = initializer?.getText(source) ?? '';
  if (/\bcheckedMutation\s*\(/.test(initializerText)) return true;

  let guarded = false;
  descendants(scope, node => {
    if (node.pos >= call.pos || !ts.isCallExpression(node)) return;
    if (node.expression.getText(source) === 'requireOk'
      && node.arguments[0]?.getText(source) === responseName) guarded = true;
  });
  return guarded;
}

const MUTATION_METHODS = new Set(['POST', 'PUT', 'PATCH', 'DELETE']);

function isStaticMessage(node) {
  return Boolean(node) && (ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node));
}

function directAwaitedRequireOk(statement, responseName, source, fetchCall) {
  if (!statement || !ts.isExpressionStatement(statement) || !ts.isAwaitExpression(statement.expression)) return false;
  const call = statement.expression.expression;
  if (!ts.isCallExpression(call) || call.expression.getText(source) !== 'requireOk' || !isStaticMessage(call.arguments[1])) {
    return false;
  }
  if (responseName) return call.arguments[0]?.getText(source) === responseName;
  const first = call.arguments[0];
  return ts.isAwaitExpression(first) && first.expression === fetchCall;
}

function exhaustiveFailureGuard(statement, responseName, source) {
  if (!statement || !ts.isIfStatement(statement)
    || statement.expression.getText(source).replace(/\s/g, '') !== `!${responseName}.ok`
    || statement.elseStatement) return false;
  const terminal = ts.isBlock(statement.thenStatement)
    ? statement.thenStatement.statements.length === 1 && statement.thenStatement.statements[0]
    : statement.thenStatement;
  if (!terminal) return false;
  if (ts.isThrowStatement(terminal)) {
    const expression = terminal.expression;
    return ts.isNewExpression(expression)
      && expression.expression.getText(source) === 'Error'
      && isStaticMessage(expression.arguments?.[0]);
  }
  if (!ts.isReturnStatement(terminal) || !terminal.expression) return false;
  const returned = ts.isAwaitExpression(terminal.expression) ? terminal.expression.expression : terminal.expression;
  return ts.isCallExpression(returned)
    && returned.expression.getText(source) === 'requireOk'
    && returned.arguments[0]?.getText(source) === responseName
    && isStaticMessage(returned.arguments[1]);
}

function mutationViolations(source, file = 'inline.ts') {
  const violations = [];
  descendants(source, node => {
    if (ts.isCallExpression(node) && node.expression.getText(source) === 'checkedMutation') {
      const guidance = node.arguments[2];
      if (!(ts.isStringLiteral(guidance) || ts.isNoSubstitutionTemplateLiteral(guidance))) {
        const line = source.getLineAndCharacterOfPosition(node.getStart(source)).line + 1;
        violations.push(`${file}:${line}: checkedMutation requires static guidance`);
      }
      return;
    }
    if (!ts.isCallExpression(node) || node.expression.getText(source) !== 'fetch') return;
    if (node.arguments[0]?.getText(source).includes(':listCollectionIds')) return;
    const options = node.arguments[1];
    if (!options || !ts.isObjectLiteralExpression(options)) return;
    const methodProperty = options.properties.find(property =>
      (ts.isPropertyAssignment(property) || ts.isShorthandPropertyAssignment(property))
        && property.name.getText(source) === 'method');
    if (!methodProperty) return;
    const line = source.getLineAndCharacterOfPosition(node.getStart(source)).line + 1;
    if (ts.isShorthandPropertyAssignment(methodProperty)) {
      violations.push(`${file}:${line}: dynamic mutation methods require checkedMutation`);
      return;
    }
    const method = methodProperty.initializer;
    if (!(ts.isStringLiteral(method) || ts.isNoSubstitutionTemplateLiteral(method))) {
      violations.push(`${file}:${line}: dynamic mutation methods require checkedMutation`);
      return;
    }
    if (!MUTATION_METHODS.has(method.text)) return;

    let containingStatement = node.parent;
    while (containingStatement && !ts.isExpressionStatement(containingStatement)) containingStatement = containingStatement.parent;
    if (containingStatement && directAwaitedRequireOk(containingStatement, undefined, source, node)) {
      return;
    }

    let declaration = node.parent;
    while (declaration && !ts.isVariableDeclaration(declaration)) declaration = declaration.parent;
    const statement = declaration?.parent?.parent;
    const block = statement?.parent;
    if (!declaration || !ts.isIdentifier(declaration.name) || !ts.isVariableStatement(statement) || !ts.isBlock(block)) {
      violations.push(`${file}:${line}: mutation fetch must be checked structurally`);
      return;
    }
    const next = block.statements[block.statements.indexOf(statement) + 1];
    if (!directAwaitedRequireOk(next, declaration.name.text, source)
      && !exhaustiveFailureGuard(next, declaration.name.text, source)) {
      violations.push(`${file}:${line}: requireOk must immediately follow the mutation`);
    }
  });
  return violations;
}

describe('UI error rendering policy', () => {
  it('does not read text or JSON bodies in known non-2xx branches', () => {
    const violations = [];
    for (const file of sourceFiles()) {
      const source = parse(file);
      descendants(source, node => {
        if (!ts.isCallExpression(node) || !ts.isPropertyAccessExpression(node.expression)) return;
        if (!['text', 'json'].includes(node.expression.name.text)) return;
        if (!ts.isIdentifier(node.expression.expression)) return;
        const responseName = node.expression.expression.text;
        const branch = failureBranchForBodyRead(node, responseName, source);
        if (branch === 'failure' || (branch === 'unknown' && !bodyReadIsGuarded(node, responseName, source))) {
          const line = source.getLineAndCharacterOfPosition(node.getStart(source)).line + 1;
          violations.push(`${file.slice(sourceDir.length + 1)}:${line}`);
        }
      });
    }
    expect(violations).toEqual([]);
  });

  it('guards every mutating fetch before success handling', () => {
    const violations = [];
    for (const file of sourceFiles()) {
      const source = parse(file);
      violations.push(...mutationViolations(source, file.slice(sourceDir.length + 1)));
    }
    expect(violations).toEqual([]);
  });

  it('rejects success-before-guard and identifier mutation methods', () => {
    const source = ts.createSourceFile('regression.ts', `
      async function broken(refresh, method) {
        const first = await fetch('/api/first', { method: 'POST' });
        refresh();
        await requireOk(first, 'Too late.');
        const second = await fetch('/api/second', { method });
        await requireOk(second, 'Dynamic method.');
      }
    `, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);

    expect(mutationViolations(source, 'regression.ts')).toEqual([
      'regression.ts:3: requireOk must immediately follow the mutation',
      'regression.ts:6: dynamic mutation methods require checkedMutation',
    ]);
  });

  it('rejects callback and non-exhaustive conditional guards', () => {
    const source = ts.createSourceFile('nested.ts', `
      async function broken(flag) {
        const callback = await fetch('/api/callback', { method: 'POST' });
        [callback].forEach(async response => await requireOk(response, 'Nested.'));
        const conditional = await fetch('/api/conditional', { method: 'DELETE' });
        if (!conditional.ok) {
          if (flag) throw new Error('Sometimes.');
        }
      }
    `, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);

    expect(mutationViolations(source, 'nested.ts')).toEqual([
      'nested.ts:3: requireOk must immediately follow the mutation',
      'nested.ts:5: requireOk must immediately follow the mutation',
    ]);
  });

  it('accepts direct and exhaustive immediate guards', () => {
    const source = ts.createSourceFile('valid.ts', `
      async function valid() {
        const direct = await fetch('/api/direct', { method: 'POST' });
        await requireOk(direct, 'Direct failure.');
        const exhaustive = await fetch('/api/exhaustive', { method: 'DELETE' });
        if (!exhaustive.ok) {
          return await requireOk(exhaustive, 'Delete failure.');
        }
        refresh();
      }
    `, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);

    expect(mutationViolations(source, 'valid.ts')).toEqual([]);
  });
});
