import { readFileSync, readdirSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { describe, expect, it } from 'vitest';
import ts from 'typescript';

const sourceDir = resolve(process.cwd(), 'src');

function sourceFiles(directory = sourceDir) {
  return readdirSync(directory, { withFileTypes: true }).flatMap(entry => {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) return sourceFiles(path);
    return path.endsWith('.tsx') && !entry.name.includes('.test.') ? [path] : [];
  });
}

function explicitName(opening) {
  return opening.attributes.properties.some(attribute =>
    ts.isJsxAttribute(attribute)
    && (attribute.name.text === 'aria-label' || attribute.name.text === 'aria-labelledby')
    && attribute.initializer
    && (!ts.isStringLiteral(attribute.initializer) || attribute.initializer.text.trim() !== ''),
  );
}

const genericNames = new Set([
  'add', 'close', 'collapse details', 'copy', 'delete', 'download',
  'edit', 'expand details', 'refresh', 'send', 'start', 'stop', 'terminal',
]);

function staticAttributeName(opening, attributeName) {
  const attribute = opening.attributes.properties.find(candidate =>
    ts.isJsxAttribute(candidate) && candidate.name.text === attributeName,
  );
  if (!attribute?.initializer) return '';
  if (ts.isStringLiteral(attribute.initializer)) {
    return attribute.initializer.text.trim().toLowerCase();
  }
  if (
    ts.isJsxExpression(attribute.initializer)
    && attribute.initializer.expression
    && (ts.isStringLiteral(attribute.initializer.expression)
      || ts.isNoSubstitutionTemplateLiteral(attribute.initializer.expression))
  ) {
    return attribute.initializer.expression.text.trim().toLowerCase();
  }
  return '';
}

function staticTooltipName(element) {
  let parent = element.parent;
  while (parent && !ts.isJsxElement(parent)) parent = parent.parent;
  if (!parent || parent.openingElement.tagName.getText() !== 'Tooltip') return false;
  return parent.openingElement.attributes.properties.some(attribute =>
    ts.isJsxAttribute(attribute)
    && attribute.name.text === 'title'
    && attribute.initializer,
  );
}

function descendants(node, visit) {
  visit(node);
  ts.forEachChild(node, child => descendants(child, visit));
}

function insideMappedCollection(node) {
  for (let parent = node.parent; parent; parent = parent.parent) {
    if (
      ts.isCallExpression(parent)
      && ts.isPropertyAccessExpression(parent.expression)
      && parent.expression.name.text === 'map'
    ) return true;
  }
  return false;
}

describe('icon button accessibility policy', () => {
  it('requires every icon-only button to have an enforceable accessible name', () => {
    const violations = [];
    for (const file of sourceFiles()) {
      const source = ts.createSourceFile(file, readFileSync(file, 'utf8'), ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
      descendants(source, node => {
        if (!ts.isJsxElement(node) || node.openingElement.tagName.getText(source) !== 'IconButton') return;
        const line = source.getLineAndCharacterOfPosition(node.getStart(source)).line + 1;
        const location = `${file.slice(sourceDir.length + 1)}:${line}`;
        if (!explicitName(node.openingElement) && !staticTooltipName(node)) {
          violations.push(location);
          return;
        }
        const staticName = staticAttributeName(node.openingElement, 'aria-label');
        if (genericNames.has(staticName) && insideMappedCollection(node)) {
          violations.push(`${location} (${staticName})`);
        }
      });
    }
    expect(violations).toEqual([]);
  });
});
