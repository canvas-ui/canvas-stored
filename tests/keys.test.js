import { test, describe } from 'node:test';
import assert from 'node:assert';
import { normalizeKey, validateKey, isIgnored, matchesPrefixes, conflictKey, MIRROR_IGNORE_DEFAULTS } from '../src/sync/keys.js';
import { decide } from '../src/sync/Mirror.js';

describe('sync keys', () => {
    test('normalizeKey: NFC, forward slashes, no empty segments', () => {
        assert.strictEqual(normalizeKey('a//b/'), 'a/b');
        assert.strictEqual(normalizeKey('/a\\b'), 'a/b');
        assert.strictEqual(normalizeKey('cafe\u0301.txt'), 'caf\u00e9.txt');
        assert.strictEqual(normalizeKey(''), '');
        assert.strictEqual(normalizeKey(null), '');
    });

    test('validateKey rejects what the hub or another platform would refuse', () => {
        assert.strictEqual(validateKey('Docs/notes.md'), null);
        assert.strictEqual(validateKey(''), 'empty');
        assert.strictEqual(validateKey('a\0b'), 'nul');
        assert.strictEqual(validateKey('../x'), 'traversal');
        assert.strictEqual(validateKey('a/./b'), 'traversal');
        assert.strictEqual(validateKey('/etc/passwd'), 'absolute');
        assert.strictEqual(validateKey('C:/x'), 'absolute');
        assert.strictEqual(validateKey('CON'), 'reserved-name');
        assert.strictEqual(validateKey('docs/nul.txt'), 'reserved-name');
        assert.strictEqual(validateKey('com1.tar.gz'), 'reserved-name');
        assert.strictEqual(validateKey('a:b.txt'), 'illegal-chars');
        assert.strictEqual(validateKey('what?.txt'), 'illegal-chars');
        assert.strictEqual(validateKey('trailing. '), 'trailing-dot-or-space');
        assert.strictEqual(validateKey('dir./x'), 'trailing-dot-or-space');
        assert.strictEqual(validateKey(`${'x'.repeat(256)}.txt`), 'segment-too-long');
        assert.strictEqual(validateKey(Array.from({ length: 40 }, () => 'y'.repeat(120)).join('/')), 'too-long');
    });

    test('isIgnored honours the mirror defaults (dot:true) and directory pruning', () => {
        for (const k of ['.workspace/db/x', '.stored-tmp/123', 'a/.DS_Store', 'Thumbs.db', 'x/desktop.ini', 'docs/~$report.docx', 'a.tmp', 'dl/movie.part', 'dl/x.crdownload', 'docs/.~lock.a.odt#', 'src/.main.js.swp']) {
            assert.strictEqual(isIgnored(k, MIRROR_IGNORE_DEFAULTS), true, k);
        }
        for (const k of ['notes.md', 'a/b.txt', 'workspace/x', 'tmp/x.txt']) {
            assert.strictEqual(isIgnored(k, MIRROR_IGNORE_DEFAULTS), false, k);
        }
        assert.strictEqual(isIgnored('node_modules/x/y.js', ['node_modules/**']), true);
        assert.strictEqual(isIgnored('a/node_modules/x.js', ['**/node_modules/**']), true);
        assert.strictEqual(isIgnored('Photos/raw/x.cr2', ['Photos/raw']), true, 'a bare directory pattern covers its subtree');
        assert.strictEqual(isIgnored('anything', []), false);
    });

    test('matchesPrefixes: plain prefixes cover their subtree, globs go through picomatch, empty = all', () => {
        assert.strictEqual(matchesPrefixes('x', []), true);
        assert.strictEqual(matchesPrefixes('Docs/a.md', ['Docs']), true);
        assert.strictEqual(matchesPrefixes('Docs', ['Docs/']), true);
        assert.strictEqual(matchesPrefixes('Documents/a.md', ['Docs']), false);
        assert.strictEqual(matchesPrefixes('Photos/2024/a.jpg', ['Photos/20*']), true);
        assert.strictEqual(matchesPrefixes('Photos/2024/a.jpg', ['Photos/20*/**']), true);
        assert.strictEqual(matchesPrefixes('Photos/2024/a.jpg', ['Docs', '**/*.md']), false);
    });

    test('conflictKey: <stem> (conflict from <device> <YYYY-MM-DD HHmm>).<ext>', () => {
        const at = new Date(Date.UTC(2026, 8, 5, 14, 7));
        assert.strictEqual(conflictKey('Docs/report.docx', 'laptop', at), 'Docs/report (conflict from laptop 2026-09-05 1407).docx');
        assert.strictEqual(conflictKey('README', 'my/laptop', at), 'README (conflict from my_laptop 2026-09-05 1407)');
        assert.strictEqual(conflictKey('.env', 'pc', at), '.env (conflict from pc 2026-09-05 1407)');
        assert.strictEqual(conflictKey('a.tar.gz', 'pc', at), 'a.tar (conflict from pc 2026-09-05 1407).gz');
    });
});

describe('three-way table (decide)', () => {
    const A = 'a', B = 'b', C = 'c', X = null;
    test('rows from the protocol doc', () => {
        assert.deepStrictEqual(decide(A, A, A), { action: 'nothing' });
        assert.deepStrictEqual(decide(X, X, X), { action: 'nothing' });
        assert.deepStrictEqual(decide(B, A, A), { action: 'push', ifMatch: 'a' });
        assert.deepStrictEqual(decide(A, X, X), { action: 'push', ifMatch: null });
        assert.deepStrictEqual(decide(X, A, A), { action: 'delete-remote', ifMatch: 'a' });
        assert.deepStrictEqual(decide(A, A, B), { action: 'pull', sha256: 'b' });
        assert.deepStrictEqual(decide(X, X, A), { action: 'pull', sha256: 'a' });
        assert.deepStrictEqual(decide(A, A, X), { action: 'trash-local' });
        assert.deepStrictEqual(decide(B, A, B), { action: 'adopt' });
        assert.deepStrictEqual(decide(X, A, X), { action: 'adopt' });
        assert.deepStrictEqual(decide(A, X, A), { action: 'adopt' });
        assert.deepStrictEqual(decide(B, A, C), { action: 'conflict', remote: 'c' });
        assert.deepStrictEqual(decide(A, X, B), { action: 'conflict', remote: 'b' });
        assert.deepStrictEqual(decide(X, A, B), { action: 'pull', sha256: 'b' }, 'we deleted, hub edited → pull');
        assert.deepStrictEqual(decide(B, A, X), { action: 'push', ifMatch: null }, 'hub deleted, we edited → push as new');
    });
});
