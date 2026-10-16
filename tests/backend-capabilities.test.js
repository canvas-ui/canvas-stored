import test from 'node:test';
import assert from 'node:assert/strict';
import BackendManager, { getBackendCapabilities } from '../src/backends/BackendManager.js';
import StorageBackend from '../src/backends/StorageBackend.js';
import FileBackend from '../src/backends/file/index.js';

for (const [driver, paths, upload, mutableContainers] of [
    ['file', true, true, true], ['gdrive', true, true, true],
    ['s3', true, true, false], ['canvas', true, false, false],
    ['cacache', false, false, false], ['http', false, false, false],
    ['unknown', false, false, false],
]) {
    test(`${driver} declares path features without opening storage or needing credentials`, () => {
        const features = getBackendCapabilities({ driver });
        assert.equal(features.paths, paths);
        assert.equal(features.upload, upload);
        assert.equal(features.mutableContainers, mutableContainers);
        const readOnly = getBackendCapabilities({ driver, readOnly: true });
        assert.equal(readOnly.paths, paths, 'namespace support is independent of write permission');
        assert.equal(readOnly.write, false);
        assert.equal(readOnly.upload, false);
        assert.equal(readOnly.mutableContainers, false);
        assert.equal(readOnly.delete, false);
    });
}

test('live capabilities use the same driver declaration and preserve runtime mount detection', () => {
    const backend = new FileBackend('disk', { driver: 'file', root: '/tmp', remote: true });
    assert.deepEqual(backend.capabilities, getBackendCapabilities(backend.config));
    assert.equal(backend.canWrite, true);
    backend.config.readOnly = true;
    assert.equal(backend.canWrite, false);
    assert.equal(backend.canDelete, false);
});

test('registered drivers declare their own features, unknown drivers fail closed', () => {
    class Custom extends StorageBackend {
        static getCapabilities(config) { return { ...super.getCapabilities(config), paths: true }; }
    }
    BackendManager.register('test-paths', Custom);
    const manager = new BackendManager();
    const backend = manager.add('custom', { driver: 'test-paths' });
    assert.equal(getBackendCapabilities({ driver: 'test-paths' }).paths, true);
    assert.equal(backend.capabilities.paths, true);
    assert.equal(getBackendCapabilities({ driver: 'not-registered' }).write, false);
});
