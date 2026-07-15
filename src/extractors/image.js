'use strict';

import { readFile } from 'fs/promises';
import debugInstance from 'debug';
const debug = debugInstance('canvas:stored:extract:image');

// Lazy, optional deps — resolved once. If absent, that sub-extraction no-ops.
let _exifr;
let _imageSize;
let _loaded = false;
async function load() {
    if (_loaded) { return; }
    _loaded = true;
    try { _exifr = (await import('exifr')).default; } catch { debug('exifr not installed — skipping EXIF/GPS'); }
    try {
        const m = await import('image-size');
        _imageSize = m.imageSize || m.default; // v2 named export / v1 default
    } catch { debug('image-size not installed — skipping dimensions'); }
}

function clean(obj) {
    const out = {};
    for (const [k, v] of Object.entries(obj)) {
        if (v !== undefined && v !== null && v !== '') { out[k] = v; }
    }
    return out;
}

/**
 * Extract image dimensions + EXIF (incl. GPS geo + capture date). Returns
 * { dimensions?, geo?, exif? }. Never throws.
 */
export async function extractImage(source, _mimeType) {
    await load();
    let buf = source.data;
    if (!buf && source.file) {
        try { buf = await readFile(source.file); } catch (e) { debug(`read failed: ${e.message}`); return {}; }
    }
    if (!buf) { return {}; }

    const out = {};

    if (_imageSize) {
        try {
            const d = _imageSize(buf);
            if (d?.width && d?.height) {
                out.dimensions = clean({ width: d.width, height: d.height, type: d.type, orientation: d.orientation });
            }
        } catch (e) { debug(`dimensions: ${e.message}`); }
    }

    if (_exifr) {
        try {
            const ex = await _exifr.parse(buf, { gps: true, tiff: true, exif: true, ifd0: true });
            if (ex) {
                if (typeof ex.latitude === 'number' && typeof ex.longitude === 'number') {
                    // `source` marks provenance so consumers can reconcile this
                    // against device/manual geo instead of last-writer-wins.
                    // `accuracy` = GPSHPositioningError, the camera's own
                    // horizontal error radius in metres — the thing that explains
                    // a pin sitting a block away from where the photo was taken.
                    out.geo = clean({
                        lat: ex.latitude,
                        lon: ex.longitude,
                        alt: typeof ex.GPSAltitude === 'number' ? ex.GPSAltitude : undefined,
                        accuracy: typeof ex.GPSHPositioningError === 'number' ? ex.GPSHPositioningError : undefined,
                        source: 'exif',
                    });
                }
                const date = ex.DateTimeOriginal || ex.CreateDate || ex.ModifyDate;
                const exif = clean({
                    make: ex.Make,
                    model: ex.Model,
                    lensModel: ex.LensModel,
                    orientation: ex.Orientation,
                    iso: ex.ISO,
                    fNumber: ex.FNumber,
                    exposureTime: ex.ExposureTime,
                    focalLength: ex.FocalLength,
                    capturedAt: date instanceof Date ? date.toISOString() : (typeof date === 'string' ? date : undefined),
                });
                if (Object.keys(exif).length) { out.exif = exif; }
            }
        } catch (e) { debug(`exif: ${e.message}`); }
    }

    return out;
}

export default extractImage;
