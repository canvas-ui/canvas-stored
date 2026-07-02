'use strict';

import debugInstance from 'debug';
const debug = debugInstance('canvas:stored:extract:audio');

let _mm;
let _loaded = false;
async function load() {
    if (_loaded) { return; }
    _loaded = true;
    try { _mm = await import('music-metadata'); } catch { debug('music-metadata not installed — skipping audio tags'); }
}

function clean(obj) {
    const out = {};
    for (const [k, v] of Object.entries(obj)) {
        if (v !== undefined && v !== null && v !== '') { out[k] = v; }
    }
    return out;
}

/**
 * Extract audio tags (ID3/Vorbis) + format info → { media }. Never throws.
 */
export async function extractAudio(source, mimeType) {
    await load();
    if (!_mm) { return {}; }
    try {
        const meta = source.data
            ? await _mm.parseBuffer(source.data, { mimeType })
            : await _mm.parseFile(source.file);
        const c = meta.common || {};
        const f = meta.format || {};
        const media = clean({
            title: c.title,
            artist: c.artist,
            album: c.album,
            year: c.year,
            genre: Array.isArray(c.genre) ? c.genre[0] : c.genre,
            track: c.track?.no ?? undefined,
            duration: f.duration,
            bitrate: f.bitrate,
            sampleRate: f.sampleRate,
            codec: f.codec,
        });
        return Object.keys(media).length ? { media } : {};
    } catch (e) {
        debug(`audio parse: ${e.message}`);
        return {};
    }
}

export default extractAudio;
