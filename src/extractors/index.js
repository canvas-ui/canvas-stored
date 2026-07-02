'use strict';

import debugInstance from 'debug';
const debug = debugInstance('canvas:stored:extract');

import { extractImage } from './image.js';
import { extractAudio } from './audio.js';

/**
 * Best-effort metadata extraction from blob bytes at ingest, dispatched by mime.
 *
 * Contract: NEVER throws — extraction failure must never fail a store. Parser
 * libs (exifr, image-size, music-metadata) are lazy + OPTIONAL; if one isn't
 * installed, that modality silently returns {}. Returns a flat metadata object
 * ({ geo, exif, dimensions, media }) to be merged into the blob's `custom` record
 * — never file bytes.
 *
 * @param {{file?:string, data?:Buffer}} source  temp-file path OR in-memory buffer
 * @param {{mimeType?:string, key?:string}} [opts]
 * @returns {Promise<object>} extracted metadata ({} when nothing/unsupported)
 */
export async function extract(source, { mimeType = '', key } = {}) {
    try {
        if (!source || (!source.file && !source.data)) { return {}; }
        if (/^image\//.test(mimeType)) { return (await extractImage(source, mimeType)) || {}; }
        if (/^audio\//.test(mimeType)) { return (await extractAudio(source, mimeType)) || {}; }
        return {};
    } catch (e) {
        debug(`extract failed (${mimeType}, ${key}): ${e.message}`);
        return {};
    }
}

export default extract;
