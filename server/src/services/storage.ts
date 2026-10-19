import { Hono } from "hono";
import type { AppContext } from "../core/hono-types";
import { profileAsync } from "../core/server-timing";
import { buildFileResponse } from "../utils/storage-drivers";
import {
    buildBlobUrl,
    buildFileKey,
    planDirectUpload,
    recordFile,
    resolveFileSource,
    storeFile,
} from "../utils/storage-manager";
import { getStorageRow, driverForRow } from "../utils/storage-manager";

function buf2hex(buffer: ArrayBuffer) {
    return [...new Uint8Array(buffer)]
        .map(x => x.toString(16).padStart(2, '0'))
        .join('');
}

/** 小于该大小的文件按内容做 SHA-1 去重；更大的文件不在 Worker 内存中再复制一份，改用随机 ID。 */
const HASH_SIZE_LIMIT = 20 * 1024 * 1024;
/** 经 Worker 中转上传的体积上限（Cloudflare 请求体上限约 100MB，留出 multipart 开销）。 */
export const PROXY_UPLOAD_LIMIT = 95 * 1024 * 1024;
/** 预签名直传的单文件上限（S3 单次 PUT 最大 5GB，这里保守限制为 2GB）。 */
export const DIRECT_UPLOAD_LIMIT = 2 * 1024 * 1024 * 1024;

function randomId() {
    return crypto.randomUUID().replace(/-/g, "");
}

function statusForError(error: any) {
    return error?.message?.includes('is not defined') ? 500 : 400;
}

export function StorageService(): Hono {
    const app = new Hono();

    // POST /storage  —— 经 Worker 中转上传（图片、小文件）
    app.post('/', async (c: AppContext) => {
        const uid = c.get('uid');
        const env = c.get('env');
        const db = c.get('db');

        if (!uid) {
            return c.text('Unauthorized', 401);
        }

        const body = await profileAsync(c, 'storage_parse', () => c.req.parseBody());
        const file = body.file as File | string | undefined;
        if (!file || typeof file === "string") {
            return c.text('file is required', 400);
        }
        const fileName = (typeof body.key === "string" && body.key) || file.name || "file";

        if (file.size > PROXY_UPLOAD_LIMIT) {
            return c.text(`文件超过 ${Math.floor(PROXY_UPLOAD_LIMIT / 1024 / 1024)}MB，无法经服务器中转上传。请在后台添加 S3 / R2 存储以启用大文件直传。`, 413);
        }

        let identity: string;
        if (file.size <= HASH_SIZE_LIMIT) {
            const fileBuffer = await profileAsync(c, 'storage_file_buffer', () => file.arrayBuffer());
            const hashArray = await profileAsync(c, 'storage_hash', () => crypto.subtle.digest({ name: 'SHA-1' }, fileBuffer));
            identity = buf2hex(hashArray);
        } else {
            identity = randomId();
        }

        const meta = {
            key: buildFileKey(identity, fileName),
            name: file.name || fileName,
            mime: file.type || "",
            size: file.size,
        };

        try {
            const result = await profileAsync(c, 'storage_put', () =>
                storeFile(db, env, meta, file, new URL(c.req.url).origin));
            return c.json({ url: result.url });
        } catch (e: any) {
            console.error(e.message);
            return c.text(e.message, statusForError(e));
        }
    });

    // POST /storage/presign  —— 大文件直传：返回预签名地址，浏览器直接 PUT 到存储桶
    app.post('/presign', async (c: AppContext) => {
        const uid = c.get('uid');
        const env = c.get('env');
        const db = c.get('db');
        const jwt = c.get('jwt');
        if (!uid) {
            return c.text('Unauthorized', 401);
        }

        const body = await c.req.json().catch(() => null) as { name?: string; size?: number; type?: string } | null;
        const name = body?.name?.trim();
        const size = Number(body?.size);
        if (!name || !Number.isFinite(size) || size <= 0) {
            return c.text('Invalid input', 400);
        }
        if (size > DIRECT_UPLOAD_LIMIT) {
            return c.text(`文件超过 ${Math.floor(DIRECT_UPLOAD_LIMIT / 1024 / 1024 / 1024)}GB 上限`, 413);
        }

        const key = buildFileKey(randomId(), name);
        const plan = await planDirectUpload(db, env, key, size);
        if (!plan) {
            return c.json({ mode: "proxy" });
        }

        const mime = body?.type || "";
        const token = await jwt.sign({
            kind: "storage-upload",
            uid,
            key,
            storageId: plan.storageId,
            objectKey: plan.objectKey,
            name,
            mime,
            size,
            exp: Math.floor(Date.now() / 1000) + 3600 + 300,
        });

        return c.json({
            mode: "direct",
            url: plan.upload.url,
            method: plan.upload.method,
            headers: { ...plan.upload.headers, ...(mime ? { "Content-Type": mime } : {}) },
            token,
        });
    });

    // POST /storage/complete  —— 直传完成后确认：校验对象确实存在，再登记到文件表
    app.post('/complete', async (c: AppContext) => {
        const uid = c.get('uid');
        const env = c.get('env');
        const db = c.get('db');
        const jwt = c.get('jwt');
        if (!uid) {
            return c.text('Unauthorized', 401);
        }

        const body = await c.req.json().catch(() => null) as { token?: string } | null;
        const payload = body?.token ? await jwt.verify(body.token) : false;
        if (!payload || payload.kind !== "storage-upload" || payload.uid !== uid) {
            return c.text('Invalid or expired upload token', 400);
        }

        const storage = await getStorageRow(db, Number(payload.storageId));
        if (!storage) {
            return c.text('Storage no longer exists', 400);
        }

        try {
            const driver = await driverForRow(env, storage);
            const head = await driver.head(String(payload.objectKey));
            if (!head) {
                return c.text('Uploaded object not found in storage', 400);
            }
            if (head.size && head.size !== Number(payload.size)) {
                return c.text('Uploaded object size mismatch', 400);
            }
        } catch (e: any) {
            console.error(e.message);
            return c.text(e.message, 400);
        }

        await recordFile(db, {
            key: String(payload.key),
            storageId: storage.id,
            objectKey: String(payload.objectKey),
            name: String(payload.name || ""),
            mime: String(payload.mime || ""),
            size: Number(payload.size),
        });

        return c.json({ url: buildBlobUrl(String(payload.key), new URL(c.req.url).origin) });
    });

    return app;
}

export function BlobService(): Hono {
    const app = new Hono();

    app.get("/*", async (c: AppContext) => {
        const env = c.get("env");
        const db = c.get("db");
        const rawKey = c.req.path.replace(/^\/blob\/?/, "");

        if (!rawKey) {
            return c.text("Blob key is required", 400);
        }

        let key: string;
        try {
            key = decodeURIComponent(rawKey);
        } catch {
            return c.text("Invalid blob key", 400);
        }

        try {
            const source = await profileAsync(c, "blob_resolve", () => resolveFileSource(db, env, key));
            if (!source) {
                return c.text("Not found", 404);
            }

            // 自定义存储配置了公开域名并开启"直链跳转"：302 到存储自己的域名，不占用 Worker 流量。
            // 文章里的链接仍然是 /api/blob/<key>，随时可以关闭跳转或更换域名。
            const storage = source.storage;
            if (storage?.redirect === 1 && storage.publicUrl) {
                const target = `${storage.publicUrl.replace(/\/+$/, "")}/${source.objectKey
                    .split("/")
                    .map(encodeURIComponent)
                    .join("/")}`;
                return new Response(null, {
                    status: 302,
                    headers: { Location: target, "Cache-Control": "public, max-age=300" },
                });
            }

            const range = c.req.header("Range");
            const upstream = await profileAsync(c, "blob_fetch", () => source.driver.get(source.objectKey, range));
            if (!upstream) {
                return c.text("Not found", 404);
            }

            return buildFileResponse(upstream, {
                mime: source.row?.mime,
                name: source.row?.name,
            });
        } catch (error) {
            console.error("Blob fetch failed:", error);
            return c.text("Blob fetch failed", 500);
        }
    });

    return app;
}
