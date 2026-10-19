import { and, asc, desc, eq, sql } from "drizzle-orm";
import type { DB } from "../core/hono-types";
import { storageFiles, storages } from "../db/schema";
import { path_join } from "./path";
import { decryptSecret, encryptSecret } from "./storage-secrets";
import {
    createDriver,
    createEnvDriverIfConfigured,
    isCapacityError,
    type PresignedUpload,
    type StorageBody,
    type StorageConnection,
    type StorageDriver,
    type StorageType,
} from "./storage-drivers";
import { getStoragePublicUrl, resolveStorageTarget } from "./storage";

export type StorageRow = typeof storages.$inferSelect;
export type StorageFileRow = typeof storageFiles.$inferSelect;

export const DEFAULT_STORAGE_ID = 0;

function trimSlash(value: string) {
    return value.replace(/\/+$/, "");
}

export function sanitizeExtension(name: string): string {
    const idx = name.lastIndexOf(".");
    if (idx < 0) return "";
    const ext = name.slice(idx + 1).toLowerCase().replace(/[^a-z0-9]/g, "").slice(0, 10);
    return ext;
}

export function buildFileKey(hashOrId: string, fileName: string): string {
    const ext = sanitizeExtension(fileName);
    return ext ? `${hashOrId}.${ext}` : hashOrId;
}

export function buildBlobPath(key: string): string {
    return `/api/blob/${key.split("/").map(encodeURIComponent).join("/")}`;
}

export function buildBlobUrl(key: string, baseUrl?: string): string {
    const path = buildBlobPath(key);
    return baseUrl ? `${trimSlash(baseUrl)}${path}` : path;
}

export async function rowToConnection(env: Env, row: StorageRow): Promise<StorageConnection> {
    return {
        type: row.type as StorageType,
        endpoint: row.endpoint,
        bucket: row.bucket,
        region: row.region || "auto",
        accessKey: row.accessKey,
        secret: await decryptSecret(env, row.secretEnc),
        forcePathStyle: row.forcePathStyle === 1,
    };
}

export async function driverForRow(env: Env, row: StorageRow): Promise<StorageDriver> {
    return createDriver(await rowToConnection(env, row));
}

export async function listStorageUsage(db: DB): Promise<Map<number, { size: number; count: number }>> {
    const rows = await db
        .select({
            storageId: storageFiles.storageId,
            size: sql<number>`coalesce(sum(${storageFiles.size}), 0)`,
            count: sql<number>`count(*)`,
        })
        .from(storageFiles)
        .groupBy(storageFiles.storageId);
    return new Map(rows.map((row) => [row.storageId, { size: Number(row.size), count: Number(row.count) }]));
}

export async function listStorageRows(db: DB): Promise<StorageRow[]> {
    return db.select().from(storages).orderBy(desc(storages.priority), asc(storages.id));
}

export async function getStorageRow(db: DB, id: number): Promise<StorageRow | undefined> {
    const rows = await db.select().from(storages).where(eq(storages.id, id)).limit(1);
    return rows[0];
}

export async function getFileRow(db: DB, key: string): Promise<StorageFileRow | undefined> {
    const rows = await db.select().from(storageFiles).where(eq(storageFiles.key, key)).limit(1);
    return rows[0];
}

export async function recordFile(db: DB, row: Omit<StorageFileRow, "createdAt">) {
    await db
        .insert(storageFiles)
        .values(row)
        .onConflictDoUpdate({
            target: storageFiles.key,
            set: {
                storageId: row.storageId,
                objectKey: row.objectKey,
                name: row.name,
                mime: row.mime,
                size: row.size,
            },
        });
}

async function markStorage(db: DB, id: number, status: "ok" | "full" | "error", lastError = "") {
    await db
        .update(storages)
        .set({ status, lastError: lastError.slice(0, 500), updatedAt: new Date() })
        .where(eq(storages.id, id));
}

export interface UploadCandidate {
    /** 0 表示环境变量默认存储 */
    id: number;
    row?: StorageRow;
    folder: string;
}

/**
 * 选择上传目标，顺序：
 * 1. 已启用、未标记为"已满"、未超过容量上限的自定义存储（按优先级从高到低）
 * 2. 环境变量绑定的默认存储（兜底）
 */
export async function pickUploadCandidates(db: DB, env: Env, incomingSize: number): Promise<UploadCandidate[]> {
    const rows = await db
        .select()
        .from(storages)
        .where(and(eq(storages.enabled, 1)))
        .orderBy(desc(storages.priority), asc(storages.id));

    const candidates: UploadCandidate[] = [];
    if (rows.length > 0) {
        const usage = await listStorageUsage(db);
        for (const row of rows) {
            if (row.status === "full") continue;
            if (row.quotaBytes > 0) {
                const used = usage.get(row.id)?.size ?? 0;
                if (used + incomingSize > row.quotaBytes) continue;
            }
            candidates.push({ id: row.id, row, folder: row.folder });
        }
    }

    // 环境默认存储始终作为最后兜底；配置不完整时由后续 put 抛出明确的错误信息
    candidates.push({ id: DEFAULT_STORAGE_ID, folder: env.S3_FOLDER || "" });
    return candidates;
}

export interface StoredFile {
    key: string;
    url: string;
    storageId: number;
    reused: boolean;
}

export interface FileMeta {
    key: string;
    name: string;
    mime: string;
    size: number;
}

/**
 * 按优先级把文件写入存储，失败自动切换到下一个。
 * 容量类错误会把该存储标记为"已满"，之后不再选用，直到管理员在后台重置。
 */
export async function storeFile(
    db: DB,
    env: Env,
    meta: FileMeta,
    body: StorageBody,
    baseUrl?: string,
): Promise<StoredFile> {
    // 内容寻址 key 已存在且所在存储仍然有效 → 直接复用，不重复上传
    const existing = await getFileRow(db, meta.key);
    if (existing) {
        const stillValid =
            existing.storageId === DEFAULT_STORAGE_ID || Boolean(await getStorageRow(db, existing.storageId));
        if (stillValid) {
            return { key: meta.key, url: buildBlobUrl(meta.key, baseUrl), storageId: existing.storageId, reused: true };
        }
    }

    const candidates = await pickUploadCandidates(db, env, meta.size);
    let lastError: unknown = null;

    for (const candidate of candidates) {
        try {
            if (candidate.id === DEFAULT_STORAGE_ID) {
                const envDriver = createEnvDriverIfConfigured(env);
                if (!envDriver) {
                    // 自定义存储都失败了，且没有默认存储兜底：抛出自定义存储的真实错误，而不是 "S3_ENDPOINT is not defined"
                    if (lastError) throw lastError;
                    // 没有任何自定义存储：沿用旧逻辑的报错信息（S3_ENDPOINT is not defined 等）
                    resolveStorageTarget(env);
                    throw new Error("No storage is configured");
                }
                const objectKey = path_join(envDriver.folder, meta.key);
                await envDriver.driver.put(objectKey, body, meta.mime || undefined);
                // 默认存储沿用旧的 URL 规则（有 S3_ACCESS_HOST 时返回直链，否则 /api/blob/<folder>/<key>）
                await recordFile(db, {
                    key: path_join(envDriver.folder, meta.key),
                    storageId: DEFAULT_STORAGE_ID,
                    objectKey,
                    name: meta.name,
                    mime: meta.mime,
                    size: meta.size,
                });
                return {
                    key: objectKey,
                    url: getStoragePublicUrl(env, objectKey, baseUrl),
                    storageId: DEFAULT_STORAGE_ID,
                    reused: false,
                };
            }

            const row = candidate.row!;
            const driver = await driverForRow(env, row);
            const objectKey = path_join(row.folder, meta.key);
            await driver.put(objectKey, body, meta.mime || undefined);
            await recordFile(db, {
                key: meta.key,
                storageId: row.id,
                objectKey,
                name: meta.name,
                mime: meta.mime,
                size: meta.size,
            });
            if (row.status !== "ok" || row.lastError) {
                await markStorage(db, row.id, "ok");
            }
            return { key: meta.key, url: buildBlobUrl(meta.key, baseUrl), storageId: row.id, reused: false };
        } catch (error) {
            lastError = error;
            const message = error instanceof Error ? error.message : String(error);
            console.error(`[Storage] upload to #${candidate.id} failed: ${message}`);
            if (candidate.row) {
                try {
                    await markStorage(db, candidate.id, isCapacityError(error) ? "full" : "error", message);
                } catch (markError) {
                    console.error("[Storage] failed to update storage status", markError);
                }
            }
        }
    }

    throw lastError instanceof Error ? lastError : new Error("Upload failed");
}

export interface DirectUploadPlan {
    storageId: number;
    objectKey: string;
    upload: PresignedUpload;
}

/** 为大文件挑选一个支持预签名直传的存储（S3 / R2 兼容）。没有则返回 null，调用方走 Worker 中转。 */
export async function planDirectUpload(
    db: DB,
    env: Env,
    key: string,
    size: number,
    expiresInSeconds = 3600,
): Promise<DirectUploadPlan | null> {
    const candidates = await pickUploadCandidates(db, env, size);
    for (const candidate of candidates) {
        if (!candidate.row) continue;
        if (candidate.row.type === "webdav") continue;
        try {
            const driver = await driverForRow(env, candidate.row);
            if (!driver.presignPut) continue;
            const objectKey = path_join(candidate.row.folder, key);
            const upload = await driver.presignPut(objectKey, expiresInSeconds);
            return { storageId: candidate.row.id, objectKey, upload };
        } catch (error) {
            console.error(`[Storage] presign for #${candidate.id} failed`, error);
        }
    }
    return null;
}

export interface FileSource {
    driver: StorageDriver;
    objectKey: string;
    row?: StorageFileRow;
    storage?: StorageRow;
}

/** 根据 /api/blob/<key> 里的 key 找到文件实际所在的存储。 */
export async function resolveFileSource(db: DB, env: Env, key: string): Promise<FileSource | null> {
    const fileRow = await getFileRow(db, key);

    if (fileRow && fileRow.storageId !== DEFAULT_STORAGE_ID) {
        const storage = await getStorageRow(db, fileRow.storageId);
        if (!storage) return null;
        return { driver: await driverForRow(env, storage), objectKey: fileRow.objectKey, row: fileRow, storage };
    }

    // 默认存储里的文件（旧文章里的链接，或没有配置自定义存储时上传的文件）
    const envDriver = createEnvDriverIfConfigured(env);
    if (!envDriver) return null;
    return { driver: envDriver.driver, objectKey: fileRow?.objectKey ?? key, row: fileRow };
}

export { encryptSecret };
