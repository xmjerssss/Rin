import { inArray } from "drizzle-orm";
import { Hono } from "hono";
import type { AppContext } from "../core/hono-types";
import { adminOnly } from "../core/route-boundaries";
import {
    cache,
    comments,
    feedHashtags,
    feeds,
    friends,
    hashtags,
    moments,
    storageFiles,
    storages,
    users,
    visitStats,
} from "../db/schema";
import { createEnvDriverIfConfigured } from "../utils/storage-drivers";
import { driverForRow } from "../utils/storage-manager";

const BACKUP_SCHEMA_VERSION = 1;

/** 获取当前系统可用的最佳存储驱动（优先使用自定义的高优先级存储，否则使用默认存储） */
async function getActiveStorageDriver(env: Env, db: any) {
    const customStorages = await db.select().from(storages);
    const active = customStorages.find((s: any) => s.enabled === 1 && s.status === "ok");
    if (active) {
        const driver = await driverForRow(env, active);
        return { driver, folder: active.folder || "backups", name: active.name };
    }
    const envDriver = createEnvDriverIfConfigured(env);
    if (envDriver) {
        return { driver: envDriver.driver, folder: envDriver.folder || "backups", name: "默认存储" };
    }
    return null;
}

export function BackupService(): Hono {
    const app = new Hono();

    // 构建完整数据快照
    async function createSnapshot(db: any) {
        const [
            userRows,
            feedRows,
            momentRows,
            friendRows,
            commentRows,
            hashtagRows,
            feedHashtagRows,
            configRows,
            storageRows,
            storageFileRows,
            visitStatRows,
        ] = await Promise.all([
            db.select().from(users),
            db.select().from(feeds),
            db.select().from(moments),
            db.select().from(friends),
            db.select().from(comments),
            db.select().from(hashtags),
            db.select().from(feedHashtags),
            db.select().from(cache).where(inArray(cache.type, ["client.config", "server.config"])),
            db.select().from(storages),
            db.select().from(storageFiles),
            db.select().from(visitStats),
        ]);

        const totalMediaBytes = storageFileRows.reduce((acc: number, f: any) => acc + (f.size || 0), 0);

        return {
            version: BACKUP_SCHEMA_VERSION,
            app: "rin",
            exportedAt: new Date().toISOString(),
            metadata: {
                counts: {
                    feeds: feedRows.length,
                    moments: momentRows.length,
                    comments: commentRows.length,
                    friends: friendRows.length,
                    hashtags: hashtagRows.length,
                    storages: storageRows.length,
                    mediaFiles: storageFileRows.length,
                },
                mediaSummary: {
                    totalFiles: storageFileRows.length,
                    totalBytes: totalMediaBytes,
                },
            },
            data: {
                users: userRows,
                feeds: feedRows,
                moments: momentRows,
                friends: friendRows,
                comments: commentRows,
                hashtags: hashtagRows,
                feedHashtags: feedHashtagRows,
                configs: configRows,
                storages: storageRows,
                storageFiles: storageFileRows,
                visitStats: visitStatRows,
            },
        };
    }

    // 执行数据恢复
    async function executeRestore(db: any, cacheManager: any, payload: any) {
        if (!payload || !payload.data || payload.app !== "rin") {
            throw new Error("无效的备份文件结构，请确保上传由 Rin 导出的备份包");
        }

        const data = payload.data;

        // D1 批处理写入工具，每次 30 条，彻底避免 SQLite 参数超限
        async function chunkedInsert<T extends Record<string, any>>(table: any, rows: T[], chunkSize = 30) {
            if (!rows || rows.length === 0) return;
            for (let i = 0; i < rows.length; i += chunkSize) {
                const chunk = rows.slice(i, i + chunkSize);
                await db.insert(table).values(chunk as any);
            }
        }

        const parseDate = (d: any) => (d ? new Date(d) : new Date());

        // 1. 清理原有数据（按逆外键依赖顺序）
        await db.delete(feedHashtags);
        await db.delete(comments);
        await db.delete(visitStats);
        await db.delete(feeds);
        await db.delete(hashtags);
        await db.delete(moments);
        await db.delete(friends);
        await db.delete(storageFiles);
        await db.delete(storages);
        await db.delete(cache).where(inArray(cache.type, ["client.config", "server.config"]));
        await db.delete(users);

        // 2. 依次按拓扑依赖顺序安全恢复插入
        if (data.users?.length) {
            await chunkedInsert(users, data.users.map((u: any) => ({
                ...u,
                createdAt: parseDate(u.createdAt),
                updatedAt: parseDate(u.updatedAt),
            })));
        }

        if (data.hashtags?.length) {
            await chunkedInsert(hashtags, data.hashtags.map((h: any) => ({
                ...h,
                createdAt: parseDate(h.createdAt),
                updatedAt: parseDate(h.updatedAt),
            })));
        }

        if (data.feeds?.length) {
            await chunkedInsert(feeds, data.feeds.map((f: any) => ({
                ...f,
                createdAt: parseDate(f.createdAt),
                updatedAt: parseDate(f.updatedAt),
            })));
        }

        if (data.feedHashtags?.length) {
            await chunkedInsert(feedHashtags, data.feedHashtags.map((fh: any) => ({
                ...fh,
                createdAt: parseDate(fh.createdAt),
                updatedAt: parseDate(fh.updatedAt),
            })));
        }

        if (data.comments?.length) {
            await chunkedInsert(comments, data.comments.map((cm: any) => ({
                ...cm,
                createdAt: parseDate(cm.createdAt),
                updatedAt: parseDate(cm.updatedAt),
            })));
        }

        if (data.moments?.length) {
            await chunkedInsert(moments, data.moments.map((m: any) => ({
                ...m,
                createdAt: parseDate(m.createdAt),
                updatedAt: parseDate(m.updatedAt),
            })));
        }

        if (data.friends?.length) {
            await chunkedInsert(friends, data.friends.map((fr: any) => ({
                ...fr,
                createdAt: parseDate(fr.createdAt),
                updatedAt: parseDate(fr.updatedAt),
            })));
        }

        if (data.storages?.length) {
            await chunkedInsert(storages, data.storages.map((s: any) => ({
                ...s,
                createdAt: parseDate(s.createdAt),
                updatedAt: parseDate(s.updatedAt),
            })));
        }

        if (data.storageFiles?.length) {
            await chunkedInsert(storageFiles, data.storageFiles.map((sf: any) => ({
                ...sf,
                createdAt: parseDate(sf.createdAt),
            })));
        }

        if (data.configs?.length) {
            await chunkedInsert(cache, data.configs.map((cf: any) => ({
                ...cf,
                createdAt: parseDate(cf.createdAt),
                updatedAt: parseDate(cf.updatedAt),
            })));
        }

        if (data.visitStats?.length) {
            await chunkedInsert(visitStats, data.visitStats.map((vs: any) => ({
                ...vs,
                updatedAt: parseDate(vs.updatedAt),
            })));
        }

        // 清理系统缓存
        await cacheManager.clear();
    }

    // 1. GET /backup/export —— 下载完整备份 JSON 文件到本地
    app.get("/export", adminOnly(async (c: AppContext) => {
        const db = c.get("db");
        const snapshot = await createSnapshot(db);
        const jsonStr = JSON.stringify(snapshot, null, 2);
        const dateStr = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
        const fileName = `rin-backup-${dateStr}.json`;

        return new Response(jsonStr, {
            status: 200,
            headers: {
                "Content-Type": "application/json; charset=utf-8",
                "Content-Disposition": `attachment; filename="${fileName}"`,
                "Cache-Control": "no-store",
            },
        });
    }));

    // 2. POST /backup/export-to-storage —— 备份并存入已绑定的 S3/R2/WebDAV 存储桶
    app.post("/export-to-storage", adminOnly(async (c: AppContext) => {
        const db = c.get("db");
        const env = c.get("env");

        const target = await getActiveStorageDriver(env, db);
        if (!target) {
            return c.json({ ok: false, error: "未找到任何可用的存储桶（请先在后台「存储管理」添加 R2/S3/WebDAV）" }, 400);
        }

        const snapshot = await createSnapshot(db);
        const jsonStr = JSON.stringify(snapshot, null, 2);
        const dateStr = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
        const objectKey = `${target.folder ? `${target.folder}/` : ""}backups/rin-backup-${dateStr}.json`.replace(/\/+/g, "/");

        try {
            await target.driver.put(objectKey, jsonStr, "application/json");
            return c.json({
                ok: true,
                target: target.name,
                path: objectKey,
                exportedAt: snapshot.exportedAt,
                metadata: snapshot.metadata,
            });
        } catch (error: any) {
            return c.json({ ok: false, error: `写入存储桶失败: ${error.message || String(error)}` }, 500);
        }
    }));

    // 3. POST /backup/import —— 从上传的文件恢复全量数据
    app.post("/import", adminOnly(async (c: AppContext) => {
        const db = c.get("db");
        const cacheManager = c.get("cache");
        let payload: any;

        const contentType = c.req.header("content-type") || "";
        if (contentType.includes("multipart/form-data")) {
            const form = await c.req.parseBody();
            const file = form.file as File | string | undefined;
            if (!file || typeof file === "string") {
                return c.json({ ok: false, error: "请上传备份文件（.json）" }, 400);
            }
            try {
                payload = JSON.parse(await file.text());
            } catch {
                return c.json({ ok: false, error: "备份文件格式损坏，无法解析为 JSON" }, 400);
            }
        } else {
            payload = await c.req.json().catch(() => null);
        }

        try {
            await executeRestore(db, cacheManager, payload);
            return c.json({
                ok: true,
                message: "数据恢复成功！",
                counts: payload.metadata?.counts || {},
            });
        } catch (error: any) {
            return c.json({ ok: false, error: error.message || "恢复失败" }, 500);
        }
    }));

    return app;
}
