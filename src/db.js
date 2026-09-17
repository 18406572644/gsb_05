'use strict';

const { DatabaseSync } = require('node:sqlite');
const { now } = require('./util');

/**
 * 持久层：SQLite（WAL 模式）。
 *
 * 可靠性设计要点：
 * 1. 消息与房间序号 seq 在同一事务中「先落库、后广播」——进程崩溃也不丢已确认消息。
 * 2. messages 上 (room_id, sender_id, client_msg_id) 唯一约束——客户端重试/网络重复
 *    提交同一条消息时不会产生重复记录，实现发送幂等。
 * 3. seq 为每房间单调递增序号，由 rooms.last_seq 计数器在事务内分配——保证房间内
 *    消息全序（时序可控），客户端可凭 seq 检测空洞并触发补发。
 *
 * 编辑 / 撤回 / 审计：
 * 4. 每条消息带 version（编辑按 CAS：UPDATE ... WHERE version=? 递增），撤回是终态
 *    （revoked_at 置位后编辑/再次撤回都不能覆盖），「旧操作覆盖新状态」在 SQL 层被挡住。
 * 5. message_events 是独立的房间级事件流（每房间单调 rev）：编辑/撤回每发生一次写一行，
 *    保留操作者、原因、变更前后内容。它同时承担三个职责：完整审计、在线广播的事件载荷、
 *    断线后按 rev 补发（与按 seq 补发的消息快照流相互独立）。
 * 6. 撤回不清空 messages.content（审计取证需要），只在读取出口把 content 脱敏为 null；
 *    消息行仍按原 seq 保留，历史翻页与补发都能看到「已撤回」占位。
 */

const SCHEMA = `
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
PRAGMA synchronous = FULL;

CREATE TABLE IF NOT EXISTS users (
  id           TEXT PRIMARY KEY,
  name         TEXT NOT NULL UNIQUE,
  token_random TEXT NOT NULL,
  created_at   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS rooms (
  id         TEXT PRIMARY KEY,
  name       TEXT NOT NULL UNIQUE,
  created_by TEXT NOT NULL REFERENCES users(id),
  created_at INTEGER NOT NULL,
  last_seq   INTEGER NOT NULL DEFAULT 0,
  last_event_rev INTEGER NOT NULL DEFAULT 0  -- 房间事件流（编辑/撤回）计数器
);

CREATE TABLE IF NOT EXISTS members (
  room_id     TEXT NOT NULL REFERENCES rooms(id),
  user_id     TEXT NOT NULL REFERENCES users(id),
  role        TEXT NOT NULL DEFAULT 'member' CHECK (role IN ('admin','member')),
  muted_until INTEGER NOT NULL DEFAULT 0,
  joined_at   INTEGER NOT NULL,
  PRIMARY KEY (room_id, user_id)
);

CREATE TABLE IF NOT EXISTS messages (
  room_id       TEXT NOT NULL REFERENCES rooms(id),
  seq           INTEGER NOT NULL,
  client_msg_id TEXT NOT NULL,
  sender_id     TEXT NOT NULL REFERENCES users(id),
  content       TEXT NOT NULL,
  ts            INTEGER NOT NULL,
  version       INTEGER NOT NULL DEFAULT 1,      -- 当前内容版本，每次编辑 +1
  edited_at     INTEGER,                         -- 最近一次编辑时间
  revoked_at    INTEGER,                         -- 撤回时间；非空即终态（已撤回占位）
  revoked_by    TEXT REFERENCES users(id),       -- 撤回人（本人或管理员）
  revoke_reason TEXT,                            -- 撤回/审核原因
  PRIMARY KEY (room_id, seq),
  UNIQUE (room_id, sender_id, client_msg_id)  -- 幂等键
);

-- 消息事件流：一行一次编辑/撤回。rev 为房间内单调递增序号（事件全序）。
-- 既是审计记录，也是在线广播与断线补发的事件日志。
CREATE TABLE IF NOT EXISTS message_events (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  room_id        TEXT NOT NULL REFERENCES rooms(id),
  rev            INTEGER NOT NULL,
  seq            INTEGER NOT NULL,
  type           TEXT NOT NULL CHECK (type IN ('edit','revoke')),
  actor_id       TEXT NOT NULL REFERENCES users(id),
  op_id          TEXT,                            -- 客户端操作幂等 ID
  reason         TEXT,
  version        INTEGER NOT NULL,                -- 该操作生效后的消息版本
  edited_at      INTEGER,                         -- edit：编辑时间
  content_before TEXT,                            -- 变更前内容（revoke 时为被撤回原文）
  content_after  TEXT,                            -- 变更后内容（revoke 为 NULL）
  ts             INTEGER NOT NULL,
  UNIQUE (room_id, rev),
  FOREIGN KEY (room_id, seq) REFERENCES messages(room_id, seq)
);

-- 服务端保存的每用户每房间已确认游标（断线补发的兜底依据）
CREATE TABLE IF NOT EXISTS cursors (
  room_id      TEXT NOT NULL REFERENCES rooms(id),
  user_id      TEXT NOT NULL REFERENCES users(id),
  last_ack_seq INTEGER NOT NULL DEFAULT 0,
  last_ack_rev INTEGER NOT NULL DEFAULT 0,
  updated_at   INTEGER NOT NULL,
  PRIMARY KEY (room_id, user_id)
);
`;

/** 旧库升级：为已存在的表补齐编辑/撤回时代的列（SQLite 不支持 ADD COLUMN IF NOT EXISTS） */
const MIGRATIONS = [
  { table: 'rooms', column: 'last_event_rev', ddl: "ALTER TABLE rooms ADD COLUMN last_event_rev INTEGER NOT NULL DEFAULT 0" },
  { table: 'messages', column: 'version', ddl: "ALTER TABLE messages ADD COLUMN version INTEGER NOT NULL DEFAULT 1" },
  { table: 'messages', column: 'edited_at', ddl: "ALTER TABLE messages ADD COLUMN edited_at INTEGER" },
  { table: 'messages', column: 'revoked_at', ddl: "ALTER TABLE messages ADD COLUMN revoked_at INTEGER" },
  { table: 'messages', column: 'revoked_by', ddl: "ALTER TABLE messages ADD COLUMN revoked_by TEXT REFERENCES users(id)" },
  { table: 'messages', column: 'revoke_reason', ddl: "ALTER TABLE messages ADD COLUMN revoke_reason TEXT" },
  { table: 'cursors', column: 'last_ack_rev', ddl: "ALTER TABLE cursors ADD COLUMN last_ack_rev INTEGER NOT NULL DEFAULT 0" },
];

const MSG_SELECT = `
  SELECT m.room_id AS roomId, m.seq, m.client_msg_id AS clientMsgId,
         m.sender_id AS "from", u.name AS fromName, m.content, m.ts,
         m.version, m.edited_at AS editedAt,
         m.revoked_at AS revokedAt, m.revoked_by AS revokedBy,
         ru.name AS revokedByName, m.revoke_reason AS revokeReason
    FROM messages m
    JOIN users u ON u.id = m.sender_id
    LEFT JOIN users ru ON ru.id = m.revoked_by
`;

const EVENT_SELECT = `
  SELECT e.room_id AS roomId, e.rev, e.seq, e.type,
         e.actor_id AS actorId, u.name AS actorName,
         e.op_id AS opId, e.reason, e.version, e.edited_at AS editedAt,
         e.content_before AS contentBefore, e.content_after AS contentAfter, e.ts
    FROM message_events e
    LEFT JOIN users u ON u.id = e.actor_id
`;

/** 读取出口脱敏：撤回消息的正文不下发（原文只保留在服务端审计里） */
function present(m) {
  if (m && m.revokedAt) return { ...m, content: null };
  return m;
}

class ChatDB {
  constructor(dbPath) {
    this.db = new DatabaseSync(dbPath);
    this.db.exec(SCHEMA);
    this._migrate();
    this.db.exec(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_message_events_op
         ON message_events(room_id, actor_id, op_id) WHERE op_id IS NOT NULL`
    );
    this._prepare();
  }

  _migrate() {
    for (const { table, column, ddl } of MIGRATIONS) {
      const cols = this.db.prepare(`PRAGMA table_info(${table})`).all();
      if (!cols.some((c) => c.name === column)) this.db.exec(ddl);
    }
  }

  _prepare() {
    const d = this.db;
    this.stmt = {
      insertUser: d.prepare('INSERT INTO users (id, name, token_random, created_at) VALUES (?, ?, ?, ?)'),
      userByName: d.prepare('SELECT * FROM users WHERE name = ?'),
      userById: d.prepare('SELECT * FROM users WHERE id = ?'),

      insertRoom: d.prepare('INSERT INTO rooms (id, name, created_by, created_at) VALUES (?, ?, ?, ?)'),
      roomById: d.prepare('SELECT * FROM rooms WHERE id = ?'),
      roomByName: d.prepare('SELECT * FROM rooms WHERE name = ?'),
      roomsForUser: d.prepare(
        `SELECT r.id, r.name, r.last_seq AS lastSeq, r.last_event_rev AS lastEventRev,
                m.role, m.muted_until AS mutedUntil
           FROM rooms r JOIN members m ON m.room_id = r.id
          WHERE m.user_id = ? ORDER BY r.created_at`
      ),

      upsertMember: d.prepare(
        `INSERT INTO members (room_id, user_id, role, muted_until, joined_at)
         VALUES (?, ?, ?, 0, ?)
         ON CONFLICT (room_id, user_id) DO NOTHING`
      ),
      member: d.prepare('SELECT * FROM members WHERE room_id = ? AND user_id = ?'),
      setMuted: d.prepare('UPDATE members SET muted_until = ? WHERE room_id = ? AND user_id = ?'),
      membersOfRoom: d.prepare(
        `SELECT m.user_id AS userId, u.name, m.role, m.muted_until AS mutedUntil
           FROM members m JOIN users u ON u.id = m.user_id WHERE m.room_id = ?`
      ),

      // —— 消息写入（事务内使用）——
      msgByClientId: d.prepare(
        `${MSG_SELECT} WHERE m.room_id = ? AND m.sender_id = ? AND m.client_msg_id = ?`
      ),
      msgBySeq: d.prepare(`${MSG_SELECT} WHERE m.room_id = ? AND m.seq = ?`),
      rawMsgBySeq: d.prepare(
        'SELECT sender_id AS senderId, version, revoked_at AS revokedAt FROM messages WHERE room_id = ? AND seq = ?'
      ),
      bumpSeq: d.prepare('UPDATE rooms SET last_seq = last_seq + 1 WHERE id = ? RETURNING last_seq'),
      insertMsg: d.prepare(
        `INSERT INTO messages (room_id, seq, client_msg_id, sender_id, content, ts)
         VALUES (?, ?, ?, ?, ?, ?)`
      ),

      // —— 编辑 / 撤回（事务内使用，全部带状态条件，防止旧操作覆盖新状态）——
      casEdit: d.prepare(
        `UPDATE messages
            SET content = ?, version = version + 1, edited_at = ?
          WHERE room_id = ? AND seq = ? AND version = ? AND revoked_at IS NULL`
      ),
      revoke: d.prepare(
        `UPDATE messages
            SET revoked_at = ?, revoked_by = ?, revoke_reason = ?
          WHERE room_id = ? AND seq = ? AND revoked_at IS NULL`
      ),
      bumpEventRev: d.prepare(
        'UPDATE rooms SET last_event_rev = last_event_rev + 1 WHERE id = ? RETURNING last_event_rev'
      ),
      insertEvent: d.prepare(
        `INSERT INTO message_events
           (room_id, rev, seq, type, actor_id, op_id, reason, version,
            edited_at, content_before, content_after, ts)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      ),
      eventByOp: d.prepare(
        `${EVENT_SELECT} WHERE e.room_id = ? AND e.actor_id = ? AND e.op_id = ?`
      ),

      // —— 消息读取 ——
      msgsAfter: d.prepare(`${MSG_SELECT} WHERE m.room_id = ? AND m.seq > ? ORDER BY m.seq LIMIT ?`),
      msgsBefore: d.prepare(
        `${MSG_SELECT} WHERE m.room_id = ? AND m.seq < ? ORDER BY m.seq DESC LIMIT ?`
      ),

      // —— 事件读取 ——
      eventsAfter: d.prepare(`${EVENT_SELECT} WHERE e.room_id = ? AND e.rev > ? ORDER BY e.rev LIMIT ?`),
      eventsBefore: d.prepare(
        `${EVENT_SELECT} WHERE e.room_id = ? AND e.rev < ? ORDER BY e.rev DESC LIMIT ?`
      ),
      eventsForMsg: d.prepare(`${EVENT_SELECT} WHERE e.room_id = ? AND e.seq = ? ORDER BY e.rev`),

      // —— 游标 ——
      upsertCursor: d.prepare(
        `INSERT INTO cursors (room_id, user_id, last_ack_seq, last_ack_rev, updated_at)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT (room_id, user_id)
         DO UPDATE SET last_ack_seq = MAX(last_ack_seq, excluded.last_ack_seq),
                       last_ack_rev = MAX(last_ack_rev, excluded.last_ack_rev),
                       updated_at = excluded.updated_at`
      ),
      cursor: d.prepare(
        'SELECT last_ack_seq AS lastAckSeq, last_ack_rev AS lastAckRev FROM cursors WHERE room_id = ? AND user_id = ?'
      ),
    };

  }

  /** 在 IMMEDIATE 事务中执行 fn，失败回滚。node:sqlite 为同步驱动，单进程内无并发交错。 */
  _tx(fn) {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const r = fn();
      this.db.exec('COMMIT');
      return r;
    } catch (err) {
      try { this.db.exec('ROLLBACK'); } catch { /* 已回滚 */ }
      throw err;
    }
  }

  // ---------- 用户 ----------

  createUser(id, name, tokenRandom) {
    this.stmt.insertUser.run(id, name, tokenRandom, now());
    return this.stmt.userById.get(id);
  }

  getUserByName(name) { return this.stmt.userByName.get(name); }
  getUserById(id) { return this.stmt.userById.get(id); }

  // ---------- 房间与成员 ----------

  createRoom(id, name, creatorId) {
    return this._tx(() => {
      this.stmt.insertRoom.run(id, name, creatorId, now());
      // 创建者即管理员
      this.stmt.upsertMember.run(id, creatorId, 'admin', now());
      return this.stmt.roomById.get(id);
    });
  }

  getRoom(id) { return this.stmt.roomById.get(id); }
  getRoomByName(name) { return this.stmt.roomByName.get(name); }
  listRoomsForUser(userId) { return this.stmt.roomsForUser.all(userId); }
  listMembers(roomId) { return this.stmt.membersOfRoom.all(roomId); }

  joinRoom(roomId, userId) {
    this.stmt.upsertMember.run(roomId, userId, 'member', now());
    return this.stmt.member.get(roomId, userId);
  }

  getMember(roomId, userId) { return this.stmt.member.get(roomId, userId); }

  /** 设置禁言截止时间（0 表示解除禁言） */
  setMuted(roomId, userId, mutedUntil) {
    this.stmt.setMuted.run(mutedUntil, roomId, userId);
    return this.stmt.member.get(roomId, userId);
  }

  // ---------- 消息 ----------

  /**
   * 幂等写入消息。
   * 返回 { message, duplicate }：
   *  - duplicate=false：新消息，已分配 seq 并落库（调用方负责广播）；
   *  - duplicate=true ：同 clientMsgId 的消息已存在，直接返回原消息（调用方只回 ACK，不再广播）。
   */
  insertMessage({ roomId, clientMsgId, senderId, content }) {
    return this._tx(() => {
      const existing = this.stmt.msgByClientId.get(roomId, senderId, clientMsgId);
      if (existing) return { message: present(existing), duplicate: true };

      const { last_seq: seq } = this.stmt.bumpSeq.get(roomId);
      const ts = now();
      this.stmt.insertMsg.run(roomId, seq, clientMsgId, senderId, content, ts);
      const message = this.stmt.msgByClientId.get(roomId, senderId, clientMsgId);
      return { message: present(message), duplicate: false };
    });
  }

  /** 取单条消息的当前快照（含编辑/撤回状态），不存在返回 null */
  getMessage(roomId, seq) {
    const m = this.stmt.msgBySeq.get(roomId, seq);
    return m ? present(m) : null;
  }

  /**
   * 编辑消息（CAS）。权限由调用方先判断，这里只做状态机校验。
   * opId 命中已有事件时视为操作重试，返回原事件与当前快照（不再次广播）。
   * 返回 status：
   *  - ok         编辑已提交：{ message, event }
   *  - duplicate  同 opId 的操作已完成：{ message, event }
   *  - not_found  消息不存在
   *  - revoked    消息已撤回（终态，拒绝编辑）
   *  - conflict   版本不匹配：{ current }（当前快照，供调用方回传客户端刷新）
   */
  editMessage({ roomId, seq, content, expectedVersion, editorId, opId, reason = null }) {
    return this._tx(() => {
      if (opId) {
        const dup = this.stmt.eventByOp.get(roomId, editorId, opId);
        if (dup) return { status: 'duplicate', event: dup, message: this.getMessage(roomId, seq) };
      }

      const raw = this.stmt.rawMsgBySeq.get(roomId, seq);
      if (!raw) return { status: 'not_found' };
      if (raw.revokedAt) return { status: 'revoked', message: this.getMessage(roomId, seq) };
      if (raw.version !== expectedVersion) {
        return { status: 'conflict', current: this.getMessage(roomId, seq) };
      }

      const before = this.stmt.msgBySeq.get(roomId, seq);
      const editedAt = now();
      const r = this.stmt.casEdit.run(content, editedAt, roomId, seq, expectedVersion);
      if (r.changes === 0) {
        // 极端竞争下事务内仍被抢先（理论上不会发生：单写者事务），按冲突处理
        return { status: 'conflict', current: this.getMessage(roomId, seq) };
      }
      const { last_event_rev: rev } = this.stmt.bumpEventRev.get(roomId);
      const newVersion = expectedVersion + 1;
      this.stmt.insertEvent.run(
        roomId, rev, seq, 'edit', editorId, opId, reason,
        newVersion, editedAt, before.content, content, editedAt
      );
      return {
        status: 'ok',
        message: this.getMessage(roomId, seq),
        event: this.stmt.eventsAfter.all(roomId, rev - 1, 1)[0],
      };
    });
  }

  /**
   * 撤回消息（终态）。权限（本人/管理员）由调用方判断。
   * 返回 status：
   *  - ok       撤回已提交：{ message, event }
   *  - duplicate 同 opId 的操作已完成：{ message, event }
   *  - already  消息此前已被撤回：{ message }（幂等成功，不再次广播）
   *  - not_found
   */
  revokeMessage({ roomId, seq, actorId, opId, reason = null }) {
    return this._tx(() => {
      if (opId) {
        const dup = this.stmt.eventByOp.get(roomId, actorId, opId);
        if (dup) return { status: 'duplicate', event: dup, message: this.getMessage(roomId, seq) };
      }

      const raw = this.stmt.rawMsgBySeq.get(roomId, seq);
      if (!raw) return { status: 'not_found' };
      if (raw.revokedAt) return { status: 'already', message: this.getMessage(roomId, seq) };

      const before = this.stmt.msgBySeq.get(roomId, seq);
      const revokedAt = now();
      const r = this.stmt.revoke.run(revokedAt, actorId, reason, roomId, seq);
      if (r.changes === 0) return { status: 'already', message: this.getMessage(roomId, seq) };
      const { last_event_rev: rev } = this.stmt.bumpEventRev.get(roomId);
      this.stmt.insertEvent.run(
        roomId, rev, seq, 'revoke', actorId, opId, reason,
        raw.version, null, before.content, null, revokedAt
      );
      return {
        status: 'ok',
        message: this.getMessage(roomId, seq),
        event: this.stmt.eventsAfter.all(roomId, rev - 1, 1)[0],
      };
    });
  }

  /** 断线补发：取 seq > afterSeq 的消息快照（升序，最多 limit 条） */
  getMessagesAfter(roomId, afterSeq, limit) {
    return this.stmt.msgsAfter.all(roomId, afterSeq, limit).map(present);
  }

  /** 历史翻页：取 seq < beforeSeq 的消息，返回时按升序排列 */
  getMessagesBefore(roomId, beforeSeq, limit) {
    return this.stmt.msgsBefore.all(roomId, beforeSeq, limit).reverse().map(present);
  }

  // ---------- 事件流 ----------

  /** 断线补发：取 rev > afterRev 的编辑/撤回事件（升序） */
  getEventsAfter(roomId, afterRev, limit) {
    return this.stmt.eventsAfter.all(roomId, afterRev, limit);
  }

  /** 事件流向后翻页（预留，当前补发一次性追平） */
  getEventsBefore(roomId, beforeRev, limit) {
    return this.stmt.eventsBefore.all(roomId, beforeRev, limit).reverse();
  }

  /** 单条消息的完整审计记录（所有编辑/撤回事件，按 rev 升序） */
  getAuditTrail(roomId, seq) {
    return this.stmt.eventsForMsg.all(roomId, seq);
  }

  // ---------- 游标 ----------

  saveCursor(roomId, userId, lastAckSeq, lastAckRev = 0) {
    this.stmt.upsertCursor.run(roomId, userId, lastAckSeq, lastAckRev, now());
  }

  getCursor(roomId, userId) {
    const row = this.stmt.cursor.get(roomId, userId);
    return row ? { lastAckSeq: row.lastAckSeq, lastAckRev: row.lastAckRev } : { lastAckSeq: 0, lastAckRev: 0 };
  }

  close() {
    try { this.db.exec('PRAGMA wal_checkpoint(TRUNCATE)'); } catch { /* 内存库无 WAL */ }
    this.db.close();
  }
}

module.exports = { ChatDB };
