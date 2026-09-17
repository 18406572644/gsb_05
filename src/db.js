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
 * 编辑 / 撤回与审计：
 * - messages.version 为消息状态版本号：创建为 1，每次编辑或撤回 +1。编辑必须携带
 *   expectedVersion，事务内比对不一致即拒绝（乐观锁），杜绝迟到的旧操作覆盖新状态。
 * - message_revisions 为只增审计表：创建/每次编辑/撤回各落一行（内容快照、操作人、
 *   原因、时间），撤回不清空 messages.content，审计链路完整。
 * - message_events 是房间内第二条全序事件流（rooms.last_event_seq 计数），记录编辑/
 *   撤回。断线重连时与消息流分别按水位补发，保证「离线期间旧消息被改动」也能收敛。
 * - 撤回后 messages.recalled_at>0，读取层（补发/翻页/单条）统一把 content 投影为
 *   空串并带 recalled 占位，历史记录里位置、序号、操作时间全部保留。
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
  id             TEXT PRIMARY KEY,
  name           TEXT NOT NULL UNIQUE,
  created_by     TEXT NOT NULL REFERENCES users(id),
  created_at     INTEGER NOT NULL,
  last_seq       INTEGER NOT NULL DEFAULT 0,
  last_event_seq INTEGER NOT NULL DEFAULT 0
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
  version       INTEGER NOT NULL DEFAULT 1,   -- 状态版本：创建=1，每次编辑/撤回 +1
  edited_at     INTEGER NOT NULL DEFAULT 0,  -- 最近一次编辑时间
  recalled_at   INTEGER NOT NULL DEFAULT 0,  -- 撤回时间（0=未撤回），撤回后仍保留行
  recalled_by   TEXT,                        -- 撤回人（本人或管理员）
  recall_reason TEXT,                        -- 撤回/审核原因
  PRIMARY KEY (room_id, seq),
  UNIQUE (room_id, sender_id, client_msg_id)  -- 幂等键
);

-- 只增审计表：消息每个版本一行（创建/编辑/撤回）
CREATE TABLE IF NOT EXISTS message_revisions (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  room_id    TEXT NOT NULL,
  msg_seq    INTEGER NOT NULL,
  version    INTEGER NOT NULL,
  action     TEXT NOT NULL CHECK (action IN ('create','edit','recall')),
  content    TEXT,                 -- create/edit 的内容快照；recall 为 NULL
  actor_id   TEXT NOT NULL,
  actor_name TEXT NOT NULL,        -- 冗余操作人名称，审计记录自包含
  reason     TEXT,                 -- 撤回原因（编辑也可附带）
  ts         INTEGER NOT NULL,
  UNIQUE (room_id, msg_seq, version)
);

-- 房间内编辑/撤回事件流（独立于消息 seq 的全序流水，断线补发依据）
CREATE TABLE IF NOT EXISTS message_events (
  room_id   TEXT NOT NULL REFERENCES rooms(id),
  event_seq INTEGER NOT NULL,
  msg_seq   INTEGER NOT NULL,
  version   INTEGER NOT NULL,
  action    TEXT NOT NULL CHECK (action IN ('edit','recall')),
  ts        INTEGER NOT NULL,
  PRIMARY KEY (room_id, event_seq)
);

-- 服务端保存的每用户每房间已确认游标（断线补发的兜底依据）
CREATE TABLE IF NOT EXISTS cursors (
  room_id            TEXT NOT NULL REFERENCES rooms(id),
  user_id            TEXT NOT NULL REFERENCES users(id),
  last_ack_seq       INTEGER NOT NULL DEFAULT 0,
  last_ack_event_seq INTEGER NOT NULL DEFAULT 0,
  updated_at         INTEGER NOT NULL,
  PRIMARY KEY (room_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_events_room ON message_events (room_id, event_seq);
CREATE INDEX IF NOT EXISTS idx_revisions_room ON message_revisions (room_id, msg_seq, version);
`;

const MSG_SELECT = `
  SELECT m.room_id AS roomId, m.seq, m.client_msg_id AS clientMsgId,
         m.sender_id AS "from", u.name AS fromName,
         m.content AS rawContent, m.ts, m.version,
         m.edited_at AS editedAt, m.recalled_at AS recalledAt,
         m.recalled_by AS recalledBy, m.recall_reason AS recallReason,
         ru.name AS recalledByName
    FROM messages m
    JOIN users u ON u.id = m.sender_id
    LEFT JOIN users ru ON ru.id = m.recalled_by
`;

/** 数据库行 → 对外消息视图：撤回后屏蔽内容，保留占位与操作元数据 */
function projectMessage(m) {
  const recalled = m.recalledAt > 0;
  return {
    roomId: m.roomId,
    seq: m.seq,
    clientMsgId: m.clientMsgId,
    from: m.from,
    fromName: m.fromName,
    content: recalled ? '' : m.rawContent,
    ts: m.ts,
    version: m.version,
    editedAt: m.editedAt,
    recalled,
    recalledAt: recalled ? m.recalledAt : 0,
    recalledBy: recalled ? m.recalledBy : null,
    recalledByName: recalled ? m.recalledByName : null,
    recallReason: recalled ? m.recallReason : null,
  };
}

class ChatDB {
  constructor(dbPath) {
    this.db = new DatabaseSync(dbPath);
    this.db.exec(SCHEMA);
    this._migrate();
    this._prepare();
  }

  /** 旧库补列（CREATE TABLE IF NOT EXISTS 不会改变已有表结构），并回填审计基线 */
  _migrate() {
    const columnsOf = (table) => new Set(this.db.prepare(`PRAGMA table_info(${table})`).all().map((c) => c.name));
    const addColumn = (table, column, ddl) => {
      if (!columnsOf(table).has(column)) {
        this.db.exec(`ALTER TABLE ${table} ADD COLUMN ${ddl}`);
        return true;
      }
      return false;
    };
    addColumn('rooms', 'last_event_seq', 'last_event_seq INTEGER NOT NULL DEFAULT 0');
    const messagesMigrated = addColumn('messages', 'version', 'version INTEGER NOT NULL DEFAULT 1');
    addColumn('messages', 'edited_at', 'edited_at INTEGER NOT NULL DEFAULT 0');
    addColumn('messages', 'recalled_at', 'recalled_at INTEGER NOT NULL DEFAULT 0');
    addColumn('messages', 'recalled_by', 'recalled_by TEXT');
    addColumn('messages', 'recall_reason', 'recall_reason TEXT');
    addColumn('cursors', 'last_ack_event_seq', 'last_ack_event_seq INTEGER NOT NULL DEFAULT 0');

    // 旧库首次升级：为存量消息补 version=1 的 create 审计行（只增审计表已在建表时创建）
    if (messagesMigrated) {
      this.db.exec(`
        INSERT INTO message_revisions (room_id, msg_seq, version, action, content, actor_id, actor_name, reason, ts)
        SELECT m.room_id, m.seq, 1, 'create', m.content, m.sender_id, u.name, NULL, m.ts
          FROM messages m JOIN users u ON u.id = m.sender_id
         WHERE NOT EXISTS (
           SELECT 1 FROM message_revisions r
            WHERE r.room_id = m.room_id AND r.msg_seq = m.seq AND r.version = 1
         )`);
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
        `SELECT r.id, r.name, r.last_seq AS lastSeq, r.last_event_seq AS lastEventSeq,
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
      bumpSeq: d.prepare('UPDATE rooms SET last_seq = last_seq + 1 WHERE id = ? RETURNING last_seq'),
      bumpEventSeq: d.prepare(
        'UPDATE rooms SET last_event_seq = last_event_seq + 1 WHERE id = ? RETURNING last_event_seq'
      ),
      insertMsg: d.prepare(
        `INSERT INTO messages (room_id, seq, client_msg_id, sender_id, content, ts)
         VALUES (?, ?, ?, ?, ?, ?)`
      ),
      insertRevision: d.prepare(
        `INSERT INTO message_revisions (room_id, msg_seq, version, action, content, actor_id, actor_name, reason, ts)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
      ),
      insertEvent: d.prepare(
        'INSERT INTO message_events (room_id, event_seq, msg_seq, version, action, ts) VALUES (?, ?, ?, ?, ?, ?)'
      ),

      // —— 编辑 / 撤回（事务内先读后写，依赖行当前版本做乐观锁）——
      rawMsgForUpdate: d.prepare(
        `SELECT seq, sender_id AS senderId, version, recalled_at AS recalledAt
           FROM messages WHERE room_id = ? AND seq = ?`
      ),
      applyEdit: d.prepare(
        'UPDATE messages SET content = ?, version = ?, edited_at = ? WHERE room_id = ? AND seq = ?'
      ),
      applyRecall: d.prepare(
        `UPDATE messages SET recalled_at = ?, recalled_by = ?, recall_reason = ?, version = ?
          WHERE room_id = ? AND seq = ?`
      ),
      lastRecallEvent: d.prepare(
        `SELECT event_seq AS eventSeq FROM message_events
          WHERE room_id = ? AND msg_seq = ? AND action = 'recall'
          ORDER BY event_seq DESC LIMIT 1`
      ),

      // —— 消息读取 ——
      msgsAfter: d.prepare(`${MSG_SELECT} WHERE m.room_id = ? AND m.seq > ? ORDER BY m.seq LIMIT ?`),
      msgsBefore: d.prepare(
        `${MSG_SELECT} WHERE m.room_id = ? AND m.seq < ? ORDER BY m.seq DESC LIMIT ?`
      ),

      // —— 事件补发：关联到事件发生时的版本快照（revisions），并带上消息当前撤回状态 ——
      eventsAfter: d.prepare(
        `SELECT e.event_seq AS eventSeq, e.msg_seq AS msgSeq, e.version, e.action,
                e.ts AS eventTs,
                m.client_msg_id AS clientMsgId, m.sender_id AS "from", u.name AS fromName,
                m.recalled_at AS recalledAt,
                r.content AS revContent, r.reason AS revReason, r.ts AS actionTs,
                r.actor_id AS actorId, ra.name AS actorName
           FROM message_events e
           JOIN messages m ON m.room_id = e.room_id AND m.seq = e.msg_seq
           JOIN users u ON u.id = m.sender_id
           LEFT JOIN message_revisions r
                  ON r.room_id = e.room_id AND r.msg_seq = e.msg_seq AND r.version = e.version
           LEFT JOIN users ra ON ra.id = r.actor_id
          WHERE e.room_id = ? AND e.event_seq > ?
          ORDER BY e.event_seq LIMIT ?`
      ),

      // —— 审计 ——
      revisions: d.prepare(
        `SELECT version, action, content, actor_id AS actorId, actor_name AS actorName,
                reason, ts
           FROM message_revisions WHERE room_id = ? AND msg_seq = ? ORDER BY version`
      ),

      // —— 游标 ——
      upsertCursor: d.prepare(
        `INSERT INTO cursors (room_id, user_id, last_ack_seq, last_ack_event_seq, updated_at)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT (room_id, user_id)
         DO UPDATE SET last_ack_seq = MAX(last_ack_seq, excluded.last_ack_seq),
                       last_ack_event_seq = MAX(last_ack_event_seq, excluded.last_ack_event_seq),
                       updated_at = excluded.updated_at`
      ),
      cursor: d.prepare(
        `SELECT last_ack_seq AS lastAckSeq, last_ack_event_seq AS lastAckEventSeq
           FROM cursors WHERE room_id = ? AND user_id = ?`
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
      if (existing) return { message: projectMessage(existing), duplicate: true };

      const { last_seq: seq } = this.stmt.bumpSeq.get(roomId);
      const ts = now();
      this.stmt.insertMsg.run(roomId, seq, clientMsgId, senderId, content, ts);
      const row = this.stmt.msgByClientId.get(roomId, senderId, clientMsgId);
      // 审计：版本 1 = 创建
      this.stmt.insertRevision.run(roomId, seq, 1, 'create', content, senderId, row.fromName, null, ts);
      return { message: projectMessage(row), duplicate: false };
    });
  }

  getMessage(roomId, seq) {
    const row = this.stmt.msgBySeq.get(roomId, seq);
    return row ? projectMessage(row) : null;
  }

  /**
   * 编辑消息（乐观锁 + 审计 + 事件，一个事务内完成）。
   * status:
   *  - ok           编辑成功，返回新消息视图与 eventSeq；
   *  - not_found    消息不存在；
   *  - conflict     expectedVersion 与当前版本不一致（并发编辑/撤回/重发迟到），current 为最新视图；
   *  - recalled     消息已撤回，不允许编辑。
   */
  editMessage({ roomId, seq, expectedVersion, content, actorId, reason = null }) {
    return this._tx(() => {
      const cur = this.stmt.rawMsgForUpdate.get(roomId, seq);
      if (!cur) return { status: 'not_found' };
      if (cur.recalledAt > 0) return { status: 'recalled', message: this.getMessage(roomId, seq) };
      if (cur.version !== expectedVersion) {
        return { status: 'conflict', current: this.getMessage(roomId, seq) };
      }

      const version = cur.version + 1;
      const ts = now();
      this.stmt.applyEdit.run(content, version, ts, roomId, seq);
      const { last_event_seq: eventSeq } = this.stmt.bumpEventSeq.get(roomId);
      this.stmt.insertEvent.run(roomId, eventSeq, seq, version, 'edit', ts);
      const actorName = this.stmt.userById.get(actorId)?.name || actorId;
      this.stmt.insertRevision.run(roomId, seq, version, 'edit', content, actorId, actorName, reason, ts);
      return { status: 'ok', message: this.getMessage(roomId, seq), eventSeq, ts };
    });
  }

  /**
   * 撤回消息（本人或管理员，权限由调用方判定；乐观锁 + 审计 + 事件，一个事务内完成）。
   * 重复撤回（同版本重试）幂等返回 already_recalled 与原事件流水号，不产生新版本。
   * status: ok / not_found / conflict / already_recalled。
   */
  recallMessage({ roomId, seq, expectedVersion, actorId, reason = null }) {
    return this._tx(() => {
      const cur = this.stmt.rawMsgForUpdate.get(roomId, seq);
      if (!cur) return { status: 'not_found' };
      if (cur.recalledAt > 0) {
        const last = this.stmt.lastRecallEvent.get(roomId, seq);
        return {
          status: 'already_recalled',
          message: this.getMessage(roomId, seq),
          eventSeq: last ? last.eventSeq : null,
        };
      }
      if (cur.version !== expectedVersion) {
        return { status: 'conflict', current: this.getMessage(roomId, seq) };
      }

      const version = cur.version + 1;
      const ts = now();
      this.stmt.applyRecall.run(ts, actorId, reason, version, roomId, seq);
      const { last_event_seq: eventSeq } = this.stmt.bumpEventSeq.get(roomId);
      this.stmt.insertEvent.run(roomId, eventSeq, seq, version, 'recall', ts);
      const actor = this.stmt.userById.get(actorId);
      this.stmt.insertRevision.run(
        roomId, seq, version, 'recall', null, actorId, actor?.name || actorId, reason, ts
      );
      return { status: 'ok', message: this.getMessage(roomId, seq), eventSeq, ts };
    });
  }

  /** 断线补发：取 seq > afterSeq 的消息（升序，最多 limit 条）；撤回消息以占位形式返回 */
  getMessagesAfter(roomId, afterSeq, limit) {
    return this.stmt.msgsAfter.all(roomId, afterSeq, limit).map(projectMessage);
  }

  /** 历史翻页：取 seq < beforeSeq 的消息，返回时按升序排列 */
  getMessagesBefore(roomId, beforeSeq, limit) {
    return this.stmt.msgsBefore.all(roomId, beforeSeq, limit).reverse().map(projectMessage);
  }

  /**
   * 状态事件补发：event_seq > afterEventSeq 的编辑/撤回事件（升序），事件流保持完整
   * （顺序与水位连续，客户端才能单调收敛）。但若消息当前已撤回，历史 edit 事件的内容
   * 置空——撤回后任何版本的正文都不再外泄，客户端随后收到 recall 事件收敛为占位。
   */
  getEventsAfter(roomId, afterEventSeq, limit) {
    return this.stmt.eventsAfter.all(roomId, afterEventSeq, limit).map((e) => ({
      eventSeq: e.eventSeq,
      msgSeq: e.msgSeq,
      version: e.version,
      action: e.action,
      ts: e.actionTs,
      clientMsgId: e.clientMsgId,
      from: e.from,
      fromName: e.fromName,
      content: e.action === 'edit' && e.recalledAt === 0 ? e.revContent : null,
      actorId: e.actorId,
      actorName: e.actorName,
      reason: e.revReason,
    }));
  }

  /** 审计轨迹：消息的全部版本（创建/编辑/撤回）。权限由调用方判定。 */
  listRevisions(roomId, seq) {
    if (!this.stmt.msgBySeq.get(roomId, seq)) return null;
    return this.stmt.revisions.all(roomId, seq);
  }

  // ---------- 游标 ----------

  saveCursor(roomId, userId, lastAckSeq, lastAckEventSeq = 0) {
    this.stmt.upsertCursor.run(roomId, userId, lastAckSeq, lastAckEventSeq, now());
  }

  getCursor(roomId, userId) {
    const row = this.stmt.cursor.get(roomId, userId);
    return {
      lastAckSeq: row ? row.lastAckSeq : 0,
      lastAckEventSeq: row ? row.lastAckEventSeq : 0,
    };
  }

  close() {
    try { this.db.exec('PRAGMA wal_checkpoint(TRUNCATE)'); } catch { /* 内存库无 WAL */ }
    this.db.close();
  }
}

module.exports = { ChatDB, projectMessage };
