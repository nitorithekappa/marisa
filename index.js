require('dotenv').config();
const {
  Client,
  GatewayIntentBits,
  EmbedBuilder,
  SlashCommandBuilder,
  Events,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  MessageFlags,
  PermissionFlagsBits,
} = require('discord.js');
const fs = require('fs');
const Database = require('better-sqlite3');
const cron = require('node-cron');

// ───────────── Configuração ─────────────
const TOKEN = process.env.TOKEN;
const GUILD_ID = process.env.GUILD_ID; // servidor onde os comandos serão registrados
const RANKING_CHANNEL_ID = process.env.RANKING_CHANNEL_ID; // chat do ranking semanal
const MIN_MEMBERS = parseInt(process.env.MIN_MEMBERS || '2', 10); // pessoas mínimas para a sessão contar
const WEEK_RESET_DAY = parseInt(process.env.WEEK_RESET_DAY || '4', 10); // 0=domingo ... 4=quinta
const WEEK_RESET_HOUR = parseInt(process.env.WEEK_RESET_HOUR || '10', 10); // semana vira às 10:00
const RANKING_CRON = `0 ${WEEK_RESET_HOUR} * * ${WEEK_RESET_DAY}`; // envio do ranking semanal
const TIMEZONE = process.env.TIMEZONE || 'America/Sao_Paulo';
const SHOW_CHANNEL = process.env.SHOW_CHANNEL === 'true'; // mostra o nome do canal no ranking
const IGNORED_CHANNEL_IDS = new Set(
  (process.env.IGNORED_CHANNEL_IDS || '').split(',').map((id) => id.trim()).filter(Boolean)
); // canais de voz que nunca contam (separados por vírgula)
const TOP_WEEKLY = 10;
const MIN_SESSION_MS = 60 * 1000; // ignora sessões com menos de 1 minuto
const MIN_MEMBER_MINUTES = parseInt(process.env.MIN_MEMBER_MINUTES || '30', 10);
const MIN_MEMBER_MS = MIN_MEMBER_MINUTES * 60 * 1000; // tempo mínimo de cada pessoa na call para contar
const END_GRACE_MINUTES = parseInt(process.env.END_GRACE_MINUTES || '5', 10);
const END_GRACE_MS = END_GRACE_MINUTES * 60 * 1000; // call só termina após ficar esse tempo com menos de MIN_MEMBERS
const RESUME_WINDOW_MS = 5 * 60 * 1000; // retoma sessão se o bot voltou em até 5 min

if (!TOKEN) {
  console.error('Defina TOKEN no arquivo .env');
  process.exit(1);
}

// ───────────── Banco de dados ─────────────
const db = new Database('ranking.db');
db.exec(`
  CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    start_ms INTEGER NOT NULL,
    end_ms INTEGER NOT NULL,
    duration_ms INTEGER NOT NULL,
    members TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_sessions_rank ON sessions (guild_id, duration_ms DESC);

  CREATE TABLE IF NOT EXISTS active_sessions (
    channel_id TEXT PRIMARY KEY,
    guild_id TEXT NOT NULL,
    start_ms INTEGER NOT NULL,
    members TEXT NOT NULL,
    heartbeat_ms INTEGER NOT NULL
  );
`);

const insertSession = db.prepare(
  `INSERT INTO sessions (guild_id, channel_id, start_ms, end_ms, duration_ms, members)
   VALUES (?, ?, ?, ?, ?, ?)`
);
const upsertActive = db.prepare(
  `INSERT INTO active_sessions (channel_id, guild_id, start_ms, members, heartbeat_ms)
   VALUES (?, ?, ?, ?, ?)
   ON CONFLICT(channel_id) DO UPDATE SET members = excluded.members, heartbeat_ms = excluded.heartbeat_ms`
);
const deleteActive = db.prepare(`DELETE FROM active_sessions WHERE channel_id = ?`);
const deleteGuildSessions = db.prepare(`DELETE FROM sessions WHERE guild_id = ?`);
const touchActive = db.prepare(`UPDATE active_sessions SET heartbeat_ms = ? WHERE channel_id = ?`);
const topSessions = db.prepare(
  `SELECT channel_id, duration_ms, members FROM sessions
   WHERE guild_id = ? ORDER BY duration_ms DESC LIMIT ?`
);
// conta só a parte de cada sessão que caiu dentro do período [from, to)
const topSessionsBetween = db.prepare(
  `SELECT channel_id, members, MIN(end_ms, @to) - MAX(start_ms, @from) AS duration_ms
   FROM sessions
   WHERE guild_id = @guild AND end_ms > @from AND start_ms < @to
   ORDER BY duration_ms DESC LIMIT @limit`
);

// ───────────── Sessões em memória ─────────────
// channelId -> { guildId, start, members:Map<userId, { ms, since }>, belowSince }
// ms = tempo acumulado na call; since = desde quando está na call agora (null se saiu)
// belowSince = desde quando a call está com menos de MIN_MEMBERS (null se está cheia)
const active = new Map();

function serializeMembers(members) {
  return JSON.stringify(Object.fromEntries(members));
}

function parseMembers(json, fallbackStart) {
  const data = JSON.parse(json);
  // formato antigo: lista de ids, sem tempo por pessoa
  if (Array.isArray(data)) return new Map(data.map((id) => [id, { ms: 0, since: fallbackStart }]));
  return new Map(Object.entries(data));
}

function memberTime(m, at) {
  return m.ms + (m.since !== null ? Math.max(0, at - m.since) : 0);
}

// ids de quem ficou pelo menos MIN_MEMBER_MS na call, do maior tempo para o menor
function qualifiedMembers(s, at) {
  return [...s.members]
    .map(([id, m]) => [id, memberTime(m, at)])
    .filter(([, ms]) => ms >= MIN_MEMBER_MS)
    .sort((a, b) => b[1] - a[1])
    .map(([id]) => id);
}

function persistActive(channelId) {
  const s = active.get(channelId);
  if (!s) return;
  upsertActive.run(channelId, s.guildId, s.start, serializeMembers(s.members), Date.now());
}

function endSession(channelId, endMs = Date.now()) {
  const s = active.get(channelId);
  if (!s) return;
  const duration = endMs - s.start;
  const members = qualifiedMembers(s, endMs);
  if (duration >= MIN_SESSION_MS && members.length >= MIN_MEMBERS) {
    insertSession.run(s.guildId, channelId, s.start, endMs, duration, JSON.stringify(members));
  }
  active.delete(channelId);
  deleteActive.run(channelId);
}

function updateChannel(channel) {
  if (!channel || !channel.isVoiceBased()) return;
  if (channel.id === channel.guild.afkChannelId) return; // ignora canal AFK
  if (IGNORED_CHANNEL_IDS.has(channel.id)) return;

  const humans = channel.members.filter((m) => !m.user.bot);
  let s = active.get(channel.id);

  const full = humans.size >= MIN_MEMBERS;
  if (!s && !full) return;

  const now = Date.now();
  if (!s) {
    s = { guildId: channel.guild.id, start: now, members: new Map(), belowSince: null };
    active.set(channel.id, s);
  }
  // abaixo do mínimo: não encerra na hora, espera END_GRACE_MS (ver checkGraceExpired)
  if (full) s.belowSince = null;
  else if (s.belowSince == null) s.belowSince = now;

  humans.forEach((m) => {
    const entry = s.members.get(m.id);
    if (!entry) s.members.set(m.id, { ms: 0, since: now });
    else if (entry.since === null) entry.since = now; // voltou para a call
  });
  for (const [id, entry] of s.members) {
    if (entry.since !== null && !humans.has(id)) {
      entry.ms += now - entry.since; // saiu da call
      entry.since = null;
    }
  }
  persistActive(channel.id);
}

// Encerra as calls que ficaram abaixo do mínimo por mais de END_GRACE_MS.
// A call termina no momento em que caiu abaixo do mínimo.
function checkGraceExpired(now = Date.now()) {
  for (const [channelId, s] of active) {
    if (s.belowSince != null && now - s.belowSince >= END_GRACE_MS) endSession(channelId, s.belowSince);
  }
}

// ───────────── Ranking ─────────────
function formatDuration(ms) {
  const totalMin = Math.floor(ms / 60000);
  const h = Math.floor(totalMin / 60);
  const m = totalMin % 60;
  return h > 0 ? `${h}h${String(m).padStart(2, '0')}m` : `${m}m`;
}

function formatMembers(ids) {
  const shown = ids.slice(0, 6).map((id) => `<@${id}>`).join(', ');
  const extra = ids.length - 6;
  return extra > 0 ? `${shown} +${extra}` : shown;
}

// Início da semana atual: último WEEK_RESET_DAY às WEEK_RESET_HOUR no fuso TIMEZONE
function weekStart(nowMs = Date.now()) {
  const parts = Object.fromEntries(
    new Intl.DateTimeFormat('en-US', {
      timeZone: TIMEZONE,
      year: 'numeric', month: 'numeric', day: 'numeric',
      hour: 'numeric', minute: 'numeric', second: 'numeric',
      weekday: 'short', hourCycle: 'h23',
    })
      .formatToParts(new Date(nowMs))
      .map((p) => [p.type, p.value])
  );
  const weekday = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'].indexOf(parts.weekday);
  const y = +parts.year, mo = +parts.month - 1, d = +parts.day;
  const localAsUtc = Date.UTC(y, mo, d, +parts.hour, +parts.minute, +parts.second);
  const offset = localAsUtc - Math.floor(nowMs / 1000) * 1000; // diferença do fuso para UTC

  let daysBack = (weekday - WEEK_RESET_DAY + 7) % 7;
  if (daysBack === 0 && +parts.hour < WEEK_RESET_HOUR) daysBack = 7;
  return Date.UTC(y, mo, d - daysBack, WEEK_RESET_HOUR) - offset;
}

function formatDate(ms) {
  return new Date(ms).toLocaleString('pt-BR', {
    timeZone: TIMEZONE, day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit',
  });
}

// period: undefined = all time, ou { from, to } em ms
function buildRankingEmbed(guildId, limit = 10, period) {
  const rows = period
    ? topSessionsBetween.all({ guild: guildId, from: period.from, to: period.to, limit })
    : topSessions.all(guildId, limit);
  const entries = rows.map((r) => ({
    channelId: r.channel_id,
    duration: r.duration_ms,
    members: JSON.parse(r.members),
    live: false,
  }));

  // inclui calls em andamento (de todos os canais)
  const now = Date.now();
  for (const [channelId, s] of active.entries()) {
    if (s.guildId !== guildId) continue;
    const start = period ? Math.max(s.start, period.from) : s.start;
    const end = period ? Math.min(now, period.to) : now;
    const members = qualifiedMembers(s, now);
    if (end - start <= 0 || members.length < MIN_MEMBERS) continue;
    entries.push({ channelId, duration: end - start, members, live: true });
  }

  entries.sort((a, b) => b.duration - a.duration);
  const top = entries.slice(0, limit);

  const medals = ['🥇', '🥈', '🥉'];
  const description = top.length
    ? top
        .map((e, i) => {
          const pos = medals[i] || `**${i + 1}.**`;
          const live = e.live ? ' 🔴 *ao vivo*' : '';
          const chName = client.channels.cache.get(e.channelId)?.name ?? 'canal removido';
          const where = SHOW_CHANNEL ? ` — ${chName}` : '';
          return `${pos} ${formatMembers(e.members)}${where} — **${formatDuration(e.duration)}**${live}`;
        })
        .join('\n')
    : period
      ? 'Nenhuma call registrada nesta semana.'
      : 'Nenhuma call registrada ainda.';

  const footer = `Conta apenas quem ficou ${MIN_MEMBER_MINUTES}+ min em calls com ${MIN_MEMBERS}+ pessoas`;
  return new EmbedBuilder()
    .setTitle(period ? '📅 Ranking de calls (semanal)' : '🏆 Ranking de calls (all time)')
    .setDescription(description)
    .setFooter({
      text: period ? `${formatDate(period.from)} até ${formatDate(period.to)} • ${footer}` : footer,
    })
    .setColor(period ? 0x57f287 : 0x5865f2)
    .setTimestamp();
}

async function sendWeeklyRanking() {
  try {
    const channel = await client.channels.fetch(RANKING_CHANNEL_ID);
    if (!channel || !channel.isTextBased()) throw new Error('Canal inválido');
    // semana que acabou de fechar: da virada anterior até agora
    const now = Date.now();
    const period = { from: weekStart(now - 60 * 1000), to: now };
    await channel.send({
      embeds: [buildRankingEmbed(channel.guild.id, TOP_WEEKLY, period)],
      allowedMentions: { parse: [] }, // menciona sem notificar
    });
  } catch (err) {
    console.error('Erro ao enviar ranking semanal:', err);
  }
}

// Apaga o ranking do servidor (all time e semanal) e recomeça as calls em andamento do zero.
// Antes, salva uma cópia do banco em backups/ para poder desfazer.
async function resetRanking(guild) {
  fs.mkdirSync('backups', { recursive: true });
  const backupPath = `backups/ranking-${new Date().toISOString().replace(/[:.]/g, '-')}.db`;
  await db.backup(backupPath);

  const { changes } = deleteGuildSessions.run(guild.id);
  for (const [channelId, s] of active) {
    if (s.guildId !== guild.id) continue;
    active.delete(channelId);
    deleteActive.run(channelId);
  }
  guild.channels.cache.filter((c) => c.isVoiceBased()).forEach(updateChannel);
  return { removed: changes, backupPath };
}

// ───────────── Cliente Discord ─────────────
const client = new Client({
  intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildVoiceStates],
});

client.once(Events.ClientReady, async () => {
  console.log(`Logado como ${client.user.tag}`);

  // Registra os comandos /ranking e /resetar-ranking
  const rankingCommand = new SlashCommandBuilder()
    .setName('ranking')
    .setDescription('Mostra o ranking de maior tempo em call')
    .addStringOption((o) =>
      o
        .setName('periodo')
        .setDescription('Semana atual ou all time (padrão: all time)')
        .addChoices({ name: 'semanal', value: 'semanal' }, { name: 'all time', value: 'alltime' })
    )
    .addIntegerOption((o) =>
      o.setName('limite').setDescription('Quantas posições mostrar (1-25)').setMinValue(1).setMaxValue(25)
    )
    .toJSON();
  const resetCommand = new SlashCommandBuilder()
    .setName('resetar-ranking')
    .setDescription('Apaga todo o ranking (all time e semanal) deste servidor')
    .setDefaultMemberPermissions(PermissionFlagsBits.Administrator)
    .toJSON();
  const commands = [rankingCommand, resetCommand];

  if (GUILD_ID) {
    await client.guilds.cache.get(GUILD_ID)?.commands.set(commands);
  } else {
    await client.application.commands.set(commands);
  }

  // Recupera sessões que estavam abertas quando o bot caiu
  const orphans = db.prepare('SELECT * FROM active_sessions').all();
  for (const o of orphans) {
    active.set(o.channel_id, {
      guildId: o.guild_id,
      start: o.start_ms,
      members: parseMembers(o.members, o.start_ms),
      belowSince: null,
    });
    const stale = Date.now() - o.heartbeat_ms > RESUME_WINDOW_MS;
    const channelExists = client.channels.cache.has(o.channel_id);
    if (stale || !channelExists) endSession(o.channel_id, o.heartbeat_ms);
  }

  // Varre as calls atuais (retoma as retomadas e inicia as novas)
  for (const guild of client.guilds.cache.values()) {
    guild.channels.cache.filter((c) => c.isVoiceBased()).forEach(updateChannel);
  }

  // Heartbeat: marca que o bot está vivo (para recuperação em caso de queda)
  setInterval(() => {
    const now = Date.now();
    for (const channelId of active.keys()) touchActive.run(now, channelId);
  }, 60 * 1000);

  // Encerra calls que ficaram abaixo do mínimo por mais que a tolerância
  setInterval(checkGraceExpired, 15 * 1000);

  // Ranking semanal automático
  if (RANKING_CHANNEL_ID) {
    cron.schedule(RANKING_CRON, sendWeeklyRanking, { timezone: TIMEZONE });
    console.log(`Ranking semanal agendado: "${RANKING_CRON}" (${TIMEZONE})`);
  } else {
    console.warn('RANKING_CHANNEL_ID não definido: ranking semanal desativado.');
  }
});

client.on(Events.VoiceStateUpdate, (oldState, newState) => {
  updateChannel(oldState.channel);
  if (newState.channelId !== oldState.channelId) updateChannel(newState.channel);
});

async function handleRanking(interaction) {
  const limit = interaction.options.getInteger('limite') ?? 10;
  const weekly = interaction.options.getString('periodo') === 'semanal';
  const period = weekly ? { from: weekStart(), to: Date.now() } : undefined;
  await interaction.reply({
    embeds: [buildRankingEmbed(interaction.guildId, limit, period)],
    allowedMentions: { parse: [] },
  });
}

async function handleReset(interaction) {
  if (!interaction.inGuild() || !interaction.memberPermissions?.has(PermissionFlagsBits.Administrator)) {
    await interaction.reply({ content: 'Só administradores podem resetar o ranking.', flags: MessageFlags.Ephemeral });
    return;
  }

  const buttons = new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('reset-confirm').setLabel('Apagar ranking').setStyle(ButtonStyle.Danger),
    new ButtonBuilder().setCustomId('reset-cancel').setLabel('Cancelar').setStyle(ButtonStyle.Secondary)
  );
  const response = await interaction.reply({
    content:
      '⚠️ Isso apaga **todo o ranking** deste servidor (all time e semanal) e recomeça do zero as calls em andamento. Tem certeza?',
    components: [buttons],
    flags: MessageFlags.Ephemeral,
    withResponse: true,
  });

  let click;
  try {
    click = await response.resource.message.awaitMessageComponent({
      filter: (i) => i.user.id === interaction.user.id,
      time: 30_000,
    });
  } catch {
    await interaction.editReply({ content: 'Tempo esgotado, nada foi apagado.', components: [] });
    return;
  }

  if (click.customId !== 'reset-confirm') {
    await click.update({ content: 'Cancelado, nada foi apagado.', components: [] });
    return;
  }

  try {
    const { removed, backupPath } = await resetRanking(interaction.guild);
    console.log(`Ranking resetado por ${interaction.user.tag} (${removed} calls). Backup: ${backupPath}`);
    await click.update({
      content: `🗑️ Ranking resetado: ${removed} call(s) apagada(s). Um backup foi salvo no servidor do bot.`,
      components: [],
    });
  } catch (err) {
    console.error('Erro ao resetar ranking:', err);
    await click.update({ content: 'Erro ao resetar o ranking, nada foi apagado.', components: [] });
  }
}

client.on(Events.InteractionCreate, async (interaction) => {
  if (!interaction.isChatInputCommand()) return;
  if (interaction.commandName === 'ranking') await handleRanking(interaction);
  else if (interaction.commandName === 'resetar-ranking') await handleReset(interaction);
});

client.login(TOKEN);
