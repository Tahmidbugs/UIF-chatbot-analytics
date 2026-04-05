const axios = require("axios");
const fs = require("fs");
const path = require("path");
const { BigQuery } = require("@google-cloud/bigquery");

require("dotenv").config({ path: path.join(__dirname, ".env") });

function resolveDryRunPath() {
  const explicit = process.env.ANALYTICS_OUTPUT_FILE?.trim();
  if (explicit) return path.resolve(process.cwd(), explicit);
  const dry = process.env.ANALYTICS_DRY_RUN;
  if (dry === "1" || /^true$/i.test(dry || "")) {
    return path.join(process.cwd(), "previous-dry-run.json");
  }
  return null;
}

const dryRunPath = resolveDryRunPath();

/** Max characters per cell when printing dry-run tables (full values still in JSON file). */
const DRY_RUN_TABLE_MAX_CELL = 80;

function truncateForTable(value) {
  if (value === undefined || value === null) return "";
  const s =
    typeof value === "object" ? JSON.stringify(value) : String(value);
  if (s.length <= DRY_RUN_TABLE_MAX_CELL) return s;
  return `${s.slice(0, DRY_RUN_TABLE_MAX_CELL - 1)}…`;
}

function rowsForDisplayTable(rows) {
  return rows.map((row) => {
    const out = {};
    for (const key of Object.keys(row)) {
      out[key] = truncateForTable(row[key]);
    }
    return out;
  });
}

function logDryRunWouldInsertTable(tableLabel, rows, opts = {}) {
  const n = rows.length;
  const sub = opts.subtitle ? ` (${opts.subtitle})` : "";
  console.log(`\n[dry-run] ${tableLabel}${sub} — would insert ${n} row(s)`);
  if (n === 0) return;
  console.table(rowsForDisplayTable(rows));
}

/** Full evaluation `reason` text (table cells are truncated). */
function logEvaluationRationales(evaluations) {
  for (const e of evaluations) {
    if (e.reason != null && String(e.reason).trim() !== "") {
      const label = e.name ? `[${e.name}] ` : "";
      console.log(
        `${label}rationale:\n${String(e.reason)
          .split("\n")
          .map((line) => `  ${line}`)
          .join("\n")}\n`
      );
    }
  }
}

/** Full JSON per turn: on by default in dry run; PRINT_ALL_TURNS=0|false off; PRINT_ALL_TURNS=1|true always on. */
function shouldPrintAllTurns() {
  if (/^0|false$/i.test(process.env.PRINT_ALL_TURNS || "")) return false;
  if (/^1|true$/i.test(process.env.PRINT_ALL_TURNS || "")) return true;
  return Boolean(dryRunPath);
}

function printAllTranscriptTurns(transcriptID, turns) {
  if (!shouldPrintAllTurns()) return;
  console.log(
    `\n========== ALL TURNS ${transcriptID} (count=${turns.length}) ==========`
  );
  turns.forEach((turn, i) => {
    console.log(
      `\n----- [${i}] type=${turn.type} format=${turn.format ?? ""} startTime=${turn.startTime ?? ""} -----`
    );
    console.log(JSON.stringify(turn, null, 2));
  });
  console.log(`\n========== END ${transcriptID} ==========\n`);
}

// Define API params
const projectID = "667becda461c4d81807fe278";
const authorizationToken = "VF.DM.66942097410275bab4e8a41e.Wb7qsO1heu6qOLCh";
// Voiceflow Analytics usage API v2 — https://docs.voiceflow.com/api-reference/usage/query-usage
const analyticsUsageUrl = "https://analytics-api.voiceflow.com/v2/query/usage";
// headers
const headers = {
  accept: "application/json",
  "content-type": "application/json",
  authorization: `${authorizationToken}`,
};

// ----- Voiceflow Analytics transcript (Search, GET /v1/transcript/{id}, End) -----
// GET transcript uses Search transcript `id` (not runtime v2 list _id).
// https://docs.voiceflow.com/api-reference/transcript/get-transcript
// https://docs.voiceflow.com/api-reference/transcript/search-transcripts
// Evaluations: returned on the same GET body under transcript.evaluations (no separate run-evaluation call).

/** Transcript object from GET /v1/transcript/{id} (contains logs, evaluations, id, …). */
function getTranscriptRootFromApiBody(body) {
  const t = body?.transcript;
  if (!t) return null;
  if (t.transcript && typeof t.transcript === "object") return t.transcript;
  return t;
}

function getTranscriptLogsFromApiBody(body) {
  const root = getTranscriptRootFromApiBody(body);
  if (!root || !Array.isArray(root.logs)) return [];
  return root.logs;
}

function getEvaluationsFromApiBody(body) {
  const root = getTranscriptRootFromApiBody(body);
  if (!root || !Array.isArray(root.evaluations)) return [];
  return root.evaluations;
}

/** Trace log `data` is { time, type, turnID, payload } (no top-level startTime). */
function analyticsTraceLogToRuntimeTurn(log) {
  if (!log || log.type !== "trace") return null;
  const data = log.data;
  if (!data || typeof data !== "object" || Array.isArray(data)) return null;
  const t = typeof data.type === "string" ? data.type : null;
  if (!t) return null;

  const startTime =
    typeof log.createdAt === "string"
      ? log.createdAt
      : typeof log.updatedAt === "string"
        ? log.updatedAt
        : undefined;

  const base = {
    turnID: data.turnID,
    startTime,
    nluError: data.nluError,
  };

  if (t === "request") {
    return {
      ...base,
      type: "request",
      payload: data.payload,
      format: "request",
    };
  }

  if (t === "launch") {
    return {
      ...base,
      type: "launch",
      payload: data.payload != null ? data.payload : {},
      format: "launch",
    };
  }

  return {
    ...base,
    type: t,
    payload: data,
    format: t === "request" ? "request" : "trace",
  };
}

/**
 * Interact / API modality: user free text is often `type: "action"` with
 * `data: { type: "text", payload: "<user message>" }` — no `request` trace.
 */
function analyticsActionLogToRuntimeTurn(log) {
  if (!log || log.type !== "action") return null;
  const data = log.data;
  if (!data || typeof data !== "object") return null;
  if (data.type !== "text" || typeof data.payload !== "string") return null;
  const query = data.payload.trim();
  if (!query) return null;

  const startTime =
    typeof log.createdAt === "string"
      ? log.createdAt
      : typeof log.updatedAt === "string"
        ? log.updatedAt
        : undefined;

  return {
    turnID: data.turnID,
    type: "request",
    format: "request",
    startTime,
    payload: {
      type: "intent",
      payload: {
        query,
        intent: { name: "None" },
      },
    },
  };
}

function mapAnalyticsTranscriptLogsToTurns(logs) {
  if (!Array.isArray(logs)) return [];
  const out = [];
  for (const log of logs) {
    let turn = analyticsTraceLogToRuntimeTurn(log);
    if (!turn) turn = analyticsActionLogToRuntimeTurn(log);
    if (turn) out.push(turn);
  }
  return out;
}

async function searchAnalyticsTranscriptIds(startTime, endTime) {
  const url = `https://analytics-api.voiceflow.com/v1/transcript/project/${encodeURIComponent(projectID)}`;
  const ids = [];
  let skip = 0;
  const take = 100;
  for (;;) {
    const { data, status } = await axios.post(
      url,
      { startDate: startTime, endDate: endTime },
      {
        headers,
        params: { take, skip, order: "DESC" },
        validateStatus: () => true,
      }
    );
    if (status >= 400) {
      console.error(
        "[analytics transcript search] HTTP",
        status,
        data?.message || JSON.stringify(data)
      );
      break;
    }
    const batch = data.transcripts || [];
    for (const row of batch) {
      if (row.id) ids.push(row.id);
    }
    if (batch.length < take) break;
    skip += take;
  }
  return ids;
}

/** Single GET returns logs (turns) and embedded evaluations — no separate run-evaluation call. */
async function fetchTranscriptFromAnalytics(transcriptID) {
  const response = await axios.get(
    `https://analytics-api.voiceflow.com/v1/transcript/${encodeURIComponent(transcriptID)}`,
    {
      headers: {
        accept: "application/json",
        ...headers,
      },
    }
  );
  const body = response.data;
  const turns = mapAnalyticsTranscriptLogsToTurns(
    getTranscriptLogsFromApiBody(body)
  );
  const evaluations = getEvaluationsFromApiBody(body);
  return { turns, evaluations };
}

// bigQuery settings
const datasetID = "chatbot_analytics";
const client = dryRunPath
  ? null
  : new BigQuery({
      projectId: "double-venture-436617-u9",
    });

/** Tables that use a TIMESTAMP column named `timestamp` for row time (session / insert window). */
const BQ_TABLES_WITH_ROW_TIMESTAMP = new Set([
  "conversation_summary",
  "conversation_logs",
  "chatbot_didnt_understand_table",
  "top_intents_table",
  "sessions_table",
]);

/**
 * Delete rows where `timestamp` is within the last `minutesAgo` minutes (rolling window from now).
 * @param {string} tableName — must be in BQ_TABLES_WITH_ROW_TIMESTAMP
 * @param {number} minutesAgo — positive; floored; capped at 525600 (one year)
 * @param {{ dryRun?: boolean }} [options] — if dryRun, runs SELECT COUNT only
 * @returns {Promise<{ tableName: string, minutes: number, dryRun: boolean, affectedRows: number }>}
 */
async function deleteBigQueryRowsInLastMinutes(tableName, minutesAgo, options = {}) {
  const dryRun = Boolean(options.dryRun);
  if (!BQ_TABLES_WITH_ROW_TIMESTAMP.has(tableName)) {
    throw new Error(
      `Unknown table "${tableName}". Allowed: ${[...BQ_TABLES_WITH_ROW_TIMESTAMP].sort().join(", ")}`
    );
  }
  const raw = Number(minutesAgo);
  const minutes = Math.floor(raw);
  if (!Number.isFinite(minutes) || minutes < 1 || minutes > 525600) {
    throw new Error(
      "minutesAgo must be a finite number between 1 and 525600 (one year)."
    );
  }
  if (dryRunPath || !client) {
    throw new Error(
      "BigQuery client required: unset ANALYTICS_DRY_RUN / ANALYTICS_OUTPUT_FILE."
    );
  }

  const tableRef = `\`${client.projectId}.${datasetID}.${tableName}\``;
  const cutoff = `TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ${minutes} MINUTE)`;

  if (dryRun) {
    const [rows] = await client.query({
      query: `SELECT COUNT(1) AS cnt FROM ${tableRef} WHERE timestamp >= ${cutoff}`,
    });
    const cnt = Number(rows[0]?.cnt ?? 0);
    console.log(
      `[dry-run] Would delete ${cnt} row(s) from ${tableName} (timestamp >= last ${minutes} min).`
    );
    return {
      tableName,
      minutes,
      dryRun: true,
      affectedRows: cnt,
    };
  }

  const query = `DELETE FROM ${tableRef} WHERE timestamp >= ${cutoff}`;
  const [job] = await client.createQueryJob({ query });
  await job.getQueryResults();
  const [metadata] = await job.getMetadata();
  const n = Number(metadata?.statistics?.query?.numDmlAffectedRows ?? 0);
  console.log(`Deleted ${n} row(s) from ${tableName} (timestamp in last ${minutes} minutes).`);
  return {
    tableName,
    minutes,
    dryRun: false,
    affectedRows: n,
  };
}

// to get the time range for the last 10 minutes- actually does the last day now lol
function getDateRangeForLastTenMinutes() {
  const nowTime = new Date();
  const currentDate = new Date(nowTime);
  currentDate.setHours(0, 0, 0, 0);
  const startTime = currentDate.toISOString();
  const endTime = nowTime.toISOString();
  console.log(`startTime: ${startTime}, endTime: ${endTime}`);
  return { startTime, endTime };
}


//  to process data for the last hour
async function populateBQforLastHour() {
  const dryRunPayload = dryRunPath
    ? {
        meta: {
          dryRun: true,
          writtenTo: dryRunPath,
          datasetID,
          startedAt: new Date().toISOString(),
        },
        conversation_summaries: [],
        chatbot_didnt_understand: [],
        conversation_logs: [],
        top_intents: [],
        sessions: [],
        transcript_evaluations: [],
      }
    : null;

  try {
    const { startTime, endTime } = getDateRangeForLastTenMinutes();

    console.log(
      `${dryRunPath ? "[dry-run] " : ""}Processing data for the last 10 minutes: ${startTime} to ${endTime}`
    );

    const transcriptIdsInWindow = await searchAnalyticsTranscriptIds(
      startTime,
      endTime
    );
    console.log(
      `Search transcripts: ${transcriptIdsInWindow.length} id(s) in window (stored as sessions count; 1 transcript ≈ 1 session)`
    );

    await fetchAndLogConversations(
      startTime,
      endTime,
      dryRunPayload,
      transcriptIdsInWindow
    );
    await fetchTopIntents(startTime, endTime, dryRunPayload);
    await fetchTotalChatsInitiated(
      startTime,
      endTime,
      dryRunPayload,
      transcriptIdsInWindow.length
    );

    if (dryRunPayload && dryRunPath) {
      dryRunPayload.meta.finishedAt = new Date().toISOString();
      fs.writeFileSync(
        dryRunPath,
        JSON.stringify(dryRunPayload, null, 2),
        "utf8"
      );
      console.log(`Dry run: wrote ${dryRunPath} (no BigQuery)`);
    }

    console.log("Completed processing data for the last 10 minutes.");
  } catch (error) {
    console.error("Error processing data:", error.message);
  }
}

/**
 * Voiceflow AI / number evaluation from GET /v1/transcript (same rules as former rating-only helper).
 * Optional TRANSCRIPT_EVALUATION_NAME (or EVALUATION_NAME) to pick by name.
 * @returns {{ rating: number | null, evaluation_rationale: string | null }}
 */
function pickNumericEvaluationForSummary(evals) {
  if (!Array.isArray(evals) || evals.length === 0) {
    return { rating: null, evaluation_rationale: null };
  }
  const byName =
    process.env.TRANSCRIPT_EVALUATION_NAME?.trim() ||
    process.env.EVALUATION_NAME?.trim();
  let candidate;
  if (byName) {
    candidate = evals.find((e) => e && e.name === byName);
  }
  if (!candidate) {
    candidate = evals.find((e) => e && e.type === "number");
  }
  if (!candidate) {
    return { rating: null, evaluation_rationale: null };
  }
  const v = candidate.value;
  let rating;
  if (typeof v === "number" && !Number.isNaN(v)) {
    rating = v;
  } else {
    const n = parseFloat(String(v).replace(/,/g, ""));
    rating = Number.isNaN(n) ? null : n;
  }
  const reason = candidate.reason;
  const evaluation_rationale =
    reason != null && String(reason).trim() !== ""
      ? String(reason)
      : null;
  return { rating, evaluation_rationale };
}

/**
 * Same rules as when inserting conversation_summary: code-step rating wins; otherwise Voiceflow
 * numeric evaluation supplies rating + evaluation_rationale.
 */
function computeRatingAndEvaluationRationale(transcriptDialog, voiceflowEvaluations) {
  let rating = null;
  transcriptDialog.forEach((turn) => {
    const extractedRating = extractSatisfactionRating(turn);
    if (extractedRating !== null) {
      rating = extractedRating;
    }
  });
  let evaluation_rationale = null;
  const vfEval = pickNumericEvaluationForSummary(voiceflowEvaluations);
  if (rating == null && vfEval.rating != null) {
    rating = vfEval.rating;
    evaluation_rationale = vfEval.evaluation_rationale;
  }
  return { rating, evaluation_rationale };
}

// Set-v3 debug: metadata.diff.rating_number.before / .after (see Voiceflow transcript logs)
// Code step: `evaluating code - changes:` + `{rating_number}`: `""` => `3`
function extractSatisfactionRating(turn) {
  if (turn.type !== "debug") return null;
  const inner = turn.payload?.payload;
  const after = inner?.metadata?.diff?.rating_number?.after;
  if (after !== undefined && after !== null && after !== "") {
    const n = typeof after === "number" ? after : parseInt(String(after), 10);
    if (!Number.isNaN(n)) return n;
  }

  const message = inner?.message != null ? String(inner.message) : "";
  if (!message) return null;

  const backtickFromEmpty = message.match(
    /`\{rating_number\}`\s*:\s*`""`\s*=>\s*`(\d+)`/
  );
  if (backtickFromEmpty) return parseInt(backtickFromEmpty[1], 10);

  const backtickUpdate = message.match(
    /`\{rating_number\}`\s*:\s*`\d+`\s*=>\s*`(\d+)`/
  );
  if (backtickUpdate) return parseInt(backtickUpdate[1], 10);

  const plain = message.match(/\{rating_number\}\s*:\s*""\s*=>\s*(\d+)/);
  if (plain) return parseInt(plain[1], 10);

  return null;
}

// to check if a message contains Sharia-related keywords
function containsShariaKeyword(message, keywords) {
  return keywords.some((keyword) => message.toLowerCase().includes(keyword));
}

// to insert conversation summaries into BigQuery
async function insertConversationSummary(summary, dryRunPayload) {
  if (dryRunPayload) {
    dryRunPayload.conversation_summaries.push(summary);
    logDryRunWouldInsertTable("conversation_summary", [summary]);
    return;
  }
  console.log("Inserting conversation summary:");
  try {
    const [error] = await client
      .dataset(datasetID)
      .table("conversation_summary")
      .insert([summary]);

    if (error && error.length > 0) {
      console.error("Error inserting rows into conversation_summary:", error);
    } else {
      console.log(`Inserted 1 row into conversation_summary.`);
    }
  } catch (error) {
    console.error("Failed to insert conversation summary:", error.message);
  }
}

// to insert 'Sorry' queries into BigQuery
async function insertSorryQueriesIntoBigQuery(
  sorryResponses,
  timestamp,
  dryRunPayload
) {
  const rowsToInsert = sorryResponses.map((query) => ({
    query: query,
    timestamp: timestamp,
  }));
  if (rowsToInsert.length === 0) {
    console.log("No sorry queries found in the response.");
    return;
  }
  if (dryRunPayload) {
    dryRunPayload.chatbot_didnt_understand.push(...rowsToInsert);
    logDryRunWouldInsertTable("chatbot_didnt_understand_table", rowsToInsert);
    return;
  }
  console.log("Inserting 'Sorry' queries into BigQuery:");
  try {
    const [error] = await client
      .dataset(datasetID)
      .table("chatbot_didnt_understand_table")
      .insert(rowsToInsert);
    if (error && error.length > 0) {
      console.error(
        "Error inserting rows into chatbot_didnt_understand_table:",
        error
      );
    } else {
      console.log(
        `Inserted ${rowsToInsert.length} rows into chatbot_didnt_understand_table.`
      );
    }
  } catch (error) {
    console.error("Failed to insert 'Sorry' queries:", error.message);
  }
}

async function insertConversationTurns(conversationId, turns, dryRunPayload) {
  try {
    console.log(`Processing conversation ID: ${conversationId}`);
    console.log(`Total turns received: ${turns.length}`);

    const rowsToInsert = [];
    let lastMessageTimestamp = null; // Track the Unix timestamp of the last message

    turns.forEach((turn, index) => {
      const isUserMessage = turn.type === "request";
      const isChatbotMessage = turn.type === "text";

      // Only process user or chatbot messages
      if (isUserMessage || isChatbotMessage) {
        let messageTimestamp = turn.startTime
          ? new Date(turn.startTime).getTime() // Convert to Unix timestamp (ms)
          : null;

        // Adjust the timestamp if it's not the first message
        if (lastMessageTimestamp !== null) {
          messageTimestamp = Math.max(messageTimestamp, lastMessageTimestamp + 1000);
        }

        const row = {
          conversation_id: conversationId,
          message_type: isUserMessage ? "user" : "chatbot",
          message_content: isUserMessage
            ? turn.payload.payload.query
            : turn.payload.payload.message,
          timestamp: messageTimestamp ? new Date(messageTimestamp).toISOString() : null,
        };

        // console.log(
        //   `Prepared row #${index} [${row.message_type}] at ${row.timestamp}`
        // );

        // Update the last message timestamp
        lastMessageTimestamp = messageTimestamp;

        // Add the message row
        rowsToInsert.push(row);
      }
    });

    if (rowsToInsert.length > 0 && lastMessageTimestamp) {
      const separatorTimestamp = lastMessageTimestamp + 2000;

      rowsToInsert.push({
        conversation_id: conversationId,
        message_type: "separator",
        message_content: "---END OF CONVERSATION---",
        timestamp: new Date(separatorTimestamp).toISOString(),
      });

    //   console.log("Added separator row.");
    }

    if (rowsToInsert.length > 0) {
      if (dryRunPayload) {
        dryRunPayload.conversation_logs.push({
          conversation_id: conversationId,
          table: "conversation_logs",
          rows: rowsToInsert,
        });
        logDryRunWouldInsertTable("conversation_logs", rowsToInsert, {
          subtitle: `conversation_id ${conversationId}`,
        });
      } else {
        console.log(`Attempting to insert ${rowsToInsert.length} rows...`);

        const [error] = await client
          .dataset(datasetID)
          .table("conversation_logs")
          .insert(rowsToInsert);

        if (error && error.length > 0) {
          console.error("Error inserting rows into conversation_logs:", error);
        } else {
          console.log(`Successfully inserted ${rowsToInsert.length} rows.`);
        }
      }
    } else {
      console.log(
        `No valid messages for conversation ID: ${conversationId}. Skipping insertion.`
      );
    }
  } catch (error) {
    console.error("Failed to insert conversation turns:", error);
  }
}

// to fetch and log conversations for the last hour
// transcriptIdsPreloaded: optional list from Search transcripts (same pass as sessions count).
async function fetchAndLogConversations(
  startTime,
  endTime,
  dryRunPayload,
  transcriptIdsPreloaded
) {
  const shariaKeywords = ["musharakah", "mudarabah", "riba"];
  const sorryResponses = [];
  const targetResponses = [
    "Sorry, I'm not sure about that one. Is there anything else?",
    "Sorry. I didn't quite get that, can you please try asking the question in a different way?",
    "I apologize",
  ];

  const chatbotMessageIndicatesDidntUnderstand = (msg) =>
    targetResponses.some((response) => msg.includes(response)) ||
    msg.toLowerCase().includes("apologize");

  try {
    const transcriptIDs =
      transcriptIdsPreloaded ??
      (await searchAnalyticsTranscriptIds(startTime, endTime));
    console.log(
      `Processing ${transcriptIDs.length} transcript ID(s) (Search API; same ids as GET /v1/transcript/{id})`
    );

    for (const transcriptID of transcriptIDs) {
      console.log("Processing transcript ID:", transcriptID);
      const { turns: transcriptDialog, evaluations: voiceflowEvaluations } =
        await fetchTranscriptFromAnalytics(transcriptID);

      if (dryRunPayload) {
        console.log(
          `\n[dry-run] GET /v1/transcript — ${voiceflowEvaluations.length} embedded evaluation(s) for ${transcriptID}`
        );
        if (voiceflowEvaluations.length > 0) {
          console.table(
            rowsForDisplayTable(
              voiceflowEvaluations.map((e) => ({
                name: e.name,
                type: e.type,
                value: e.value,
                cost: e.cost,
                reason: e.reason,
              }))
            )
          );
          logEvaluationRationales(voiceflowEvaluations);
        }
        dryRunPayload.transcript_evaluations.push({
          transcriptID,
          source: "GET /v1/transcript",
          evaluations: voiceflowEvaluations,
        });
      } else if (voiceflowEvaluations.length > 0) {
        console.log(
          `Transcript ${transcriptID}: ${voiceflowEvaluations.length} embedded evaluation(s) from GET /v1/transcript`
        );
        logEvaluationRationales(voiceflowEvaluations);
      }

      // printAllTranscriptTurns(transcriptID, transcriptDialog);

      await insertConversationTurns(
        transcriptID,
        transcriptDialog,
        dryRunPayload
      );


      let lastUserQuery = "";
      let conversationLength = 0;
      let chatbotMessages = 0;
      let userMessages = 0;
      let nluErrors = 0;
      let selfService = true;
      let shariaTermsUsed = [];
      let tokenQuotaExceededCount = 0;
      let timestamp;

      transcriptDialog.forEach((turn) => {
        let message = "";

        if (!timestamp && turn.startTime) {
          timestamp = turn.startTime;
        }

        // If the turn is a user message, save it
        if (turn.type === "request" && turn.payload?.payload?.query) {
          lastUserQuery = turn.payload.payload.query;
          userMessages++;
          message = lastUserQuery;
        }

        // If the turn is a chatbot response and looks like a “didn’t understand” / apology reply, store the last user query
        if (turn.type === "text" && turn.payload?.payload?.message) {
          const chatbotMessage = turn.payload.payload.message;
          message = chatbotMessage;

          if (
            chatbotMessageIndicatesDidntUnderstand(chatbotMessage) &&
            lastUserQuery
          ) {
            sorryResponses.push(lastUserQuery);
          }

          // If the response is "Token Quota Exceeded," increment the count
          if (chatbotMessage === "Token Quota Exceeded") {
            tokenQuotaExceededCount++;
          }

          // Check for Sharia-related keywords
          if (containsShariaKeyword(message, shariaKeywords)) {
            shariaKeywords.forEach((keyword) => {
              if (
                message.toLowerCase().includes(keyword) &&
                !shariaTermsUsed.includes(keyword)
              ) {
                shariaTermsUsed.push(keyword);
              }
            });
          }

          chatbotMessages++;
        }

        if (
          turn.type === "request" &&
          turn.payload?.payload?.intent?.name === "live_agent"
        ) {
          selfService = false;
        }

        if (turn.nluError) {
          nluErrors++;
        }
      });

      const { rating, evaluation_rationale } = computeRatingAndEvaluationRationale(
        transcriptDialog,
        voiceflowEvaluations
      );

      conversationLength = chatbotMessages + userMessages;

      const summary = {
        conversation_id: transcriptID,
        timestamp: timestamp,
        conversation_length: conversationLength,
        chatbot_total_length: chatbotMessages,
        user_total_length: userMessages,
        rating: rating,
        evaluation_rationale: evaluation_rationale,
        self_serviced: selfService,
        sharia_terms_used: shariaTermsUsed.join(", "),
        nlu_errors: nluErrors,
        token_quota_exceeded_count: tokenQuotaExceededCount,
      };

      console.log("Conversation Summary:", summary);

      // Insert conversation summary into BigQuery
      await insertConversationSummary(summary, dryRunPayload);
      await insertSorryQueriesIntoBigQuery(
        sorryResponses,
        timestamp,
        dryRunPayload
      );
    }
  } catch (error) {
    console.error(
      `Failed to fetch data: ${
        error.response
          ? `${error.response.status} - ${error.response.statusText}`
          : error.message
      }`
    );
  }
}

/**
 * Re-fetch GET /v1/transcript for rows missing evaluation_rationale and UPDATE BigQuery.
 * Voiceflow must still return evaluations on the transcript; sessions rated only via code-step
 * stay without rationale (same as live pipeline).
 *
 * Env: BACKFILL_EVAL_LIMIT (default 500), BACKFILL_EVAL_MS_BETWEEN (default 250),
 * BACKFILL_EVAL_DRY_RUN=1 (log only), BACKFILL_EVAL_SINCE=YYYY-MM-DD (optional).
 */
async function backfillConversationEvaluationRationale() {
  if (dryRunPath) {
    console.error(
      "Backfill needs BigQuery: unset ANALYTICS_DRY_RUN and ANALYTICS_OUTPUT_FILE."
    );
    process.exitCode = 1;
    return;
  }
  if (!client) {
    console.error("BigQuery client not initialized.");
    process.exitCode = 1;
    return;
  }

  const tableId = `${client.projectId}.${datasetID}.conversation_summary`;
  const limit = Math.max(
    1,
    parseInt(process.env.BACKFILL_EVAL_LIMIT || "500", 10) || 500
  );
  const msBetween = Math.max(
    0,
    parseInt(process.env.BACKFILL_EVAL_MS_BETWEEN || "250", 10) || 0
  );
  const since = process.env.BACKFILL_EVAL_SINCE?.trim();
  const dry = /^1|true$/i.test(process.env.BACKFILL_EVAL_DRY_RUN || "");

  const params = { limit };
  let query = `
    SELECT conversation_id, rating AS stored_rating
    FROM \`${tableId}\`
    WHERE evaluation_rationale IS NULL
  `;
  if (since) {
    query += ` AND DATE(timestamp) >= @since`;
    params.since = since;
  }
  query += `
    ORDER BY timestamp DESC
    LIMIT @limit
  `;

  const [rows] = await client.query({ query, params });

  console.log(
    `Backfill: ${rows.length} row(s) with NULL evaluation_rationale${dry ? " (dry run)" : ""}.`
  );

  let updated = 0;
  let skipped = 0;
  let errors = 0;

  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    const conversation_id = row.conversation_id;
    const storedRating = row.stored_rating;

    try {
      const { turns, evaluations } = await fetchTranscriptFromAnalytics(
        conversation_id
      );
      const { rating, evaluation_rationale } =
        computeRatingAndEvaluationRationale(turns, evaluations);

      const hasRationale =
        evaluation_rationale != null &&
        String(evaluation_rationale).trim() !== "";
      const fillRatingOnly = !hasRationale && storedRating == null && rating != null;

      if (!hasRationale && !fillRatingOnly) {
        skipped++;
        if (msBetween > 0 && i + 1 < rows.length) await sleep(msBetween);
        continue;
      }

      if (dry) {
        console.log(
          `[dry-run] ${conversation_id}: ${hasRationale ? `rationale (${String(evaluation_rationale).length} chars), ` : ""}rating=${rating}`
        );
        updated++;
      } else if (hasRationale) {
        await client.query({
          query: `
            UPDATE \`${tableId}\`
            SET rating = @rating, evaluation_rationale = @evaluation_rationale
            WHERE conversation_id = @conversation_id
          `,
          params: {
            conversation_id,
            rating,
            evaluation_rationale,
          },
        });
        updated++;
      } else {
        await client.query({
          query: `
            UPDATE \`${tableId}\`
            SET rating = @rating
            WHERE conversation_id = @conversation_id
          `,
          params: { conversation_id, rating },
        });
        updated++;
      }
    } catch (e) {
      errors++;
      const msg =
        e.response != null
          ? `${e.response.status} ${e.response.statusText}`
          : e.message;
      console.error(`Backfill failed ${conversation_id}:`, msg);
    }

    if (msBetween > 0 && i + 1 < rows.length) await sleep(msBetween);
  }

  console.log(
    `Backfill done: updated ${updated}, skipped (no VF rationale / rating to add) ${skipped}, errors ${errors}.`
  );
}

// Fetch top intents for the last hour
async function fetchTopIntents(startTime, endTime, dryRunPayload) {
  try {
    const { data: body } = await axios.post(
      analyticsUsageUrl,
      {
        data: {
          name: "top_intents",
          filter: {
            projectID,
            startTime,
            endTime,
          },
        },
      },
      { headers }
    );

    const result = body?.result;
    const intentsFromResult =
      result?.intents ?? (Array.isArray(result) ? result[0]?.intents : null);

    if (intentsFromResult && intentsFromResult.length > 0) {
      let intentsArray = intentsFromResult;
      intentsArray = intentsArray.filter(
        (intent) => intent.name !== "End conversation"
      );

      const rowsToInsert = intentsArray.map((intent) => ({
        intent: intent.name,
        count: intent.count,
        timestamp: startTime,
      }));

      if (rowsToInsert.length === 0) {
        console.log("No intents found in the response.");
        return;
      }

      if (dryRunPayload) {
        dryRunPayload.top_intents.push(...rowsToInsert);
        logDryRunWouldInsertTable("top_intents_table", rowsToInsert);
        return;
      }

      console.log("Rows to insert:");
      console.table(rowsToInsert);

      const [error] = await client
        .dataset(datasetID)
        .table("top_intents_table")
        .insert(rowsToInsert);

      if (error.insertErrors && error.insertErrors.length > 0) {
        console.error(
          "Error inserting rows into BigQuery:",
          error.insertErrors
        );
      } else {
        console.log(`Inserted ${rowsToInsert.length} rows into top_intents.`);
      }
    } else {
      console.log("No data found in the response.");
    }
  } catch (error) {
    console.error("Error fetching top intents:", error.message);
  }
}

/**
 * sessions_table.count = number of transcripts returned by Search transcripts for the window.
 * Assumption: one Voiceflow transcript ≈ one chat session. Same Search pass as fetchAndLogConversations when called from populateBQforLastHour.
 */
async function fetchTotalChatsInitiated(
  startTime,
  endTime,
  dryRunPayload,
  sessionCountFromSearch
) {
  try {
    const totalSessions =
      typeof sessionCountFromSearch === "number"
        ? sessionCountFromSearch
        : (await searchAnalyticsTranscriptIds(startTime, endTime)).length;

    console.log(
      "sessions_table count (Search transcripts, 1 id ≈ 1 session):",
      totalSessions
    );

    const rowsToInsert = [
      {
        count: totalSessions,
        timestamp: endTime,
      },
    ];

    if (rowsToInsert.length === 0) {
      console.log("No sessions found in the response.");
      return;
    }

    if (dryRunPayload) {
      dryRunPayload.sessions.push(...rowsToInsert);
      logDryRunWouldInsertTable("sessions_table", rowsToInsert);
      return;
    }

    const [error] = await client
      .dataset(datasetID)
      .table("sessions_table")
      .insert(rowsToInsert);

    if (error.insertErrors && error.insertErrors.length > 0) {
      console.error("Error inserting rows into BigQuery:", error.insertErrors);
    } else {
      console.log(`Inserted ${rowsToInsert.length} rows into sessions_table.`);
    }
  } catch (error) {
    console.error("Error fetching total chats initiated:", error.message);
  }
}

if (require.main === module && process.argv[2] === "backfill-eval") {
  backfillConversationEvaluationRationale()
    .then(() => {
      if (!process.exitCode) console.log("Backfill finished.");
      process.exit(process.exitCode || 0);
    })
    .catch((err) => {
      console.error(err);
      process.exit(1);
    });
} else if (require.main === module && process.argv[2] === "delete-recent") {
  const minutes = process.argv[3];
  const tableName = process.argv[4];
  const dry = /^1|true$/i.test(process.env.DELETE_RECENT_DRY_RUN || "");
  if (!minutes || !tableName) {
    console.error(
      "Usage: node index.js delete-recent <minutes> <tableName>\n" +
        `  tableName one of: ${[...BQ_TABLES_WITH_ROW_TIMESTAMP].sort().join(", ")}\n` +
        "  DELETE_RECENT_DRY_RUN=1 to count rows only (no DELETE)."
    );
    process.exit(1);
  }
  deleteBigQueryRowsInLastMinutes(tableName, minutes, { dryRun: dry })
    .then(() => process.exit(0))
    .catch((err) => {
      console.error(err.message || err);
      process.exit(1);
    });
} else if (require.main === module) {
  populateBQforLastHour();
}

exports.populateBQforLastHour = async (req, res) => {
  try {
    await populateBQforLastHour();
    res.status(200).send("Successfully processed data for the last 10 mins.");
  } catch (error) {
    res.status(500).send("Error processing data: " + error.message);
  }
};

exports.deleteBigQueryRowsInLastMinutes = deleteBigQueryRowsInLastMinutes;
