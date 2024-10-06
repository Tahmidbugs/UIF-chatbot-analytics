const axios = require("axios");
const path = require("path");
const { BigQuery } = require("@google-cloud/bigquery");
const { v4: uuidv4 } = require("uuid");
require("dotenv").config();

const projectID = process.env.PROJECT_ID;
const authorizationToken = process.env.AUTHORIZATION_TOKEN;
const analyticsUrl = "https://analytics-api.voiceflow.com/v1/query/usage";

// BigQuery settings
const datasetID = process.env.DATASET_ID;
const conversationSummaryTableID = `blackbyrdtest.${datasetID}.${process.env.CONVERSATION_SUMMARY_TABLE}`;
const chatbotDidntUnderstandTableID = `blackbyrdtest.${datasetID}.${process.env.CHATBOT_DIDNT_UNDERSTAND_TABLE}`;

function getDateRangeForDay(day) {
  const startDate = new Date(Date.UTC(2024, 8, day));
  const endDate = new Date(Date.UTC(2024, 8, day + 1));

  const startTime = startDate.toISOString().split("T")[0] + "T00:00:00.000Z";
  const endTime = endDate.toISOString().split("T")[0] + "T00:00:00.000Z";

  return { startTime, endTime };
}

async function processMonthlyData() {
  try {
    for (let day = 25; day <= 25; day++) {
      const { startTime, endTime } = getDateRangeForDay(day);

      console.log(`Processing data for: ${startTime} to ${endTime}`);
      const transcriptsUrl = `https://api.voiceflow.com/v2/transcripts/${projectID}?startDate=${startTime}&endDate=${endTime}`;
      const transcriptDialogUrl = (transcriptID) =>
        `https://api.voiceflow.com/v2/transcripts/${projectID}/${transcriptID}`;

      await fetchAndLogConversations(transcriptsUrl, transcriptDialogUrl);
      await fetchTopIntents(startTime, endTime);
      await fetchTotalChatsInitiated(startTime, endTime);
    }

    console.log("Completed processing data for the entire month of July.");
  } catch (error) {
    console.error("Error processing July data:", error.message);
  }
}

const headers = {
  accept: "application/json",
  "content-type": "application/json",
  authorization: `${authorizationToken}`,
};

// BigQuery settings
const client = new BigQuery({
  keyFilename: path.join(__dirname, "service.json"),
});

async function fetchTranscriptIDs(transcriptsUrl) {
  const response = await axios.get(transcriptsUrl, {
    headers,
  });
  return response.data.map((transcript) => transcript._id);
}

// Function to fetch conversation dialogs for a given transcript ID
async function fetchTranscriptDialog(transcriptID, transcriptDialogUrl) {
  const response = await axios.get(transcriptDialogUrl(transcriptID), {
    headers: {
      accept: "application/json",
      Authorization: `${authorizationToken}`,
    },
  });
  return response.data;
}
function extractSatisfactionRating(turn) {
  if (turn.type === "debug" && turn.payload?.payload?.type === "code") {
    const match = turn.payload.payload.message.match(
      /`{rating_number}`: `""` => `(\d+)`/
    );
    if (match) {
      return parseInt(match[1], 10);
    }
  }
  return null;
}

function containsShariaKeyword(message, keywords) {
  return keywords.some((keyword) => message.toLowerCase().includes(keyword));
}

async function insertConversationSummary(summary) {
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

// Function to insert 'Sorry' queries into BigQuery
async function insertSorryQueriesIntoBigQuery(sorryResponses, timestamp) {
  const rowsToInsert = sorryResponses.map((query) => ({
    query: query,
    timestamp: timestamp,
  }));
  if (rowsToInsert.length === 0) {
    console.log("No sorry queries found in the response.");
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

async function fetchAndLogConversations(transcriptsUrl, transcriptDialogUrl) {
  const shariaKeywords = ["musharakah", "mudarabah", "riba"];
  const sorryResponses = [];
  const targetResponse =
    "Sorry, I'm not sure about that one. Is there anything else?";
  const targetResponse2 =
    "Sorry. I didn't quite get that, can you please try asking the question in a different way?";

  const tokenQuotaExceededResponse = "Token Quota Exceeded";

  try {
    const transcriptIDs = await fetchTranscriptIDs(transcriptsUrl);
    console.log(`Fetched ${transcriptIDs.length} transcript IDs`);

    for (const transcriptID of transcriptIDs) {
      console.log("processing transcript ID:", transcriptID);
      const transcriptDialog = await fetchTranscriptDialog(
        transcriptID,
        transcriptDialogUrl
      );

      let lastUserQuery = "";
      let conversationLength = 0;
      let chatbotMessages = 0;
      let userMessages = 0;
      let nluErrors = 0;
      let selfService = true;
      let rating = null;
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

        // If the turn is a chatbot response and matches the target response, store the last user query
        if (turn.type === "text" && turn.payload?.payload?.message) {
          const chatbotMessage = turn.payload.payload.message;
          message = chatbotMessage;
          if (
            (chatbotMessage === targetResponse ||
              chatbotMessage === targetResponse2) &&
            lastUserQuery
          ) {
            sorryResponses.push(lastUserQuery);
          }
          if (chatbotMessage === tokenQuotaExceededResponse) {
            tokenQuotaExceededCount++;
          }
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

        // Extract satisfaction rating
        const extractedRating = extractSatisfactionRating(turn);
        if (extractedRating !== null) {
          rating = extractedRating;
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

      conversationLength = chatbotMessages + userMessages;

      const summary = {
        conversation_id: transcriptID,
        timestamp: timestamp,
        conversation_length: conversationLength,
        chatbot_total_length: chatbotMessages,
        user_total_length: userMessages,
        rating: rating,
        self_serviced: selfService,
        sharia_terms_used: shariaTermsUsed.join(", "),
        nlu_errors: nluErrors,
        token_quota_exceeded_count: tokenQuotaExceededCount,
      };

      console.log("Conversation Summary:", summary);

      // Insert conversation summary into BigQuery
      await insertConversationSummary(summary);
      await insertSorryQueriesIntoBigQuery(sorryResponses, timestamp);
    }

    // Insert 'Sorry' queries into BigQuery, regardless of whether the previous insertions succeeded
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

// Fetch top intents from start of today till now
async function fetchTopIntents(startTime, endTime) {
  try {
    const options = {
      method: "POST",
      headers: headers,
      data: {
        query: [
          {
            name: "top_intents",
            filter: {
              projectID: projectID,
              startTime: startTime, // Global startTime
              endTime: endTime, // Global endTime
            },
          },
        ],
      },
    };

    const response = await axios(analyticsUrl, options);

    // Check if there is data in the response
    if (
      response.data &&
      response.data.result &&
      response.data.result.length > 0
    ) {
      // Access the intents array within the first result item
      let intentsArray = response.data.result[0].intents;
      intentsArray = intentsArray.filter(
        (intent) => intent.name !== "End conversation"
      );

      // Prepare rows for insertion into BigQuery
      const rowsToInsert = intentsArray.map((intent) => ({
        intent: intent.name,
        count: intent.count,
        timestamp: startTime,
      }));

      console.log("Rows to insert:", rowsToInsert);

      // if rowsToInsert is empty, log a message and do not insert into BigQuery
      if (rowsToInsert.length === 0) {
        console.log("No intents found in the response.");
        return;
      }

      // Uncomment this to insert into BigQuery
      const [error] = await client
        .dataset(datasetID)
        .table("top_intents_table")
        .insert(rowsToInsert);

      // // Check if there were any insert errors
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

// Count total chats initiated from start of today till now
async function fetchTotalChatsInitiated(startTime, endTime) {
  try {
    // console.log("Using global startTime:", startTime);

    const options = {
      method: "POST",
      headers: headers,
      data: {
        query: [
          {
            name: "sessions",
            filter: {
              projectID: projectID,
              startTime: startTime,
              endTime: endTime,
            },
          },
        ],
      },
    };

    const response = await axios(analyticsUrl, options);
    const totalSessions = response.data.result[0]?.count || 0;

    console.log("Total Sessions created:", totalSessions);

    const rowsToInsert = [
      {
        count: totalSessions,
        timestamp: endTime, // Global endTime
      },
    ];
    // console.log("Rows to insert:", rowsToInsert);
    // if rowsToInsert is empty, log a message and do not insert into BigQuery
    if (rowsToInsert.length === 0) {
      console.log("No sessions found in the response.");
      return;
    }

    const [error] = await client
      .dataset(datasetID)
      .table("sessions_table")
      .insert(rowsToInsert);

    // Check if there were any insert errors
    if (error.insertErrors && error.insertErrors.length > 0) {
      console.error("Error inserting rows into BigQuery:", error.insertErrors);
    } else {
      console.log(`Inserted ${rowsToInsert.length} rows into sessions_table.`);
    }
  } catch (error) {
    console.error("Error fetching total chats initiated:", error.message);
  }
}

processMonthlyData();
