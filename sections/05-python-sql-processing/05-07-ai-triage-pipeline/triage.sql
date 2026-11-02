-- Lesson 5.7: AI functions inside a Lakeflow pipeline.
-- The pipeline's default catalog and schema are lakeshore.silver.

-- A streaming table reads each new message once, so each message is classified once.
CREATE OR REFRESH STREAMING TABLE support_triaged (
  CONSTRAINT known_category EXPECT (category IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Support messages with an AI category. New messages only: inference runs once per row.'
AS SELECT msg_id, order_id, body,
          ai_classify(body,
                      '["damaged item", "late delivery", "billing", "product question"]',
                      MAP('version', '2.1')):response[0].value::string AS category
   FROM STREAM(lakeshore.silver.support_messages);

-- ai_query with a declared JSON shape, row-level errors kept instead of failing the update.
CREATE OR REFRESH MATERIALIZED VIEW support_replies
COMMENT 'Structured triage from a foundation model, capped on purpose. Recomputes on refresh.'
AS SELECT msg_id, category,
          reply.result AS triage_json,
          reply.errorMessage AS error_message
   FROM (SELECT msg_id, category,
                ai_query(
                  'databricks-meta-llama-3-3-70b-instruct',
                  'Triage this outdoor-gear support message. ' || body,
                  responseFormat => '{"type": "json_schema", "json_schema": {"name": "triage",
                    "schema": {"type": "object",
                               "properties": {"urgency": {"type": "string", "enum": ["low", "medium", "high"]},
                                              "needs_refund": {"type": "boolean"}},
                               "required": ["urgency", "needs_refund"]},
                    "strict": true}}',
                  modelParameters => named_struct('max_tokens', 60, 'temperature', 0.0),
                  failOnError => false) AS reply
         FROM support_triaged
         ORDER BY msg_id
         LIMIT 6);
