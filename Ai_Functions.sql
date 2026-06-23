-- Databricks notebook source
sagar prajapati kanput 5 years experience 

-- COMMAND ----------

-- ============================================================
-- ALL DATABRICKS AI FUNCTIONS — EXAMPLES USING ONE DATASET
-- Dataset: customer_reviews (product feedback table)
-- ============================================================

-- ▸ SAMPLE DATASET SETUP
-- ============================================================
CREATE OR REPLACE TABLE workspace.default.customer_reviews (
  review_id     INT,
  customer_name STRING,
  review_text   STRING,
  review_date   DATE,
  product       STRING,
  rating        INT
);

INSERT INTO workspace.default.customer_reviews VALUES
(1, 'John Smith',   'I absolutly lovd this laptop! The batter life is amazng and the screen is beautful. My email is john.smith@gmail.com', '2025-01-15', 'Laptop',    5),
(2, 'Maria Garcia', 'Terrible headphones. Sound quality was awful and they broke after 2 days. Very disappointed. Call me at 555-123-4567',   '2025-01-20', 'Headphones', 1),
(3, 'Amit Patel',   'The keyboard is okay, nothing special. Keys feel a bit mushy but it works fine for daily use.',                          '2025-02-01', 'Keyboard',   3),
(4, 'Sara Johnson', 'Best phone I have ever owned! Camera is outstanding and the performance is super smooth. Worth every penny.',            '2025-02-10', 'Phone',      5),
(5, 'Li Wei',       'The tablet screen cracked within a week. Customer support was unhelpful. I want a refund immediately.',                  '2025-02-15', 'Tablet',     1);

-- COMMAND ----------

select * from workspace.default.customer_reviews

-- COMMAND ----------

-- ============================================================
-- 1. ai_analyze_sentiment()
--    Detects sentiment: positive, negative, mixed, or neutral
-- ============================================================
SELECT
  review_id,
  review_text,
  ai_analyze_sentiment(review_text) AS sentiment
FROM workspace.default.customer_reviews;

-- COMMAND ----------

-- ============================================================
-- 2. ai_classify()
--    Classifies text into one of the provided labels
-- ============================================================
SELECT
  review_id,
  review_text,
  ai_classify(review_text, ARRAY('Hardware Issue', 'Softwate Issue', 'Positive Feedback', 'Customer Service Complaint', 'General Feedback')) AS category
FROM workspace.default.customer_reviews;

-- COMMAND ----------

select
  ai_query(
    'databricks-gpt-oss-120b',
    concat(
      'extract name (stirng),city (string), phone_number(int) and skills(list/array) format',
      'data: sagar prajapati kanpur +91564725474 skills aws,azure,sql',
      'Return Json object with no other extra values. Dont include  ```json'
    )
  ) as new_data

-- COMMAND ----------

select
  ai_query(
    'databricks-meta-llama-3-3-70b-instruct',
    concat(
      'extract name (stirng),city (string), phone_number(int) and skills(list/array) format',
      'data: sagar prajapati kanpur +91564725474 skills aws,azure,sql',
      'Return Json object with no other extra values. Dont include  ```json'
    )
  ) as new_data

-- COMMAND ----------

 select ai_extract('sagar prajapati kanpur 7645453 skills:aws,azure', ARRAY('city', 'name', 'phonenumber', 'skills')) AS extracted

-- COMMAND ----------

-- ============================================================
-- 3. ai_extract()
--    Extracts structured info from free-form text
-- ============================================================
SELECT
  review_id,
  review_text,
  ai_extract(review_text, ARRAY('product_feature', 'issue', 'customer_emotion')) AS extracted
FROM workspace.default.customer_reviews;

-- COMMAND ----------

-- ============================================================
-- 4. ai_fix_grammar()
--    Corrects grammar and spelling errors in text
-- ============================================================
SELECT
  review_id,
  review_text,
  ai_fix_grammar(review_text) AS corrected_text
FROM workspace.default.customer_reviews;

-- COMMAND ----------

-- 5. ai_gen()
--    Generates text from a prompt (content creation)
-- ============================================================
SELECT
  review_id,
  customer_name,
  review_text,
  ai_gen(
    'Write a short, professional thank-you reply to this customer review: ' || 'This is the customer name' ||customer_name
 || 'This is the customer feedback' || review_text || 'Dont inlcude any other message.Always include customer name at the starting to greet. Add Sagar Name at the end for Thank you'
  ) AS auto_reply
FROM workspace.default.customer_reviews
WHERE rating >= 4;

-- COMMAND ----------

-- ============================================================
-- 6. ai_mask()
--    Masks PII (personally identifiable information) in text
-- ============================================================
SELECT
  review_id,
  review_text,
  ai_mask(review_text, ARRAY('person', 'email', 'phone')) AS masked_text
FROM workspace.default.customer_reviews;


-- COMMAND ----------

sagar ==[-1344,4464,5655,]
sagas =[]

-- COMMAND ----------

select ai_similarity('sagar','sagas')

-- COMMAND ----------

-- ============================================================
-- 7. ai_similarity()
--    Computes semantic similarity between two strings (0 to 1)
-- ============================================================
SELECT
  review_id,
  product,
  ai_similarity(review_text, 'The product broke and I need a replacement') AS similarity_score
FROM workspace.default.customer_reviews
ORDER BY similarity_score DESC;

-- Useful for finding reviews most similar to a reference complaint




-- COMMAND ----------

-- ============================================================
-- 8. ai_summarize()
--    Generates a concise summary of the text
-- ============================================================
SELECT
  review_id,
  review_text,
  ai_summarize(review_text) AS summary
FROM workspace.default.customer_reviews;

-- COMMAND ----------

-- ============================================================
-- 9. ai_translate()
--    Translates text to a target language
-- ============================================================
SELECT
  review_id,
  review_text,
  ai_translate(review_text, 'French')  AS french_translation,
  ai_translate(review_text, 'Spanish') AS spanish_translation
FROM workspace.default.customer_reviews;




-- COMMAND ----------

-- DBTITLE 1,10. ai_forecast() — FIXED horizon argument
-- ============================================================
-- 10. ai_forecast()
--     Time-series forecasting (numeric + date column required)
-- ============================================================

-- First, create an aggregated time-series view
CREATE OR REPLACE TEMP VIEW daily_ratings AS
SELECT review_date, AVG(rating) AS avg_rating
FROM workspace.default.customer_reviews
GROUP BY review_date
ORDER BY review_date;

-- Forecast future average ratings (next 7 days)
SELECT *
FROM ai_forecast(
  TABLE(daily_ratings),              -- input table/view
  horizon   => '2025-02-27',         -- predict up to this date (7 days after last review)
  time_col  => 'review_date',        -- date column
  value_col => 'avg_rating'          -- numeric column to forecast
);

-- COMMAND ----------

-- ============================================================
-- 11. ai_query()
--     General-purpose: call ANY model endpoint with custom prompts
-- ============================================================

-- 11a. Using a Databricks Foundation Model
SELECT
  review_id,
  review_text,
  ai_query(
    'databricks-meta-llama-3-3-70b-instruct',
    'You are a customer support agent. Analyze the following review and provide:
     1) Sentiment (positive/negative/neutral)
     2) Key issues mentioned
     3) Suggested action for the support team
     Review: ' || review_text || 'Give me respons in json format. Dont include json word'
  ) AS ai_analysis
FROM workspace.default.customer_reviews;

-- 11b. With structured output (returns a typed struct)
-- SELECT
--   review_id,
--   ai_query(
--     'databricks-meta-llama-3-3-70b-instruct',
--     'Extract sentiment and main topic from: ' || review_text,
--     returnType => 'STRUCT<sentiment: STRING, topic: STRING>'
--   ) AS structured_result
-- FROM workspace.default.customer_reviews;







-- COMMAND ----------

-- ============================================================
-- BONUS: COMBINING MULTIPLE AI FUNCTIONS IN ONE QUERY
-- ============================================================
SELECT
  review_id,
  customer_name,
  ai_fix_grammar(review_text)                        AS clean_review,
  ai_analyze_sentiment(review_text)                   AS sentiment,
  ai_classify(review_text, ARRAY('Praise', 'Complaint', 'Suggestion', 'Neutral')) AS category,
  ai_summarize(review_text)                           AS summary,
  ai_mask(review_text, ARRAY('person', 'email', 'phone')) AS safe_review,
  ai_translate(review_text, 'Japanese')               AS japanese_text
FROM workspace.default.customer_reviews;
