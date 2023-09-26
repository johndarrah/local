-- Validating SSN numbers:
SELECT
  '000-00-0000'                                 AS ssn_1
  , '000-00-000a'                               AS ssn_2
  , '000-00-000'                                AS ssn_3
  , ssn_1 REGEXP '^[0-9]{3}-[0-9]{2}-[0-9]{4}$' AS ssn_1_is_correct
  , ssn_2 REGEXP '^[0-9]{3}-[0-9]{2}-[0-9]{4}$' AS ssn_2_is_correct
  , ssn_3 REGEXP '^[0-9]{3}-[0-9]{2}-[0-9]{4}$' AS ssn_3_is_correct
;

-- Identifying email addresses:
SELECT
  'darrah@gmail.com'                                                    AS email_1
  , 'darrahgmail.com'                                                   AS email_2
  , 'darrah@gmail'                                                      AS email_3
  , email_1 REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,4}$' AS email_1_correct
  , email_2 REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,4}$' AS email_2_correct
  , email_3 REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,4}$' AS email_3_correct
;

-- Identifying strings that begin or end with characters:
SELECT
  'chat this is a test'                AS channel_1
  , 'this is the phone channel: voice' AS channel_2
  , channel_1 REGEXP '^chat.+.'        AS channel_starts_with_chat
  , channel_2 REGEXP '.+.voice$'       AS channel_ends_with_voice
;
