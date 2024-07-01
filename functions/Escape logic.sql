-- underscore is treated as a wildcard in the ilike statement
-- \\_ treats the underscore as a literal underscore
-- I need to confirm how the hashtags appear in the comment and if an combination of _ and spaces are used. e.g. rm_dl or rm dl
SELECT
  1 = 1
  , '#$IV_DL_AHO' AS word1
  , word1 ILIKE '%ar_dl%'                                                                                                               AS test1
  , word1 ILIKE '%IV\\_DL\\_AHO%' ESCAPE '\\'                                                                                                 AS test2
  , word1 ILIKE '%ar\_DL%'                                                                                                              AS test3
;

SELECT
  1 = 1,
  '#cash:clear DL Account has previously been Denylisted by Compliance Team. The DL reason is Money Laundering as listed in Toolbox.' AS word1,
  word1 ILIKE '%ar_dl%' AS test1,
  word1 ILIKE '%ar\\_dl%' ESCAPE '\\' AS test2,
  word1 ILIKE '%ar\_dl%' ESCAPE '\' AS test3;