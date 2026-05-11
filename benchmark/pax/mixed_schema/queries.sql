SELECT * FROM pax_mixed.rows;
SELECT payload FROM pax_mixed.rows;
SELECT id, payload FROM pax_mixed.rows WHERE flag = true;
SELECT payload FROM pax_mixed.rows WHERE category = 'C';
SELECT id, amount FROM pax_mixed.rows WHERE amount >= 50 AND amount < 100;
SELECT payload FROM pax_mixed.rows WHERE amount >= 120 AND flag = false;
SELECT category, payload FROM pax_mixed.rows WHERE amount = 127;
SELECT category, payload FROM pax_mixed.rows WHERE amount >= 92 AND amount < 120;
