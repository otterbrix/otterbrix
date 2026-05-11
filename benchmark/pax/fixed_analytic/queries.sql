SELECT * FROM pax_fixed.rows;
SELECT id FROM pax_fixed.rows;
SELECT id, score FROM pax_fixed.rows WHERE flag = true;
SELECT id FROM pax_fixed.rows WHERE metric = 70;
SELECT id, score FROM pax_fixed.rows WHERE metric >= 30 AND metric < 80;
SELECT id FROM pax_fixed.rows WHERE flag = false AND metric >= 100;
SELECT id, score FROM pax_fixed.rows WHERE id = 17;
SELECT id, score FROM pax_fixed.rows WHERE id >= 12 AND id < 20;
