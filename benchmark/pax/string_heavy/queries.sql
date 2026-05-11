SELECT * FROM pax_string.rows;
SELECT name FROM pax_string.rows;
SELECT name, city FROM pax_string.rows WHERE region = 'north';
SELECT name FROM pax_string.rows WHERE city = 'city_03';
SELECT name, note FROM pax_string.rows WHERE visits >= 18 AND visits < 36;
SELECT note FROM pax_string.rows WHERE city = 'city_05';
SELECT name FROM pax_string.rows WHERE visits = 18;
SELECT name, city FROM pax_string.rows WHERE visits >= 30 AND visits < 45;
