'''
Tuve que hacer esta prueba por que existe una incompatibilidad de varchar text con crosstabs de postgres. Me llevo tiempo
averiguarlo, y me perdi parte del almuerzo por ello. :/
'''
drop table IF EXISTS example;

CREATE TABLE example (
  id text,
  key text,
  value text
);

INSERT INTO example VALUES
  ('123', 'firstName', 'John'),
  ('123', 'lastName', 'Doe'),
  ('111', 'firstName', 'jose'),
  ('111', 'lastName', 'castillo'),
  ('222', 'firstName', 'guru'),
  ('222', 'lastName', 'santos'),
  ('333', 'firstName', 'camil'),
  ('333', 'lastName', 'rodriguez'),
  ('444', 'lastName', 'saber');
  
 SELECT *
FROM example
ORDER BY id ASC, key ASC;

SELECT *
FROM crosstab(
    'SELECT *
     FROM example
     ORDER BY id ASC, key ASC;'
) AS ct(id text, firstname TEXT, lastname TEXT);