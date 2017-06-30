CREATE OR REPLACE TYPE sim_imaging_type FORCE AS OBJECT
(
  id               NUMBER,
  imsi_range_start NUMBER(15),
  imsi_range_end   NUMBER(15),
  sim_imaging_type NUMBER
)
;
/
