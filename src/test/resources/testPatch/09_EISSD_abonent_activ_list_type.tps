CREATE OR REPLACE
 TYPE abonent_activ_list_type FORCE AS OBJECT
(
  ab_id          NUMBER,
  root_org_id    NUMBER,
  root_org_name  VARCHAR2(255),
  org_id         NUMBER,
  org_name       VARCHAR2(255),
  callsign       VARCHAR2(128),
  imsi           VARCHAR2(128),
  date_asr       DATE,
  amount_pay     NUMBER,
  amount_place   NUMBER,
  tar_id         NUMBER,
  tar_name       VARCHAR2(128),
  person         VARCHAR2(255),
  abonent        VARCHAR2(255),
  seller         VARCHAR2(255),
  seller_emp_num VARCHAR2(50),
  data_activated DATE,
  date_act       DATE,
  ab_cost        NUMBER,
  ab_paid        NUMBER,
  dates          DATE,
  rn             NUMBER
)
/
