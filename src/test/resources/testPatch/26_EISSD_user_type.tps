CREATE OR REPLACE TYPE user_type FORCE AS OBJECT
(
  user_id               NUMBER,
  login                 VARCHAR2(50),
  password_md5          VARCHAR2(128),
  firstname             VARCHAR2(30),
  middlename            VARCHAR2(30),
  lastname              VARCHAR2(30),
  email                 VARCHAR2(255),
  roles                 num_tab,
  org_id                NUMBER,
  qualifications        num_tab,
  scheds                team_count_tab,
  person_phone          VARCHAR2(16), -- телефон
  system                NUMBER,
  tz_id                 INTEGER,
  date_login_to         DATE,
  order_num             VARCHAR2(50),
  fio_dover             VARCHAR2(200),
  position_dover        VARCHAR2(200),
  na_osnovanii_dover    VARCHAR2(200),
  address_dover         VARCHAR2(200),
  na_osnovanii_doc_type VARCHAR2(200),
  fio_dover_nominative  VARCHAR2(200),
  boss_email            VARCHAR2(255),
  ip_address            VARCHAR2(50),
  salt                  VARCHAR2(16),
  hash_alg_id           NUMBER,
  employee_number       VARCHAR2(20)
)
/
