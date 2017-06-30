ALTER TABLE t_users ADD employee_number VARCHAR2(20);

COMMENT ON COLUMN t_users.employee_number
  IS '“абельный номер';



ALTER TABLE t_users_hist ADD employee_number VARCHAR2(20);

COMMENT ON COLUMN t_users_hist.employee_number
  IS '“абельный номер';



ALTER TABLE t_dogovor ADD is_employee_number_required NUMBER(1);

COMMENT ON COLUMN t_dogovor.is_employee_number_required
  IS 'ќб€зательно ли заполнение табельного номера у пользователей';



ALTER TABLE t_dogovor_hst ADD is_employee_number_required NUMBER(1);

COMMENT ON COLUMN t_dogovor_hst.is_employee_number_required
  IS 'ќб€зательно ли заполнение табельного номера у пользователей';



