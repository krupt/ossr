ALTER TABLE t_dic_mvno_region ADD without_search_phys NUMBER(1) DEFAULT 0 NOT NULL;
ALTER TABLE t_dic_mvno_region ADD without_search_jur NUMBER(1) DEFAULT 0 NOT NULL;

COMMENT ON COLUMN t_dic_mvno_region.without_search_phys
  IS '¬озможно ли подключение MVNO дл€ физических лиц без поиска фиксированных услуг (0 - запрещено, 1 - разрешено)';
COMMENT ON COLUMN t_dic_mvno_region.without_search_jur
  IS '¬озможно ли подключение MVNO дл€ юридических лиц без поиска фиксированных услуг (0 - запрещено, 1 - разрешено)';




CREATE OR REPLACE PROCEDURE add_logging
(
  pi_action     IN t_logging.action%TYPE,
  pi_time_begin IN t_logging.time_begin%TYPE,
  pi_time_end   IN t_logging.time_end%TYPE,
  pi_worker_id  IN t_logging.worker_id%TYPE,
  pi_descript   IN t_logging.descript%TYPE,
  po_err_num    OUT PLS_INTEGER,
  po_err_msg    OUT VARCHAR2
) IS
  PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  SAVEPOINT sp_add_logging;

  INSERT INTO t_logging
    (action, time_begin, time_end, worker_id, descript)
  VALUES
    (pi_action, pi_time_begin, pi_time_end, pi_worker_id, pi_descript);

  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    po_err_num := SQLCODE;
    po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;
    ROLLBACK TO sp_add_logging;
END add_logging;
/
