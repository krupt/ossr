CREATE OR REPLACE PACKAGE BODY mvno_pkg AS

  с_message_pack_exists CONSTANT VARCHAR2(100) := 'Список рассылки с таким именем уже существует';

  PROCEDURE add_history
  (
    p_message_pack    mvno_message_pack_type,
    p_message_pack_id NUMBER := NULL
  );

  FUNCTION create_message_pack
  (
    p_message_pack  mvno_message_pack_type,
    p_message_list  mvno_message_tab,
    o_error_code    OUT NUMBER,
    o_error_message OUT VARCHAR2
  ) RETURN NUMBER AS
    v_message_pack_id NUMBER;
  BEGIN
    IF check_message_pack_exists(p_message_pack.name) > 0 THEN
      o_error_code := 1;
      o_error_message := с_message_pack_exists;
      RETURN - 1;
    END IF;
    SAVEPOINT create_message_pack_savepoint;
    INSERT INTO t_mvno_message_pack
      (id, NAME, subject_id, message_text, initiator, branch, day_type, start_date, end_date, start_time, end_time, state_id, worker_id, file_name)
    VALUES
      (seq_mvno_message_pack_id.nextval,
       p_message_pack.name,
       p_message_pack.subject_id,
       p_message_pack.message_text,
       p_message_pack.initiator,
       p_message_pack.branch,
       p_message_pack.day_type,
       p_message_pack.start_date,
       p_message_pack.end_date,
       p_message_pack.start_time,
       p_message_pack.end_time,
       p_message_pack.state_id,
       p_message_pack.worker_id,
       p_message_pack.file_name)
    RETURNING id INTO v_message_pack_id;
    FORALL i IN p_message_list.first .. p_message_list.last
      INSERT INTO t_mvno_message
        (id, pack_id, phone_number, region_id)
      VALUES
        (seq_mvno_message_id.nextval, v_message_pack_id, p_message_list(i).phone_number, p_message_list(i).region_id);
    add_history(p_message_pack, v_message_pack_id);
    RETURN v_message_pack_id;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO create_message_pack_savepoint;
      o_error_code := SQLCODE;
      o_error_message := SQLERRM;
      RETURN - 1;
  END;

  FUNCTION check_message_pack_exists
  (
    p_message_pack_name VARCHAR2,
    p_message_pack_id   NUMBER := NULL
  ) RETURN NUMBER AS
    v_found NUMBER;
  BEGIN
    SELECT COUNT(1)
    INTO v_found
    FROM t_mvno_message_pack
    WHERE LOWER(NAME) = LOWER(p_message_pack_name)
          AND ((p_message_pack_id IS NOT NULL AND id <> p_message_pack_id) OR p_message_pack_id IS NULL)
          AND ROWNUM = 1;
    RETURN v_found;
  END;

  PROCEDURE add_history
  (
    p_message_pack    mvno_message_pack_type,
    p_message_pack_id NUMBER := NULL
  ) AS
  BEGIN
    INSERT INTO t_mvno_message_pack_hst
      (id, pack_id, update_date, state_id, worker_id)
    VALUES
      (seq_mvno_message_pack_hst_id.nextval, NVL(p_message_pack_id, p_message_pack.id), SYSDATE, p_message_pack.state_id, p_message_pack.worker_id);
  END;

  PROCEDURE save_message_pack
  (
    p_message_pack  mvno_message_pack_type,
    o_error_code    OUT NUMBER,
    o_error_message OUT VARCHAR2
  ) AS
  BEGIN
    IF check_message_pack_exists(p_message_pack.name, p_message_pack.id) > 0 THEN
      o_error_code := 1;
      o_error_message := с_message_pack_exists;
      RETURN;
    END IF;
    SAVEPOINT save_message_pack_savepoint;
    UPDATE t_mvno_message_pack
    SET NAME         = p_message_pack.name,
        subject_id   = p_message_pack.subject_id,
        message_text = p_message_pack.message_text,
        initiator    = p_message_pack.initiator,
        branch       = p_message_pack.branch,
        day_type     = p_message_pack.day_type,
        start_date   = p_message_pack.start_date,
        end_date     = p_message_pack.end_date,
        start_time   = p_message_pack.start_time,
        end_time     = p_message_pack.end_time
    WHERE id = p_message_pack.id;
    add_history(p_message_pack);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO save_message_pack_savepoint;
      o_error_code := SQLCODE;
      o_error_message := SQLERRM;
  END;

  PROCEDURE change_message_pack_state
  (
    p_message_pack  mvno_message_pack_type,
    o_error_code    OUT NUMBER,
    o_error_message OUT VARCHAR2
  ) AS
  BEGIN
    SAVEPOINT change_pack_state_savepoint;
    UPDATE t_mvno_message_pack
    SET state_id = p_message_pack.state_id
    WHERE id = p_message_pack.id;
    add_history(p_message_pack);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO change_pack_state_savepoint;
      o_error_code := SQLCODE;
      o_error_message := SQLERRM;
  END;

  FUNCTION find_message_pack
  (
    p_id          NUMBER,
    p_name        VARCHAR2,
    p_message     VARCHAR2,
    p_start_date  DATE,
    p_end_date    DATE,
    p_start_time  VARCHAR2,
    p_end_time    VARCHAR2,
    p_regions     varchar2_100_tab,
    p_subjects    num_tab,
    p_statuses    num_tab,
    p_workers     num_tab,
    p_page_number NUMBER,
    p_page_size   NUMBER
  ) RETURN SYS_REFCURSOR AS
    v_result SYS_REFCURSOR;
  BEGIN
    OPEN v_result FOR
      SELECT id, NAME, subject, state, worker, start_date, end_date, regions, message_count, sended, COUNT
      FROM (SELECT u.*, ROWNUM rn
            FROM (SELECT pack.id,
                         NAME,
                         subject_id subject,
                         state_id state,
                         worker_id worker,
                         start_date,
                         end_date,
                         (SELECT LISTAGG(reg1.kl_region, ',') WITHIN GROUP(ORDER BY reg1.kl_region)
                          FROM t_dic_region reg1
                          WHERE reg1.reg_id IN (SELECT DISTINCT mess1.region_id
                                                FROM t_mvno_message mess1
                                                WHERE mess1.pack_id = pack.id)) regions,
                         (SELECT COUNT(1)
                          FROM t_mvno_message
                          WHERE pack_id = pack.id) message_count,
                         (SELECT COUNT(DECODE(state, 1, 1, NULL))
                          FROM t_mvno_message
                          WHERE pack_id = pack.id) sended,
                         COUNT(1) OVER() COUNT
                  FROM t_mvno_message_pack pack
                  JOIN t_mvno_message mess ON mess.pack_id = pack.id
                  JOIN t_dic_region reg ON reg.reg_id = mess.region_id
                  WHERE ((p_id IS NOT NULL AND pack.id = p_id) OR p_id IS NULL)
                        AND ((p_name IS NOT NULL AND LOWER(pack.name) LIKE LOWER('%' || p_name || '%')) OR p_name IS NULL)
                        AND ((p_message IS NOT NULL AND LOWER(pack.message_text) LIKE LOWER('%' || p_message || '%')) OR p_message IS NULL)
                        AND ((p_start_date IS NOT NULL AND p_end_date IS NOT NULL AND
                        ((pack.start_date BETWEEN p_start_date AND p_end_date) OR (pack.end_date BETWEEN p_start_date AND p_end_date) OR
                        (p_start_date BETWEEN pack.start_date AND pack.end_date) OR (p_end_date BETWEEN pack.start_date AND pack.end_date))) OR
                        (p_start_date IS NOT NULL AND p_end_date IS NULL AND p_start_date BETWEEN pack.start_date AND pack.end_date) OR
                        (p_start_date IS NULL AND p_end_date IS NOT NULL AND p_end_date BETWEEN pack.start_date AND pack.end_date) OR
                        (p_start_date IS NULL AND p_end_date IS NULL))
                        AND ((p_start_time IS NOT NULL AND p_start_time BETWEEN pack.start_time AND pack.end_time) OR p_start_time IS NULL)
                        AND ((p_end_time IS NOT NULL AND p_end_time BETWEEN pack.start_time AND pack.end_time) OR p_end_time IS NULL)
                        AND ((p_regions IS NOT NULL AND reg.kl_region IN (SELECT column_value
                                                                          FROM TABLE(p_regions))) OR p_regions IS NULL)
                        AND
                        ((p_subjects IS NOT NULL AND pack.subject_id IN (SELECT column_value
                                                                         FROM TABLE(p_subjects))) OR p_subjects IS NULL)
                        AND ((p_statuses IS NOT NULL AND pack.state_id IN (SELECT column_value
                                                                           FROM TABLE(p_statuses))) OR p_statuses IS NULL)
                        AND ((p_workers IS NOT NULL AND pack.worker_id IN (SELECT column_value
                                                                           FROM TABLE(p_workers))) OR p_workers IS NULL)
                  GROUP BY pack.id, NAME, subject_id, state_id, worker_id, start_date, end_date
                  ORDER BY pack.id) u
            WHERE (p_page_number IS NOT NULL AND ROWNUM <= p_page_number * p_page_size)
                  OR p_page_number IS NULL) l
      WHERE (p_page_number IS NOT NULL AND l.rn > (p_page_number - 1) * p_page_size)
            OR p_page_number IS NULL
      ORDER BY id;
    RETURN v_result;
  END;

  FUNCTION get_message_pack(p_message_pack_id NUMBER) RETURN mvno_message_pack_type AS
    v_message_pack mvno_message_pack_type;
  BEGIN
    SELECT mvno_message_pack_type(id,
                                  NAME,
                                  subject_id,
                                  message_text,
                                  initiator,
                                  branch,
                                  day_type,
                                  start_date,
                                  end_date,
                                  start_time,
                                  end_time,
                                  state_id,
                                  worker_id,
                                  file_name)
    INTO v_message_pack
    FROM t_mvno_message_pack
    WHERE id = p_message_pack_id
          AND ROWNUM = 1;
    RETURN v_message_pack;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN v_message_pack;
  END;

  FUNCTION get_message_pack_history(p_message_pack_id NUMBER) RETURN SYS_REFCURSOR AS
    v_result SYS_REFCURSOR;
  BEGIN
    OPEN v_result FOR
      SELECT update_date update_date, state_id state, pack.worker_id worker, org_name org_name
      FROM t_mvno_message_pack_hst pack
      JOIN t_users worker ON worker.usr_id = pack.worker_id
      JOIN t_organizations org
      USING (org_id)
      WHERE pack_id = p_message_pack_id
      ORDER BY id;
    RETURN v_result;
  END;

  FUNCTION get_message_pack_messages(p_message_pack_id NUMBER) RETURN SYS_REFCURSOR AS
    v_result SYS_REFCURSOR;
  BEGIN
    OPEN v_result FOR
      SELECT m.phone_number, MIN(reg.ps_reg_id) - 1000 ps_reg_id
      FROM t_mvno_message m
      JOIN t_dic_regions_ps reg ON reg.reg_id = m.region_id
                                   AND reg.ps_reg_id > 1000
      WHERE m.pack_id = p_message_pack_id
      GROUP BY m.phone_number;
    RETURN v_result;
  END;

  FUNCTION get_all_message_pack_states RETURN SYS_REFCURSOR AS
    v_result SYS_REFCURSOR;
  BEGIN
    OPEN v_result FOR
      SELECT *
      FROM t_mvno_dic_message_pack_state
      ORDER BY id;
    RETURN v_result;
  END;

  FUNCTION get_all_subjects RETURN SYS_REFCURSOR AS
    v_result SYS_REFCURSOR;
  BEGIN
    OPEN v_result FOR
      SELECT *
      FROM t_mvno_dic_subject
      ORDER BY id;
    RETURN v_result;
  END;

  FUNCTION get_users_by_role(p_role_id NUMBER) RETURN num_tab AS
    v_result num_tab;
  BEGIN
    SELECT DISTINCT usr_id
    BULK COLLECT
    INTO v_result
    FROM t_user_org
    WHERE role_id = p_role_id;
    RETURN v_result;
  END;

  FUNCTION get_candidates2send(p_state_id NUMBER) RETURN mvno_message_pack_tab AS
    v_packs mvno_message_pack_tab;
  BEGIN
    SELECT mvno_message_pack_type(id,
                                  NAME,
                                  subject_id,
                                  message_text,
                                  initiator,
                                  branch,
                                  day_type,
                                  start_date,
                                  end_date,
                                  start_time,
                                  end_time,
                                  state_id,
                                  worker_id,
                                  file_name)
    BULK COLLECT
    INTO v_packs
    FROM t_mvno_message_pack
    WHERE state_id = p_state_id
          AND SYSDATE >= start_date;
    RETURN v_packs;
  END;

  FUNCTION get_messages2send
  (
    p_pack_id       NUMBER,
    p_message_state NUMBER
  ) RETURN SYS_REFCURSOR AS
    v_result SYS_REFCURSOR;
  BEGIN
    OPEN v_result FOR
      SELECT id, 7 || phone_number phone_number, tz.tz_offset, state
      FROM t_mvno_message m
      JOIN t_dic_region reg ON reg.reg_id = m.region_id
      JOIN t_timezone tz ON tz.tz_id = reg.gmt
      WHERE pack_id = p_pack_id
            AND state = p_message_state
      ORDER BY id;
    RETURN v_result;
  END;

  FUNCTION get_count_messages2send
  (
    p_pack_id       NUMBER,
    p_message_state NUMBER
  ) RETURN NUMBER AS
    v_count NUMBER;
  BEGIN
    SELECT COUNT(1)
    INTO v_count
    FROM t_mvno_message m
    WHERE pack_id = p_pack_id
          AND state = p_message_state;
    RETURN v_count;
  END;

  PROCEDURE save_messages
  (
    p_messages      mvno_message_tab,
    o_error_code    OUT NUMBER,
    o_error_message OUT VARCHAR2
  ) AS
  BEGIN
    SAVEPOINT save_messages_savepoint;
    FORALL i IN p_messages.first .. p_messages.last
      UPDATE t_mvno_message
      SET state = p_messages(i).state
      WHERE id = p_messages(i).id;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO save_messages_savepoint;
      o_error_code := SQLCODE;
      o_error_message := SQLERRM;
  END;

  FUNCTION get_mvno_regions_dictionary RETURN SYS_REFCURSOR AS
    v_result SYS_REFCURSOR;
  BEGIN
    OPEN v_result FOR
      SELECT ps_reg_id - 1000 ps_reg_id, ps_reg_name, reg_id, kl_name reg_name
      FROM t_dic_regions_ps reg_ps
      JOIN t_dic_region
      USING (reg_id)
      WHERE reg_ps.id_mvno IS NOT NULL
      ORDER BY ps_reg_id;
    RETURN v_result;
  END;

END;

/