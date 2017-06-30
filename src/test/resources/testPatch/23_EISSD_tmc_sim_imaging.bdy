CREATE OR REPLACE PACKAGE BODY tmc_sim_imaging IS

  /**
  * Функция загрузки справочника графических профилей сим-карт
  */
  FUNCTION get_dic_sim_imaging RETURN SYS_REFCURSOR IS
    v_result SYS_REFCURSOR;
  BEGIN
    OPEN v_result FOR
      SELECT t.id, t.description, t.visibility, t.mapping_id
      FROM t_dic_sim_imaging t;
    RETURN v_result;
  END;

  /**
  * Функция загрузки справочника допустимых типов операций (добавление/ изменение)
  */
  FUNCTION get_dic_sim_edit_oper_type RETURN SYS_REFCURSOR IS
    v_result SYS_REFCURSOR;
  BEGIN
    OPEN v_result FOR
      SELECT t.id, t.description
      FROM t_dic_sim_edit_oper_type t;
    RETURN v_result;
  END;

  /**
  * Функция загрузки справочника источников данных для добавления графических профилей
  */
  FUNCTION get_dic_sim_data_source RETURN SYS_REFCURSOR IS
    v_result SYS_REFCURSOR;
  BEGIN
    OPEN v_result FOR
      SELECT t.id, t.description
      FROM t_dic_sim_data_source t;
    RETURN v_result;
  END;

  /**
  * Процедура добавления списка диапазонов сим-карт с предопределённым графическим профилем
  */
  PROCEDURE add_sim_imaging_tab
  (
    pi_sim_imagings  sim_imaging_tab,
    pi_worker_id     NUMBER,
    pi_data_source   NUMBER,
    pi_filename      VARCHAR2,
    po_error_code    OUT NUMBER,
    po_error_message OUT VARCHAR2
  ) IS
    v_history_ids num_tab;
  BEGIN
    SAVEPOINT add_sim_imaging_tab;
    IF range_tab_exists(pi_sim_imagings) = 0 THEN
      FORALL i IN pi_sim_imagings.first .. pi_sim_imagings.last
        INSERT INTO t_sim_by_imaging_type
          (imsi_range_start, imsi_range_end, sim_imaging_type)
        VALUES
          (pi_sim_imagings(i).imsi_range_start, pi_sim_imagings(i).imsi_range_end, pi_sim_imagings(i).sim_imaging_type);
    
      v_history_ids := add_tab_history(pi_sim_imagings, 1, pi_worker_id, po_error_code, po_error_message);
      add_tab_history_full(v_history_ids, pi_sim_imagings, pi_worker_id, pi_data_source, pi_filename, po_error_code, po_error_message);
    ELSE
      po_error_code := 1;
      po_error_message := 'Один или несколько входных диапазонов пересекаются с существующим(и). Для изменения графического профиля выберите
                           соответствующий тип операции';
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO add_sim_imaging_tab;
      po_error_code := SQLCODE;
      po_error_message := SQLERRM;
  END;

  /**
  * Процедура изменения списка диапазонов сим-карт с предопределённым графическим профилем
  */
  PROCEDURE change_sim_imaging_tab
  (
    pi_sim_imagings  sim_imaging_tab,
    pi_worker_id     NUMBER,
    pi_data_source   NUMBER,
    pi_filename      VARCHAR2,
    po_error_code    OUT NUMBER,
    po_error_message OUT VARCHAR2
  ) IS
  BEGIN
    FOR i IN pi_sim_imagings.first .. pi_sim_imagings.last
    LOOP
      change_sim_imaging(pi_sim_imagings(i), pi_worker_id, pi_data_source, pi_filename, po_error_code, po_error_message);
    END LOOP;
  END;

  /**
  * Процедура изменения диапазона сим-карт с предопределённым графическим профилем
  */
  PROCEDURE change_sim_imaging
  (
    pi_sim_imaging   sim_imaging_type,
    pi_worker_id     NUMBER,
    pi_data_source   NUMBER,
    pi_filename      VARCHAR2,
    po_error_code    OUT NUMBER,
    po_error_message OUT VARCHAR2
  ) IS
    v_images     sim_imaging_tab;
    v_history_id NUMBER;
  BEGIN
    v_images := get_conflict_sim_ranges(pi_sim_imaging.imsi_range_start, pi_sim_imaging.imsi_range_end);
    IF v_images.count = 0 THEN
      po_error_code := 1;
      po_error_message := 'Изменяемый диапазон не существует. Для добавления нового диапазона воспользуйтесь соответствующей формой';
    ELSE
      SAVEPOINT change_sim_imaging_savepoint;
      v_history_id := add_history(pi_sim_imaging, 2, pi_worker_id, po_error_code, po_error_message);
      FOR i IN v_images.first .. v_images.last
      LOOP
        IF v_images(i).imsi_range_start = pi_sim_imaging.imsi_range_start
            AND v_images(i).imsi_range_end = pi_sim_imaging.imsi_range_end THEN
          UPDATE t_sim_by_imaging_type sit
          SET sit.sim_imaging_type = pi_sim_imaging.sim_imaging_type
          WHERE sit.id = v_images(i).id;
          add_history_full(v_history_id,
                           v_images(i).id,
                           v_images(i).imsi_range_start,
                           v_images(i).imsi_range_end,
                           v_images(i).sim_imaging_type,
                           v_images(i).imsi_range_start,
                           v_images(i).imsi_range_end,
                           pi_sim_imaging.sim_imaging_type,
                           pi_worker_id,
                           pi_data_source,
                           pi_filename,
                           po_error_code,
                           po_error_message);
          RETURN;
        ELSIF v_images(i).imsi_range_start >= pi_sim_imaging.imsi_range_start
               AND v_images(i).imsi_range_end <= pi_sim_imaging.imsi_range_end THEN
          DELETE FROM t_sim_by_imaging_type sit
          WHERE sit.id = v_images(i).id;
          add_history_full(v_history_id,
                           v_images(i).id,
                           v_images(i).imsi_range_start,
                           v_images(i).imsi_range_end,
                           v_images(i).sim_imaging_type,
                           NULL,
                           NULL,
                           NULL,
                           pi_worker_id,
                           pi_data_source,
                           pi_filename,
                           po_error_code,
                           po_error_message);
        ELSIF v_images(i).imsi_range_start <= pi_sim_imaging.imsi_range_start
               AND v_images(i).imsi_range_end <= pi_sim_imaging.imsi_range_end THEN
          UPDATE t_sim_by_imaging_type sit
          SET sit.imsi_range_end = pi_sim_imaging.imsi_range_start - 1
          WHERE sit.id = v_images(i).id;
          add_history_full(v_history_id,
                           v_images(i).id,
                           v_images(i).imsi_range_start,
                           v_images(i).imsi_range_end,
                           v_images(i).sim_imaging_type,
                           v_images(i).imsi_range_start,
                           pi_sim_imaging.imsi_range_start - 1,
                           v_images(i).sim_imaging_type,
                           pi_worker_id,
                           pi_data_source,
                           pi_filename,
                           po_error_code,
                           po_error_message);
        ELSIF v_images(i).imsi_range_start >= pi_sim_imaging.imsi_range_start
               AND v_images(i).imsi_range_end >= pi_sim_imaging.imsi_range_end THEN
          UPDATE t_sim_by_imaging_type sit
          SET sit.imsi_range_start = pi_sim_imaging.imsi_range_end + 1
          WHERE sit.id = v_images(i).id;
          add_history_full(v_history_id,
                           v_images(i).id,
                           v_images(i).imsi_range_start,
                           v_images(i).imsi_range_end,
                           v_images(i).sim_imaging_type,
                           pi_sim_imaging.imsi_range_end + 1,
                           v_images(i).imsi_range_end,
                           v_images(i).sim_imaging_type,
                           pi_worker_id,
                           pi_data_source,
                           pi_filename,
                           po_error_code,
                           po_error_message);
        ELSIF v_images(i).imsi_range_start <= pi_sim_imaging.imsi_range_start
               AND v_images(i).imsi_range_end >= pi_sim_imaging.imsi_range_end THEN
          UPDATE t_sim_by_imaging_type sit
          SET sit.imsi_range_end = pi_sim_imaging.imsi_range_start - 1
          WHERE sit.id = v_images(i).id;
          add_history_full(v_history_id,
                           v_images(i).id,
                           v_images(i).imsi_range_start,
                           v_images(i).imsi_range_end,
                           v_images(i).sim_imaging_type,
                           v_images(i).imsi_range_start,
                           pi_sim_imaging.imsi_range_start - 1,
                           v_images(i).sim_imaging_type,
                           pi_worker_id,
                           pi_data_source,
                           pi_filename,
                           po_error_code,
                           po_error_message);
          INSERT INTO t_sim_by_imaging_type
            (imsi_range_start, imsi_range_end, sim_imaging_type)
          VALUES
            (pi_sim_imaging.imsi_range_end + 1, v_images(i).imsi_range_end, v_images(i).sim_imaging_type);
          add_history_full(v_history_id,
                           NULL,
                           NULL,
                           NULL,
                           NULL,
                           pi_sim_imaging.imsi_range_end + 1,
                           v_images(i).imsi_range_end,
                           v_images(i).sim_imaging_type,
                           pi_worker_id,
                           pi_data_source,
                           pi_filename,
                           po_error_code,
                           po_error_message);
        END IF;
      END LOOP;
      INSERT INTO t_sim_by_imaging_type
        (imsi_range_start, imsi_range_end, sim_imaging_type)
      VALUES
        (pi_sim_imaging.imsi_range_start, pi_sim_imaging.imsi_range_end, pi_sim_imaging.sim_imaging_type);
      add_history_full(v_history_id,
                       NULL,
                       NULL,
                       NULL,
                       NULL,
                       pi_sim_imaging.imsi_range_start,
                       pi_sim_imaging.imsi_range_end,
                       pi_sim_imaging.sim_imaging_type,
                       pi_worker_id,
                       pi_data_source,
                       pi_filename,
                       po_error_code,
                       po_error_message);
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO change_sim_imaging_savepoint;
      po_error_code := SQLCODE;
      po_error_message := SQLERRM;
  END;

  /**
  * Функция получения коллекции диапазонов, пересекающихся с коллекцией входных
  */
  FUNCTION get_conflict_ranges_for_list
  (
    pi_imaging_ranges sim_imaging_tab,
    po_error_code     OUT NUMBER,
    po_error_message  OUT VARCHAR2
  ) RETURN sim_imaging_tab IS
    v_result    sim_imaging_tab := sim_imaging_tab();
    v_temporary sim_imaging_tab;
  BEGIN
    FOR i IN pi_imaging_ranges.first .. pi_imaging_ranges.last
    LOOP
      v_temporary := v_result;
      IF range_exists(pi_imaging_ranges(i).imsi_range_start, pi_imaging_ranges(i).imsi_range_end) != 0 THEN
        v_result := v_temporary MULTISET UNION get_conflict_sim_ranges(pi_imaging_ranges(i).imsi_range_start, pi_imaging_ranges(i).imsi_range_end);
      ELSE
        po_error_code := 1;
        po_error_message := 'Один или несколько изменяемых диапазонов не существуют. Для добавления новых выберите соответсвующий тип операции';
        EXIT;
      END IF;
    END LOOP;
  
    RETURN v_result;
  END;

  /**
  * Функция получения коллекции диапазонов, пересекающихся со входным
  */
  FUNCTION get_conflict_sim_ranges
  (
    pi_start NUMBER,
    pi_end   NUMBER
  ) RETURN sim_imaging_tab IS
    v_result sim_imaging_tab;
  BEGIN
    SELECT sim_imaging_type(id, imsi_range_start, imsi_range_end, sim_imaging_type)
    BULK COLLECT
    INTO v_result
    FROM (SELECT sit.id, sit.imsi_range_start, sit.imsi_range_end, sit.sim_imaging_type
          FROM t_sim_by_imaging_type sit
          WHERE (pi_start >= sit.imsi_range_start AND sit.imsi_range_end >= pi_start)
                OR (sit.imsi_range_start >= pi_start AND pi_end >= sit.imsi_range_start)
          ORDER BY imsi_range_start, imsi_range_end);
    RETURN v_result;
  END;

  /**
  * Функция проверки пересечения коллекции диапазонов с существующими
  */
  FUNCTION range_tab_exists(pi_imaging_ranges sim_imaging_tab) RETURN NUMBER IS
  BEGIN
    FOR i IN pi_imaging_ranges.first .. pi_imaging_ranges.last
    LOOP
      IF range_exists(pi_imaging_ranges(i).imsi_range_start, pi_imaging_ranges(i).imsi_range_end) > 0 THEN
        RETURN 1;
      END IF;
    END LOOP;
  
    RETURN 0;
  END;

  /**
  * Функция проверки пересечения входного диапазона с существующими
  */
  FUNCTION range_exists
  (
    pi_start NUMBER,
    pi_end   NUMBER
  ) RETURN NUMBER IS
    v_amount NUMBER := 0;
  BEGIN
    SELECT COUNT(*)
    INTO v_amount
    FROM t_sim_by_imaging_type sit
    WHERE ((pi_start >= sit.imsi_range_start AND sit.imsi_range_end >= pi_start) OR
          (sit.imsi_range_start >= pi_start AND pi_end >= sit.imsi_range_start));
    RETURN v_amount;
  END;

  /**
  * Функция добавления записей в историческую таблицу при вставке коллекции диапазонов
  */
  FUNCTION add_tab_history
  (
    pi_imaging_ranges sim_imaging_tab,
    pi_operation_type NUMBER,
    pi_worker_id      NUMBER,
    po_error_code     OUT NUMBER,
    po_error_message  OUT VARCHAR2
  ) RETURN num_tab IS
    v_history_ids num_tab;
  BEGIN
    SAVEPOINT add_sim_imaging_tab_hst;
    FORALL i IN pi_imaging_ranges.first .. pi_imaging_ranges.last
      INSERT INTO t_sim_by_imaging_type_hst
        (imsi_range_start, imsi_range_end, sim_imaging_type, operation_type, worker_id, change_date)
      VALUES
        (pi_imaging_ranges(i).imsi_range_start,
         pi_imaging_ranges(i).imsi_range_end,
         pi_imaging_ranges(i).sim_imaging_type,
         pi_operation_type,
         pi_worker_id,
         SYSDATE)
      RETURNING id BULK COLLECT INTO v_history_ids;
    RETURN v_history_ids;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO add_sim_imaging_tab_hst;
      po_error_code := SQLCODE;
      po_error_message := SQLERRM;
  END;

  /**
  * Функция добавления записей в историческую таблицу
  */
  FUNCTION add_history
  (
    pi_sim_imaging    sim_imaging_type,
    pi_operation_type NUMBER,
    pi_worker_id      NUMBER,
    po_error_code     OUT NUMBER,
    po_error_message  OUT VARCHAR2
  ) RETURN NUMBER IS
    v_history_id NUMBER;
  BEGIN
    SAVEPOINT add_sim_imaging_hst;
    INSERT INTO t_sim_by_imaging_type_hst
      (imsi_range_start, imsi_range_end, sim_imaging_type, operation_type, worker_id, change_date)
    VALUES
      (pi_sim_imaging.imsi_range_start, pi_sim_imaging.imsi_range_end, pi_sim_imaging.sim_imaging_type, pi_operation_type, pi_worker_id, SYSDATE)
    RETURNING id INTO v_history_id;
    RETURN v_history_id;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO add_sim_imaging_hst;
      po_error_code := SQLCODE;
      po_error_message := SQLERRM;
  END;

  /**
  * Процедура добавления записей в расширенную историческую таблицу при вставке коллекции
  */
  PROCEDURE add_tab_history_full
  (
    pi_history_ids     num_tab,
    pi_new_imsi_ranges sim_imaging_tab,
    pi_worker_id       NUMBER,
    pi_data_source     NUMBER,
    pi_filename        VARCHAR2,
    po_error_code      OUT NUMBER,
    po_error_message   OUT VARCHAR2
  ) IS
  BEGIN
    SAVEPOINT add_sim_imaging_tab_hst_full;
    FORALL i IN pi_history_ids.first .. pi_history_ids.last
      INSERT INTO t_sim_by_img_type_hst_full
        (hst_id, new_imsi_range_start, new_imsi_range_end, new_sim_imaging_type, change_date, worker_id, data_source, filename)
      VALUES
        (pi_history_ids(i),
         pi_new_imsi_ranges(i).imsi_range_start,
         pi_new_imsi_ranges(i).imsi_range_end,
         pi_new_imsi_ranges(i).sim_imaging_type,
         SYSDATE,
         pi_worker_id,
         pi_data_source,
         pi_filename);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO add_sim_imaging_tab_hst_full;
      po_error_code := SQLCODE;
      po_error_message := SQLERRM;
  END;

  /**
  * Процедура добавления записей в расширенную историческую таблицу
  */
  PROCEDURE add_history_full
  (
    pi_history_id           NUMBER,
    pi_old_id               NUMBER,
    pi_old_imsi_range_start NUMBER,
    pi_old_imsi_range_end   NUMBER,
    pi_old_sim_imaging_type NUMBER,
    pi_new_imsi_range_start NUMBER,
    pi_new_imsi_range_end   NUMBER,
    pi_new_sim_imaging_type NUMBER,
    pi_worker_id            NUMBER,
    pi_data_source          NUMBER,
    pi_filename             VARCHAR2,
    po_error_code           OUT NUMBER,
    po_error_message        OUT VARCHAR2
  ) IS
  BEGIN
    SAVEPOINT add_sim_imaging_hst_full;
    INSERT INTO t_sim_by_img_type_hst_full
      (hst_id,
       old_id,
       old_imsi_range_start,
       old_imsi_range_end,
       old_sim_imaging_type,
       new_imsi_range_start,
       new_imsi_range_end,
       new_sim_imaging_type,
       change_date,
       worker_id,
       data_source,
       filename)
    VALUES
      (pi_history_id,
       pi_old_id,
       pi_old_imsi_range_start,
       pi_old_imsi_range_end,
       pi_old_sim_imaging_type,
       pi_new_imsi_range_start,
       pi_new_imsi_range_end,
       pi_new_sim_imaging_type,
       SYSDATE,
       pi_worker_id,
       pi_data_source,
       pi_filename);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO add_sim_imaging_hst_full;
      po_error_code := SQLCODE;
      po_error_message := SQLERRM;
  END;

  /**
  * Процедура поиска истории операций
  */
  FUNCTION search_history
  (
    pi_imsi_range_start NUMBER,
    pi_imsi_range_end   NUMBER,
    pi_start_date       DATE,
    pi_end_date         DATE,
    pi_operation_types  num_tab,
    pi_data_sources     num_tab,
    pi_imaging_types    num_tab,
    pi_page_number      NUMBER,
    pi_page_size        NUMBER
  ) RETURN SYS_REFCURSOR IS
    v_result SYS_REFCURSOR;
  BEGIN
    OPEN v_result FOR
      SELECT imsi_range_start, imsi_range_end, sim_imaging_type, worker_id, change_date, operation_type, data_source, filename, COUNT
      FROM (SELECT u.*, ROWNUM rn
            FROM (SELECT hst.imsi_range_start,
                         hst.imsi_range_end,
                         hst.sim_imaging_type,
                         hst.worker_id,
                         hst.change_date,
                         hst.operation_type,
                         hst_full.data_source,
                         hst_full.filename,
                         COUNT(1) OVER() COUNT
                  FROM t_sim_by_imaging_type_hst hst
                  JOIN t_sim_by_img_type_hst_full hst_full ON hst_full.hst_id = hst.id
                  WHERE (((pi_imsi_range_start IS NOT NULL AND pi_imsi_range_end IS NOT NULL) AND
                        ((pi_imsi_range_start >= hst.imsi_range_start AND pi_imsi_range_start <= hst.imsi_range_end) OR
                        (pi_imsi_range_start <= hst.imsi_range_start AND pi_imsi_range_end >= hst.imsi_range_end) OR
                        (pi_imsi_range_end >= hst.imsi_range_start AND pi_imsi_range_end <= hst.imsi_range_end))) OR
                        ((pi_imsi_range_start IS NOT NULL AND pi_imsi_range_end IS NULL) AND
                        ((pi_imsi_range_start >= hst.imsi_range_start AND pi_imsi_range_start <= hst.imsi_range_end) OR
                        (pi_imsi_range_start <= hst.imsi_range_start))) OR
                        ((pi_imsi_range_start IS NULL AND pi_imsi_range_end IS NOT NULL) AND (pi_imsi_range_end >= hst.imsi_range_end) OR
                        (pi_imsi_range_end >= hst.imsi_range_start AND pi_imsi_range_end <= hst.imsi_range_end)) OR
                        (pi_imsi_range_start IS NULL AND pi_imsi_range_end IS NULL))
                        AND ((pi_operation_types IS NOT NULL AND
                        hst.operation_type IN (SELECT column_value
                                                     FROM TABLE(pi_operation_types))) OR pi_operation_types IS NULL)
                        AND ((pi_data_sources IS NOT NULL AND
                        hst_full.data_source IN (SELECT column_value
                                                       FROM TABLE(pi_data_sources))) OR pi_data_sources IS NULL)
                        AND ((pi_imaging_types IS NOT NULL AND
                        hst.sim_imaging_type IN (SELECT column_value
                                                       FROM TABLE(pi_imaging_types))) OR pi_imaging_types IS NULL)
                        AND ((pi_start_date IS NOT NULL AND pi_start_date <= hst.change_date) OR pi_start_date IS NULL)
                        AND ((pi_end_date IS NOT NULL AND pi_end_date + 1 > hst.change_date) OR pi_end_date IS NULL)
                  GROUP BY hst.imsi_range_start,
                           hst.imsi_range_end,
                           hst.sim_imaging_type,
                           hst.worker_id,
                           hst.change_date,
                           hst.operation_type,
                           hst_full.data_source,
                           hst_full.filename
                  ORDER BY hst.change_date DESC) u
            WHERE (pi_page_number IS NOT NULL AND ROWNUM <= pi_page_number * pi_page_size)
                  OR pi_page_number IS NULL) l
      WHERE (pi_page_number IS NOT NULL AND l.rn > (pi_page_number - 1) * pi_page_size)
            OR pi_page_number IS NULL
      ORDER BY change_date DESC;
    RETURN v_result;
  END;
END;
/
